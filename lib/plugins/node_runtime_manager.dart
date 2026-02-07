import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:dio/dio.dart';
import 'package:kazumi/plugins/node_dist_manager.dart';
import 'package:kazumi/utils/logger.dart';
import 'package:kazumi/utils/storage.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

enum NodeRuntimeState { stopped, starting, running, stopping }

class NodeRuntimeManager {
  NodeRuntimeManager._();

  static final NodeRuntimeManager instance = NodeRuntimeManager._();

  static const int _maxRestarts = 3;
  static const Duration _stderrWindow = Duration(seconds: 1);
  static const int _maxStderrLinesPerWindow = 12;
  static const int _maxStderrLineLength = 240;
  static const Duration _stdoutWindow = Duration(seconds: 2);
  static const int _maxNoisyStdoutLinesPerWindow = 2;
  static const bool _forceVerboseNodeLogs = bool.fromEnvironment(
    'KAZUMI_NODE_RUNTIME_VERBOSE_LOGS',
    defaultValue: false,
  );
  static final RegExp _listenRegex =
      RegExp(r'Server listening on http://[\d.]+:(\d+)');
  static final RegExp _noisyStdoutRegex = RegExp(
    r'"msg":"(incoming request|request completed|stream closed prematurely)"',
    caseSensitive: false,
  );
  static final RegExp _criticalStderrRegex = RegExp(
    r'\b(error|exception|failed|fatal|panic|unhandled)\b',
    caseSensitive: false,
  );
  static final RegExp _sensitiveHeaderRegex = RegExp(
    r'\b(cookie|authorization|set-cookie):\s*([^,\r\n]+)',
    caseSensitive: false,
  );
  static final RegExp _sensitiveQueryRegex = RegExp(
    r'\b(auth_key|src_auth_key|token|access_token|refresh_token|apikey|api_key|password|passwd|pwd)=([^&\s]+)',
    caseSensitive: false,
  );
  static final RegExp _credentialPairRegex = RegExp(
    r'\b(password|passwd|pwd|secret)\s*[:=]\s*([^\s,;]+)',
    caseSensitive: false,
  );
  static final RegExp _sensitiveQuotedFieldRegex = RegExp(
    r"\b([a-z0-9_]*(cookie|token)|auth_key|src_auth_key|password|passwd|pwd)\b\s*:\s*'[^']*'",
    caseSensitive: false,
  );
  static final RegExp _sensitiveJsonFieldRegex = RegExp(
    r'"([a-z0-9_]*(cookie|token)|auth_key|src_auth_key|password|passwd|pwd)"\s*:\s*"[^"]*"',
    caseSensitive: false,
  );
  static final RegExp _stderrDetailRegex = RegExp(
    r'^\s*(at\s+.+|\{|\}|config:\s*\{|\w+:\s*\[Function.+)\s*,?$',
  );

  final Dio _healthDio = Dio(BaseOptions(
    connectTimeout: const Duration(seconds: 1),
    receiveTimeout: const Duration(seconds: 1),
  ));

  Process? _process;
  NodeRuntimeState _state = NodeRuntimeState.stopped;
  Completer<bool>? _startCompleter;
  bool _manualStopping = false;
  bool _autoRestartEnabled = true;
  int _restartCount = 0;
  int? _port;
  String _serverUrl = '';
  DateTime? _stderrWindowStart;
  int _stderrWindowLogged = 0;
  int _stderrWindowSuppressed = 0;
  DateTime? _stdoutWindowStart;
  int _stdoutWindowLogged = 0;
  int _stdoutWindowSuppressed = 0;

  NodeRuntimeState get state => _state;
  bool get isRunning => _state == NodeRuntimeState.running;
  String get serverUrl => _serverUrl;
  int? get port => _port;
  bool get _isVerboseNodeLogEnabled {
    if (_forceVerboseNodeLogs) {
      return true;
    }
    try {
      return GStorage.setting.get(
            SettingBoxKey.playerDebugMode,
            defaultValue: false,
          ) ==
          true;
    } catch (_) {
      return false;
    }
  }

  Future<bool> start({bool forceRestart = false}) async {
    if (!Platform.isWindows) return false;

    if (_state == NodeRuntimeState.running && !forceRestart) return true;
    if (_state == NodeRuntimeState.starting && !forceRestart) {
      return await _startCompleter?.future ?? false;
    }

    if (forceRestart &&
        (_state == NodeRuntimeState.running ||
            _state == NodeRuntimeState.starting)) {
      await stop();
    }

    _state = NodeRuntimeState.starting;
    _manualStopping = false;
    _autoRestartEnabled = true;
    _startCompleter = Completer<bool>();

    if (!await NodeDistManager.instance.hasRequiredDistFiles()) {
      KazumiLogger().w('NodeRuntime: dist files not found, skipping');
      _state = NodeRuntimeState.stopped;
      _startCompleter?.complete(false);
      return false;
    }

    if (!await _verifyDistIntegrity()) {
      KazumiLogger().w('NodeRuntime: dist integrity check failed');
      _state = NodeRuntimeState.stopped;
      _startCompleter?.complete(false);
      return false;
    }

    final runtimeDir = _resolveInstalledRuntimeDir();
    final nodeExe = File(p.join(runtimeDir.path, 'node.exe'));
    final bootstrap = File(p.join(runtimeDir.path, 'bootstrap.js'));
    if (!nodeExe.existsSync() || !bootstrap.existsSync()) {
      KazumiLogger().w(
        'NodeRuntime: runtime binaries missing (node=${nodeExe.path}, bootstrap=${bootstrap.path})',
      );
      _state = NodeRuntimeState.stopped;
      _startCompleter?.complete(false);
      return false;
    }

    final distDir = await NodeDistManager.instance.getDistDirectory();
    _port = null;
    _serverUrl = '';

    try {
      final process = await Process.start(
        nodeExe.path,
        [bootstrap.path],
        workingDirectory: runtimeDir.path,
        environment: {
          'NODE_DIST_PATH': distDir.path,
          'NODE_PATH': distDir.parent.path,
        },
      );
      _process = process;
      _resetStderrWindow();
      _resetStdoutWindow();

      process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(_onStdoutLine);

      process.stderr
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen(_onStderrLine, onDone: _flushSuppressedStderr);

      unawaited(_watchExit(process));

      final healthy =
          await _waitUntilHealthy(timeout: const Duration(seconds: 10));
      if (!healthy) {
        KazumiLogger().w('NodeRuntime: health check timeout');
        await stop();
        _startCompleter?.complete(false);
        return false;
      }

      _state = NodeRuntimeState.running;
      _restartCount = 0;
      _startCompleter?.complete(true);
      KazumiLogger().i('NodeRuntime: started at $_serverUrl');
      return true;
    } catch (e, s) {
      KazumiLogger().e('NodeRuntime: failed to start', error: e, stackTrace: s);
      _state = NodeRuntimeState.stopped;
      _startCompleter?.complete(false);
      return false;
    }
  }

  Future<void> stop() async {
    if (_state == NodeRuntimeState.stopped) {
      _serverUrl = '';
      _port = null;
      return;
    }
    _manualStopping = true;
    _autoRestartEnabled = false;
    _state = NodeRuntimeState.stopping;

    final process = _process;
    if (process != null) {
      process.kill();
      try {
        await process.exitCode.timeout(const Duration(seconds: 2));
      } catch (_) {
        process.kill(ProcessSignal.sigkill);
      }
    }

    _process = null;
    _flushSuppressedStderr();
    _resetStderrWindow();
    _flushSuppressedStdout();
    _resetStdoutWindow();
    _serverUrl = '';
    _port = null;
    _state = NodeRuntimeState.stopped;
  }

  Future<bool> restart() async {
    await stop();
    return start(forceRestart: true);
  }

  String statusText() {
    final label = switch (_state) {
      NodeRuntimeState.stopped => '已停止',
      NodeRuntimeState.starting => '启动中',
      NodeRuntimeState.running => '运行中',
      NodeRuntimeState.stopping => '停止中',
    };
    if (_serverUrl.isNotEmpty) {
      return 'Node $label ($_serverUrl)';
    }
    return 'Node $label';
  }

  // --- private ---

  void _onStdoutLine(String rawLine) {
    if (rawLine.isEmpty) {
      return;
    }

    final match = _listenRegex.firstMatch(rawLine);
    if (match != null) {
      final parsedPort = int.tryParse(match.group(1) ?? '');
      if (parsedPort != null) {
        _port = parsedPort;
        _serverUrl = 'http://127.0.0.1:$parsedPort';
      }
    }

    final line = _sanitizeNodeLine(rawLine);
    if (line.isEmpty) {
      return;
    }

    final verboseEnabled = _isVerboseNodeLogEnabled;
    if (!verboseEnabled) {
      if (_listenRegex.hasMatch(line)) {
        KazumiLogger().i('NodeRuntime stdout: $line');
      }
      return;
    }

    final isNoisy = _noisyStdoutRegex.hasMatch(line);
    if (isNoisy) {
      final now = DateTime.now();
      final windowStart = _stdoutWindowStart;
      if (windowStart == null || now.difference(windowStart) >= _stdoutWindow) {
        _flushSuppressedStdout();
        _stdoutWindowStart = now;
        _stdoutWindowLogged = 0;
        _stdoutWindowSuppressed = 0;
      }

      if (_stdoutWindowLogged >= _maxNoisyStdoutLinesPerWindow) {
        _stdoutWindowSuppressed++;
        return;
      }
      _stdoutWindowLogged++;
    }

    KazumiLogger().i('NodeRuntime stdout: $line');
  }

  Future<void> _watchExit(Process process) async {
    final code = await process.exitCode;
    if (identical(_process, process)) {
      _process = null;
      _state = NodeRuntimeState.stopped;
    }
    _flushSuppressedStderr();
    _resetStderrWindow();
    _flushSuppressedStdout();
    _resetStdoutWindow();
    KazumiLogger().w('NodeRuntime: process exited with code $code');

    if (_manualStopping || !_autoRestartEnabled || !Platform.isWindows) return;

    if (_restartCount >= _maxRestarts) {
      KazumiLogger().w('NodeRuntime: max restart retries exceeded');
      return;
    }

    _restartCount++;
    final delay = Duration(seconds: 1 << (_restartCount - 1));
    KazumiLogger().w('NodeRuntime: restarting in ${delay.inSeconds}s');
    await Future<void>.delayed(delay);
    await start(forceRestart: true);
  }

  void _onStderrLine(String rawLine) {
    final line = _sanitizeNodeLine(rawLine);
    if (line.isEmpty) {
      return;
    }

    final now = DateTime.now();
    final windowStart = _stderrWindowStart;
    if (windowStart == null || now.difference(windowStart) >= _stderrWindow) {
      _flushSuppressedStderr();
      _stderrWindowStart = now;
      _stderrWindowLogged = 0;
      _stderrWindowSuppressed = 0;
    }

    final isCritical = _criticalStderrRegex.hasMatch(line);
    if (!_isVerboseNodeLogEnabled && !isCritical) {
      _stderrWindowSuppressed++;
      return;
    }
    if (!_isVerboseNodeLogEnabled && _stderrDetailRegex.hasMatch(line)) {
      _stderrWindowSuppressed++;
      return;
    }

    if (!isCritical && _stderrWindowLogged >= _maxStderrLinesPerWindow) {
      _stderrWindowSuppressed++;
      return;
    }

    _stderrWindowLogged++;
    KazumiLogger().w('NodeRuntime stderr: $line');
  }

  String _sanitizeNodeLine(String line) {
    var sanitized = line.trimRight();
    if (sanitized.isEmpty) {
      return '';
    }

    sanitized = sanitized.replaceAllMapped(_sensitiveHeaderRegex, (match) {
      return '${match.group(1)}: <redacted>';
    });
    sanitized = sanitized.replaceAllMapped(_sensitiveQueryRegex, (match) {
      return '${match.group(1)}=<redacted>';
    });
    sanitized = sanitized.replaceAllMapped(_credentialPairRegex, (match) {
      return '${match.group(1)}=<redacted>';
    });
    sanitized = sanitized.replaceAllMapped(_sensitiveQuotedFieldRegex, (match) {
      return '${match.group(1)}: \'<redacted>\'';
    });
    sanitized = sanitized.replaceAllMapped(_sensitiveJsonFieldRegex, (match) {
      return '"${match.group(1)}":"<redacted>"';
    });

    if (sanitized.length > _maxStderrLineLength) {
      sanitized =
          '${sanitized.substring(0, _maxStderrLineLength)}...(truncated)';
    }

    return sanitized;
  }

  void _flushSuppressedStderr() {
    if (_stderrWindowSuppressed <= 0) {
      return;
    }
  }

  void _resetStderrWindow() {
    _stderrWindowStart = null;
    _stderrWindowLogged = 0;
    _stderrWindowSuppressed = 0;
  }

  void _flushSuppressedStdout() {
    if (_stdoutWindowSuppressed <= 0) {
      return;
    }
    KazumiLogger().i(
      'NodeRuntime stdout: suppressed $_stdoutWindowSuppressed noisy lines in ${_stdoutWindow.inSeconds}s',
    );
  }

  void _resetStdoutWindow() {
    _stdoutWindowStart = null;
    _stdoutWindowLogged = 0;
    _stdoutWindowSuppressed = 0;
  }

  Future<bool> _waitUntilHealthy({required Duration timeout}) async {
    final deadline = DateTime.now().add(timeout);
    while (DateTime.now().isBefore(deadline)) {
      final currentPort = _port;
      if (currentPort != null) {
        if (await _checkHealth(currentPort)) {
          _serverUrl = 'http://127.0.0.1:$currentPort';
          return true;
        }
      }
      await Future<void>.delayed(const Duration(milliseconds: 250));
    }
    return false;
  }

  Future<bool> _checkHealth(int port) async {
    try {
      final resp =
          await _healthDio.get<dynamic>('http://127.0.0.1:$port/config');
      if (resp.data is Map) {
        return (resp.data as Map).containsKey('video');
      }
      return false;
    } catch (_) {
      return false;
    }
  }

  Directory _resolveInstalledRuntimeDir() {
    final executableDir = File(Platform.resolvedExecutable).parent;

    // Release/installed: data/node_runtime/ next to the executable
    final installedDir =
        Directory(p.join(executableDir.path, 'data', 'node_runtime'));
    if (installedDir.existsSync()) return installedDir;

    // Debug (flutter run): node_runtime/ at project root.
    // Walk up from build/windows/x64/runner/Debug/ to find the project root
    // by looking for the node_runtime/ directory.
    var dir = executableDir;
    for (var i = 0; i < 6; i++) {
      final candidate = Directory(p.join(dir.path, 'node_runtime'));
      if (candidate.existsSync()) return candidate;
      final parent = dir.parent;
      if (parent.path == dir.path) break;
      dir = parent;
    }

    // Fallback to the installed path (will fail with a clear log message)
    return installedDir;
  }

  Future<bool> _verifyDistIntegrity() async {
    final supportDir = await getApplicationSupportDirectory();
    final distDir = Directory(p.join(supportDir.path, 'node_runtime', 'dist'));

    for (final name in ['index.js', 'index.config.js']) {
      final target = File(p.join(distDir.path, name));
      if (!target.existsSync()) return false;

      final md5File = File(p.join(distDir.path, '$name.md5'));
      if (!md5File.existsSync()) continue;

      final expectedRaw = await md5File.readAsString();
      final expected = _normalizeMd5(expectedRaw);
      if (expected.isEmpty) {
        KazumiLogger().w('NodeRuntime: invalid md5 content for $name');
        return false;
      }

      final bytes = await target.readAsBytes();
      final actual = md5.convert(bytes).toString();
      if (actual != expected) {
        KazumiLogger().w(
          'NodeRuntime: md5 mismatch for $name expected=$expected actual=$actual',
        );
        return false;
      }
    }
    return true;
  }

  static String _normalizeMd5(String input) {
    final text = input.trim().toLowerCase();
    for (final part in text.split(RegExp(r'\s+'))) {
      if (RegExp(r'^[a-f0-9]{32}$').hasMatch(part)) {
        return part;
      }
    }
    return '';
  }
}
