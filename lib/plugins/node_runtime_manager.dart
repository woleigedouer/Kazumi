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
  static const Duration _stopTimeout = Duration(seconds: 2);
  static const Duration _stderrWindow = Duration(seconds: 1);
  static const int _maxStderrLinesPerWindow = 12;
  static const int _maxStderrLineLength = 240;
  static const Duration _stdoutWindow = Duration(seconds: 2);
  static const int _maxNoisyStdoutLinesPerWindow = 2;
  static const bool _forceVerboseNodeLogs = bool.fromEnvironment(
    'KAZUMI_NODE_RUNTIME_VERBOSE_LOGS',
    defaultValue: false,
  );
  static const String _managedPidFileName = 'runtime.pid';
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
  final Set<int> _manualStopPids = <int>{};
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

    await _cleanupStaleRuntimeProcess(bootstrapPath: bootstrap.path);

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
      await _persistManagedPid(process.pid);
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

  Future<void> stop({bool waitForExit = true}) async {
    if (_state == NodeRuntimeState.stopped && _process == null) {
      _serverUrl = '';
      _port = null;
      await _clearManagedPid();
      return;
    }
    _manualStopping = true;
    _autoRestartEnabled = false;
    _state = NodeRuntimeState.stopping;

    final process = _process;
    if (process != null) {
      _manualStopPids.add(process.pid);
      if (!waitForExit) {
        // Fast path for app exit: best-effort kill and return quickly.
        // Any remaining stale process will be cleaned on next start.
        await _terminateProcessTree(process.pid, force: true);
      } else {
        await _terminateProcessTree(process.pid);
        try {
          await process.exitCode.timeout(_stopTimeout);
        } catch (_) {
          await _terminateProcessTree(process.pid, force: true);
          try {
            await process.exitCode.timeout(_stopTimeout);
          } catch (_) {}
        }
      }
    }

    _process = null;
    await _clearManagedPid();
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

    // If this process was manually stopped, never restart.
    if (_manualStopPids.remove(process.pid)) {
      KazumiLogger().i(
        'NodeRuntime: process exited with code $code (manual stop)',
      );
      await _clearManagedPidIfMatches(process.pid);
      _flushSuppressedStderr();
      _resetStderrWindow();
      _flushSuppressedStdout();
      _resetStdoutWindow();
      return;
    }

    final isCurrent = identical(_process, process);
    if (isCurrent) {
      _process = null;
      _state = NodeRuntimeState.stopped;
    }
    await _clearManagedPidIfMatches(process.pid);
    _flushSuppressedStderr();
    _resetStderrWindow();
    _flushSuppressedStdout();
    _resetStdoutWindow();
    KazumiLogger().w('NodeRuntime: process exited with code $code');

    // If this is not the currently managed process, do not restart.
    // This prevents stale exit events from triggering extra restarts.
    if (!isCurrent) return;

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

  Future<void> _cleanupStaleRuntimeProcess({
    required String bootstrapPath,
  }) async {
    final stalePid = await _readManagedPid();
    if (stalePid == null) {
      return;
    }

    if (_process != null && _process!.pid == stalePid) {
      return;
    }

    final commandLine = await _queryCommandLineByPid(stalePid);
    if (commandLine.isEmpty) {
      await _clearManagedPid();
      return;
    }

    if (!_isKazumiRuntimeCommand(commandLine, bootstrapPath)) {
      KazumiLogger().w(
        'NodeRuntime: skip stale pid $stalePid due to command mismatch',
      );
      await _clearManagedPid();
      return;
    }

    KazumiLogger().i('NodeRuntime: terminating stale runtime pid=$stalePid');
    await _terminateProcessTree(stalePid);
    await Future<void>.delayed(const Duration(milliseconds: 300));

    final stillRunning = await _queryCommandLineByPid(stalePid);
    if (stillRunning.isNotEmpty) {
      await _terminateProcessTree(stalePid, force: true);
    }

    await _clearManagedPid();
  }

  Future<void> _terminateProcessTree(int pid, {bool force = false}) async {
    final args = <String>['/PID', '$pid', '/T'];
    if (force) {
      args.add('/F');
    }

    try {
      final result = await Process.run('taskkill', args);
      if (result.exitCode != 0) {
        if (result.exitCode == 128) {
          return;
        }
        final stdout = result.stdout.toString();
        final stderr = result.stderr.toString();
        final output = '$stdout\n$stderr'.toLowerCase();
        if (!output.contains('not found') &&
            !output.contains('no instance') &&
            !output.contains('cannot find') &&
            !output.contains('could not find') &&
            !output.contains('没有运行的实例') &&
            !output.contains('没有找到') &&
            !output.contains('找不到') &&
            !output.contains('无法找到')) {
          KazumiLogger().w(
            'NodeRuntime: taskkill failed (pid=$pid, force=$force, code=${result.exitCode})',
          );
        }
      }
    } catch (e) {
      KazumiLogger().w(
        'NodeRuntime: taskkill exception (pid=$pid, force=$force): $e',
      );
    }
  }

  Future<String> _queryCommandLineByPid(int pid) async {
    try {
      final script =
          '\$p = Get-CimInstance Win32_Process -Filter "ProcessId = $pid" -ErrorAction SilentlyContinue; '
          'if (\$null -ne \$p) { \$p.CommandLine }';
      final result = await Process.run('powershell.exe', [
        '-NoProfile',
        '-NonInteractive',
        '-Command',
        script,
      ]);
      if (result.exitCode != 0) {
        return '';
      }
      return result.stdout.toString().trim();
    } catch (_) {
      return '';
    }
  }

  bool _isKazumiRuntimeCommand(String commandLine, String bootstrapPath) {
    if (commandLine.isEmpty) {
      return false;
    }

    final normalizedCommand = commandLine.toLowerCase().replaceAll('\\', '/');
    final normalizedBootstrap =
        bootstrapPath.toLowerCase().replaceAll('\\', '/');
    return normalizedCommand.contains(normalizedBootstrap);
  }

  Future<File> _pidFile() async {
    final supportDir = await getApplicationSupportDirectory();
    final runtimeDir = Directory(p.join(supportDir.path, 'node_runtime'));
    if (!runtimeDir.existsSync()) {
      runtimeDir.createSync(recursive: true);
    }
    return File(p.join(runtimeDir.path, _managedPidFileName));
  }

  Future<void> _persistManagedPid(int pid) async {
    try {
      final file = await _pidFile();
      await file.writeAsString('$pid', flush: true);
    } catch (e) {
      KazumiLogger().w('NodeRuntime: failed to persist pid: $e');
    }
  }

  Future<int?> _readManagedPid() async {
    try {
      final file = await _pidFile();
      if (!file.existsSync()) {
        return null;
      }
      final text = (await file.readAsString()).trim();
      return int.tryParse(text);
    } catch (_) {
      return null;
    }
  }

  Future<void> _clearManagedPid() async {
    try {
      final file = await _pidFile();
      if (file.existsSync()) {
        await file.delete();
      }
    } catch (_) {}
  }

  Future<void> _clearManagedPidIfMatches(int pid) async {
    final currentPid = await _readManagedPid();
    if (currentPid == pid) {
      await _clearManagedPid();
    }
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
