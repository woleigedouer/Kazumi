import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:dio/dio.dart';
import 'package:kazumi/utils/logger.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

class NodeDistManager {
  NodeDistManager._();

  static final NodeDistManager instance = NodeDistManager._();

  static const List<String> _distFiles = ['index.js', 'index.config.js'];

  final Dio _dio = Dio(BaseOptions(
    connectTimeout: const Duration(seconds: 12),
    receiveTimeout: const Duration(seconds: 60),
  ));

  Future<Directory> getDistDirectory() async {
    final supportDir = await getApplicationSupportDirectory();
    final distDir =
        Directory(p.join(supportDir.path, 'node_runtime', 'dist'));
    if (!await distDir.exists()) {
      await distDir.create(recursive: true);
    }
    return distDir;
  }

  Future<bool> hasRequiredDistFiles() async {
    final distDir = await getDistDirectory();
    for (final name in _distFiles) {
      if (!File(p.join(distDir.path, name)).existsSync()) {
        return false;
      }
    }
    return true;
  }

  /// Sync dist artifacts from a subscribe URL.
  /// Returns true if any file was updated.
  Future<bool> syncFromSubscribeUrl(
    String subscribeUrl, {
    void Function(double progress)? onProgress,
  }) async {
    if (!Platform.isWindows) return false;

    final raw = subscribeUrl.trim();
    if (raw.isEmpty) return false;

    Uri baseUri;
    try {
      baseUri = Uri.parse(raw);
    } catch (_) {
      KazumiLogger().w('NodeDist: invalid subscribe url');
      return false;
    }

    baseUri = _normalizeSubscribeBaseUri(baseUri);
    if (!baseUri.hasScheme || baseUri.host.isEmpty) {
      KazumiLogger().w('NodeDist: subscribe url missing scheme/host');
      return false;
    }

    final authHeader = _buildBasicAuthHeader(baseUri);
    final headers = <String, dynamic>{};
    if (authHeader != null) {
      headers[HttpHeaders.authorizationHeader] = authHeader;
    }
    final requestOptions = Options(headers: headers);

    final distDir = await getDistDirectory();
    var updatedAny = false;

    for (var i = 0; i < _distFiles.length; i++) {
      final fileName = _distFiles[i];
      final fileUpdated = await _syncSingleFile(
        baseUri: _stripUserInfo(baseUri),
        fileName: fileName,
        distDir: distDir,
        options: requestOptions,
        onFileProgress: (ratio) {
          final global = (i + ratio) / _distFiles.length;
          onProgress?.call(global.clamp(0.0, 1.0));
        },
      );
      updatedAny = updatedAny || fileUpdated;
      onProgress?.call((i + 1) / _distFiles.length);
    }

    return updatedAny;
  }

  Future<bool> _syncSingleFile({
    required Uri baseUri,
    required String fileName,
    required Directory distDir,
    required Options options,
    required void Function(double ratio) onFileProgress,
  }) async {
    final fileUri = _joinUrl(baseUri, fileName);
    final md5Uri = _joinUrl(baseUri, '$fileName.md5');
    final localFile = File(p.join(distDir.path, fileName));
    final localMd5File = File(p.join(distDir.path, '$fileName.md5'));

    final remoteMd5 = await _fetchRemoteMd5(md5Uri.toString(), options);
    if (remoteMd5.isEmpty) {
      throw Exception('NodeDist: failed to fetch remote md5 for $fileName');
    }

    if (localFile.existsSync()) {
      final localHash = await _calculateMd5(localFile);
      if (localHash == remoteMd5) {
        await localMd5File.writeAsString(remoteMd5);
        KazumiLogger().i('NodeDist: $fileName already up to date');
        return false;
      }
    }

    final tempFile = File('${localFile.path}.download');
    if (tempFile.existsSync()) {
      await tempFile.delete();
    }

    await _dio.download(
      fileUri.toString(),
      tempFile.path,
      options: options,
      onReceiveProgress: (received, total) {
        if (total <= 0) return;
        onFileProgress(received / total);
      },
    );

    final downloadedHash = await _calculateMd5(tempFile);
    if (downloadedHash != remoteMd5) {
      await tempFile.delete().catchError((_) => tempFile);
      throw Exception(
        'NodeDist: md5 mismatch for $fileName, expected=$remoteMd5 actual=$downloadedHash',
      );
    }

    if (localFile.existsSync()) {
      await localFile.delete();
    }
    await tempFile.rename(localFile.path);
    await localMd5File.writeAsString(remoteMd5);
    KazumiLogger().i('NodeDist: synced $fileName');
    return true;
  }

  Future<String> _fetchRemoteMd5(String url, Options options) async {
    final response = await _dio.get<String>(
      url,
      options: options.copyWith(responseType: ResponseType.plain),
    );
    return _normalizeMd5(response.data?.trim() ?? '');
  }

  static String _normalizeMd5(String input) {
    if (input.isEmpty) return '';
    for (final part in input.split(RegExp(r'\s+'))) {
      final token = part.trim().toLowerCase();
      if (RegExp(r'^[a-f0-9]{32}$').hasMatch(token)) {
        return token;
      }
    }
    return '';
  }

  Future<String> _calculateMd5(File file) async {
    final bytes = await file.readAsBytes();
    return md5.convert(bytes).toString();
  }

  Uri _normalizeSubscribeBaseUri(Uri uri) {
    if (uri.path.isEmpty || uri.path.endsWith('/') ) return uri;
    if (uri.pathSegments.isEmpty) return uri;
    final last = uri.pathSegments.last.toLowerCase();
    if (last.endsWith('.md5') || last.endsWith('.js')) {
      final idx = uri.path.lastIndexOf('/');
      final dirPath = idx >= 0 ? uri.path.substring(0, idx + 1) : '/';
      return uri.replace(path: dirPath.isEmpty ? '/' : dirPath);
    }
    return uri;
  }

  Uri _joinUrl(Uri baseUri, String name) {
    final normalized =
        baseUri.path.endsWith('/') ? baseUri.path : '${baseUri.path}/';
    return baseUri.replace(path: '$normalized$name');
  }

  Uri _stripUserInfo(Uri uri) {
    if (uri.userInfo.isEmpty) return uri;
    return uri.replace(userInfo: '');
  }

  String? _buildBasicAuthHeader(Uri uri) {
    if (uri.userInfo.isEmpty) return null;
    final split = uri.userInfo.split(':');
    final username = Uri.decodeComponent(split.first);
    final password =
        split.length > 1 ? Uri.decodeComponent(split.sublist(1).join(':')) : '';
    final encoded = base64Encode(utf8.encode('$username:$password'));
    return 'Basic $encoded';
  }
}
