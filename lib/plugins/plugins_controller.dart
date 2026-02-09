import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:dio/dio.dart';
import 'package:mobx/mobx.dart';
import 'package:flutter/services.dart' show rootBundle, AssetManifest;
import 'package:path_provider/path_provider.dart';
import 'package:kazumi/plugins/plugins.dart';
import 'package:kazumi/plugins/plugin_validity_tracker.dart';
import 'package:kazumi/plugins/node_dist_manager.dart';
import 'package:kazumi/plugins/node_runtime_manager.dart';
import 'package:kazumi/plugins/plugin_install_time_tracker.dart';
import 'package:kazumi/request/plugin.dart';
import 'package:kazumi/request/request.dart';
import 'package:kazumi/utils/storage.dart';
import 'package:kazumi/modules/plugin/plugin_http_module.dart';
import 'package:kazumi/utils/logger.dart';
import 'package:kazumi/request/api.dart';

part 'plugins_controller.g.dart';

// 从 1.5.1 版本开始，规则文件储存在单一的 plugins.json 文件中。
// 之前的版本中，规则以分离文件形式存储，版本更新后将这些分离文件合并为单一的 plugins.json 文件。

class PluginsController = _PluginsController with _$PluginsController;

abstract class _PluginsController with Store {
  @observable
  ObservableList<Plugin> pluginList = ObservableList.of([]);

  ObservableList<Plugin> nodePluginList = ObservableList.of([]);

  @observable
  ObservableList<PluginHTTPItem> pluginHTTPList = ObservableList.of([]);

  // 规则有效性追踪器
  final validityTracker = PluginValidityTracker();

  // 规则安装时间追踪器
  final installTimeTracker = PluginInstallTimeTracker();

  String pluginsFileName = "plugins.json";

  Directory? oldPluginDirectory;

  Directory? newPluginDirectory;

  // Initializes the plugin directory and loads all plugins
  Future<void> init() async {
    final directory = await getApplicationSupportDirectory();
    oldPluginDirectory = Directory('${directory.path}/plugins');
    if (!await oldPluginDirectory!.exists()) {
      await oldPluginDirectory!.create(recursive: true);
    }
    newPluginDirectory = Directory('${directory.path}/plugins/v2');
    if (!await newPluginDirectory!.exists()) {
      await newPluginDirectory!.create(recursive: true);
    }
    await loadAllPlugins();
    if (Platform.isWindows) {
      unawaited(_bootstrapNodeRuntimeInBackground());
      return;
    }
    await refreshNodePlugins();
  }

  Future<void> _bootstrapNodeRuntimeInBackground() async {
    final subscribeUrl = GStorage.setting
        .get(SettingBoxKey.nodeSubscribeUrl, defaultValue: '')
        .toString()
        .trim();

    if (subscribeUrl.isNotEmpty) {
      try {
        await NodeDistManager.instance.syncFromSubscribeUrl(subscribeUrl);
      } catch (e) {
        KazumiLogger().w('Plugin: sync Node dist failed: $e');
      }
    }

    try {
      await NodeRuntimeManager.instance.start();
      await refreshNodePlugins();
    } catch (e) {
      KazumiLogger().w('Plugin: bootstrap Node runtime failed: $e');
    }
  }

  List<Plugin> getSearchablePlugins() {
    if (nodePluginList.isEmpty) {
      return List<Plugin>.from(pluginList);
    }
    return <Plugin>[...pluginList, ...nodePluginList];
  }

  int getSearchablePluginCount() {
    return pluginList.length + nodePluginList.length;
  }

  Future<void> refreshNodePlugins({String? serverUrl}) async {
    nodePluginList.clear();
    if (!Platform.isWindows) {
      return;
    }
    // Priority: explicit param > running runtime > manual nodeServerUrl setting
    var effectiveUrl = serverUrl?.trim() ?? '';
    if (effectiveUrl.isEmpty) {
      effectiveUrl = NodeRuntimeManager.instance.serverUrl;
    }
    if (effectiveUrl.isEmpty) {
      final started = await NodeRuntimeManager.instance.start();
      if (started) {
        effectiveUrl = NodeRuntimeManager.instance.serverUrl;
      }
    }
    if (effectiveUrl.isEmpty) {
      effectiveUrl = GStorage.setting
          .get(SettingBoxKey.nodeServerUrl, defaultValue: '')
          .toString()
          .trim();
    }
    if (effectiveUrl.isEmpty) {
      return;
    }
    final server = _normalizeNodeServerUrl(effectiveUrl);
    try {
      final resp = await Request().get(
        '$server/config',
        options: Options(
          sendTimeout: const Duration(seconds: 2),
          receiveTimeout: const Duration(seconds: 2),
        ),
        shouldRethrow: true,
      );
      final map = _asJsonMap(resp.data);
      final video = map?['video'];
      final sites = (video is Map) ? video['sites'] : null;
      if (sites is! List) {
        return;
      }
      final seenNames = <String>{};
      for (final site in sites) {
        if (site is! Map) {
          continue;
        }
        final siteName = site['name']?.toString().trim() ?? '';
        final siteApi = site['api']?.toString().trim() ?? '';
        if (siteName.isEmpty || siteApi.isEmpty) {
          continue;
        }
        final displayName = 'Node-$siteName';
        if (seenNames.contains(displayName)) {
          continue;
        }
        seenNames.add(displayName);
        nodePluginList.add(Plugin.fromNodeSource(
          name: displayName,
          nodeServer: server,
          nodeApi: siteApi,
        ));
      }
      if (nodePluginList.isNotEmpty) {
        KazumiLogger()
            .i('Plugin: loaded Node sources ${nodePluginList.length}');
      }
    } catch (e) {
      KazumiLogger().w('Plugin: load Node sources failed: ${e.toString()}');
    }
  }

  String _normalizeNodeServerUrl(String url) {
    var server = url.trim();
    if (!server.startsWith('http://') && !server.startsWith('https://')) {
      server = 'http://$server';
    }
    if (server.endsWith('/')) {
      server = server.substring(0, server.length - 1);
    }
    return server;
  }

  Map<String, dynamic>? _asJsonMap(dynamic data) {
    if (data == null) {
      return null;
    }
    if (data is Map<String, dynamic>) {
      return data;
    }
    if (data is Map) {
      return Map<String, dynamic>.from(data);
    }
    if (data is String) {
      try {
        final decoded = jsonDecode(data);
        if (decoded is Map) {
          return Map<String, dynamic>.from(decoded);
        }
      } catch (_) {
        return null;
      }
    }
    return null;
  }

  // Loads all plugins from the directory, populates the plugin list, and saves to plugins.json if needed
  Future<void> loadAllPlugins() async {
    pluginList.clear();
    KazumiLogger()
        .i('Plugins Directory: ${newPluginDirectory!.path}');
    if (await newPluginDirectory!.exists()) {
      final pluginsFile = File('${newPluginDirectory!.path}/$pluginsFileName');
      if (await pluginsFile.exists()) {
        final jsonString = await pluginsFile.readAsString();
        pluginList.addAll(getPluginListFromJson(jsonString));
        KazumiLogger()
            .i('Plugin: Current Plugin number: ${pluginList.length}');
      } else {
        // No plugins.json
        var jsonFiles = await getPluginFiles();
        for (var filePath in jsonFiles) {
          final file = File(filePath);
          final jsonString = await file.readAsString();
          final data = jsonDecode(jsonString);
          final plugin = Plugin.fromJson(data);
          pluginList.add(plugin);
          await file.delete(recursive: true);
        }
        savePlugins();
      }
    } else {
      KazumiLogger().w('Plugin: plugin directory does not exist');
    }
  }

  // Retrieves a list of JSON plugin file paths from the plugin directory
  Future<List<String>> getPluginFiles() async {
    if (await oldPluginDirectory!.exists()) {
      final jsonFiles = oldPluginDirectory!
          .listSync()
          .where((file) => file.path.endsWith('.json') && file is File)
          .map((file) => file.path)
          .toList();
      return jsonFiles;
    } else {
      return [];
    }
  }

  // Copies plugin JSON files from the assets to the plugin directory
  Future<void> copyPluginsToExternalDirectory() async {
    final assetManifest = await AssetManifest.loadFromAssetBundle(rootBundle);
    final assets = assetManifest.listAssets();
    final jsonFiles = assets.where((String asset) =>
        asset.startsWith('assets/plugins/') && asset.endsWith('.json'));

    for (var filePath in jsonFiles) {
      final jsonString = await rootBundle.loadString(filePath);
      final plugin = Plugin.fromJson(jsonDecode(jsonString));
      pluginList.add(plugin);
    }
    await savePlugins();
    KazumiLogger().i(
        'Plugin: ${jsonFiles.length} plugin files copied to ${newPluginDirectory!.path}');
  }

  List<dynamic> pluginListToJson() {
    final List<dynamic> json = [];
    for (var plugin in pluginList) {
      json.add(plugin.toJson());
    }
    return json;
  }

  // Converts a JSON string into a list of Plugin objects.
  List<Plugin> getPluginListFromJson(String jsonString) {
    List<dynamic> json = jsonDecode(jsonString);
    List<Plugin> plugins = [];
    for (var j in json) {
      plugins.add(Plugin.fromJson(j));
    }
    return plugins;
  }

  Future<void> removePlugin(Plugin plugin) async {
    pluginList.removeWhere((p) => p.name == plugin.name);
    await savePlugins();
  }

  // Update or add plugin
  void updatePlugin(Plugin plugin) {
    bool flag = false;
    for (int i = 0; i < pluginList.length; ++i) {
      if (pluginList[i].name == plugin.name) {
        pluginList.replaceRange(i, i + 1, [plugin]);
        flag = true;
        break;
      }
    }
    if (!flag) {
      pluginList.add(plugin);
    }
    savePlugins();
  }

  void onReorder(int oldIndex, int newIndex) {
    if (oldIndex < newIndex) {
      newIndex -= 1;
    }
    final plugin = pluginList.removeAt(oldIndex);
    pluginList.insert(newIndex, plugin);
    savePlugins();
  }

  Future<void> savePlugins() async {
    final jsonData = jsonEncode(pluginListToJson());
    final pluginsFile = File('${newPluginDirectory!.path}/$pluginsFileName');
    await pluginsFile.writeAsString(jsonData);
    KazumiLogger().i('Plugin: updated plugin file $pluginsFileName');
  }

  Future<void> queryPluginHTTPList() async {
    pluginHTTPList.clear();
    var pluginHTTPListRes = await PluginHTTP.getPluginList();
    pluginHTTPList.addAll(pluginHTTPListRes);
  }

  Future<Plugin?> queryPluginHTTP(String name) async {
    Plugin? plugin;
    plugin = await PluginHTTP.getPlugin(name);
    return plugin;
  }

  String pluginStatus(PluginHTTPItem pluginHTTPItem) {
    String pluginStatus = 'install';
    for (Plugin plugin in pluginList) {
      if (pluginHTTPItem.name == plugin.name) {
        if (pluginHTTPItem.version == plugin.version) {
          pluginStatus = 'installed';
        } else {
          pluginStatus = 'update';
        }
        break;
      }
    }
    return pluginStatus;
  }

  String pluginUpdateStatus(Plugin plugin) {
    if (!pluginHTTPList.any((p) => p.name == plugin.name)) {
      return "nonexistent";
    }
    PluginHTTPItem p = pluginHTTPList.firstWhere(
      (p) => p.name == plugin.name,
    );
    return p.version == plugin.version ? "latest" : "updatable";
  }

  Future<int> tryUpdatePlugin(Plugin plugin) async {
    return await tryUpdatePluginByName(plugin.name);
  }

  Future<int> tryUpdatePluginByName(String name) async {
    var pluginHTTPItem = await queryPluginHTTP(name);
    if (pluginHTTPItem != null) {
      if (int.parse(pluginHTTPItem.api) > Api.apiLevel) {
        return 1;
      }
      updatePlugin(pluginHTTPItem);
      return 0;
    }
    return 2;
  }

  Future<int> tryUpdateAllPlugin() async {
    int count = 0;
    for (Plugin plugin in pluginList) {
      if (pluginUpdateStatus(plugin) == 'updatable') {
        if (await tryUpdatePlugin(plugin) == 0) {
          count++;
        }
      }
    }
    return count;
  }

  void removePlugins(Set<String> pluginNames) {
    for (int i = pluginList.length - 1; i >= 0; --i) {
      var name = pluginList[i].name;
      if (pluginNames.contains(name)) {
        pluginList.removeAt(i);
      }
    }
    savePlugins();
  }
}
