import 'dart:convert';
import 'package:dio/dio.dart';
import 'package:kazumi/plugins/node_runtime_manager.dart';
import 'package:kazumi/modules/search/plugin_search_module.dart';
import 'package:kazumi/modules/roads/road_module.dart';
import 'package:kazumi/request/request.dart';
import 'package:html/parser.dart';
import 'package:kazumi/request/api.dart';
import 'package:kazumi/utils/logger.dart';
import 'package:xpath_selector_html_parser/xpath_selector_html_parser.dart';
import 'package:kazumi/utils/utils.dart';

class Plugin {
  String api;
  String type;
  String name;
  String version;
  bool muliSources;
  bool useWebview;
  bool useNativePlayer;
  bool usePost;
  bool useLegacyParser;
  bool adBlocker;
  String userAgent;
  String baseUrl;
  String searchURL;
  String searchList;
  String searchName;
  String searchResult;
  String chapterRoads;
  String chapterResult;
  String referer;
  bool isNodeSource;
  String nodeServer;
  String nodeApi;

  Plugin({
    required this.api,
    required this.type,
    required this.name,
    required this.version,
    required this.muliSources,
    required this.useWebview,
    required this.useNativePlayer,
    required this.usePost,
    required this.useLegacyParser,
    required this.adBlocker,
    required this.userAgent,
    required this.baseUrl,
    required this.searchURL,
    required this.searchList,
    required this.searchName,
    required this.searchResult,
    required this.chapterRoads,
    required this.chapterResult,
    required this.referer,
    this.isNodeSource = false,
    this.nodeServer = '',
    this.nodeApi = '',
  });

  factory Plugin.fromJson(Map<String, dynamic> json) {
    return Plugin(
        api: json['api'],
        type: json['type'],
        name: json['name'],
        version: json['version'],
        muliSources: json['muliSources'],
        useWebview: json['useWebview'],
        useNativePlayer: json['useNativePlayer'],
        usePost: json['usePost'] ?? false,
        useLegacyParser: json['useLegacyParser'] ?? false,
        adBlocker: json['adBlocker'] ?? false,
        userAgent: json['userAgent'],
        baseUrl: json['baseURL'],
        searchURL: json['searchURL'],
        searchList: json['searchList'],
        searchName: json['searchName'],
        searchResult: json['searchResult'],
        chapterRoads: json['chapterRoads'],
        chapterResult: json['chapterResult'],
        referer: json['referer'] ?? '',
        isNodeSource: json['isNodeSource'] ?? false,
        nodeServer: json['nodeServer'] ?? '',
        nodeApi: json['nodeApi'] ?? '');
  }

  factory Plugin.fromNodeSource({
    required String name,
    required String nodeServer,
    required String nodeApi,
  }) {
    return Plugin(
      api: 'nodejs',
      type: 'nodejs',
      name: name,
      version: 'nodejs',
      muliSources: true,
      useWebview: true,
      useNativePlayer: true,
      usePost: true,
      useLegacyParser: false,
      adBlocker: false,
      userAgent: '',
      baseUrl: '',
      searchURL: '',
      searchList: '',
      searchName: '',
      searchResult: '',
      chapterRoads: '',
      chapterResult: '',
      referer: '',
      isNodeSource: true,
      nodeServer: nodeServer,
      nodeApi: nodeApi,
    );
  }

  factory Plugin.fromTemplate() {
    return Plugin(
        api: Api.apiLevel.toString(),
        type: 'anime',
        name: '',
        version: '',
        muliSources: true,
        useWebview: true,
        useNativePlayer: true,
        usePost: false,
        useLegacyParser: false,
        adBlocker: false,
        userAgent: '',
        baseUrl: '',
        searchURL: '',
        searchList: '',
        searchName: '',
        searchResult: '',
        chapterRoads: '',
        chapterResult: '',
        referer: '');
  }

  Map<String, dynamic> toJson() {
    final Map<String, dynamic> data = <String, dynamic>{};
    data['api'] = api;
    data['type'] = type;
    data['name'] = name;
    data['version'] = version;
    data['muliSources'] = muliSources;
    data['useWebview'] = useWebview;
    data['useNativePlayer'] = useNativePlayer;
    data['usePost'] = usePost;
    data['useLegacyParser'] = useLegacyParser;
    data['adBlocker'] = adBlocker;
    data['userAgent'] = userAgent;
    data['baseURL'] = baseUrl;
    data['searchURL'] = searchURL;
    data['searchList'] = searchList;
    data['searchName'] = searchName;
    data['searchResult'] = searchResult;
    data['chapterRoads'] = chapterRoads;
    data['chapterResult'] = chapterResult;
    data['referer'] = referer;
    data['isNodeSource'] = isNodeSource;
    data['nodeServer'] = nodeServer;
    data['nodeApi'] = nodeApi;
    return data;
  }

  Future<PluginSearchResponse> queryBangumi(String keyword,
      {bool shouldRethrow = false}) async {
    if (isNodeSource) {
      return _queryNodeBangumi(keyword, shouldRethrow: shouldRethrow);
    }
    String queryURL = searchURL.replaceAll('@keyword', keyword);
    dynamic resp;
    List<SearchItem> searchItems = [];
    if (usePost) {
      Uri uri = Uri.parse(queryURL);
      Map<String, String> queryParams = uri.queryParameters;
      Uri postUri = Uri(
        scheme: uri.scheme,
        host: uri.host,
        path: uri.path,
      );
      var httpHeaders = {
        'referer': '$baseUrl/',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept-Language': Utils.getRandomAcceptedLanguage(),
        'Connection': 'keep-alive',
      };
      resp = await Request().post(postUri.toString(),
          options: Options(headers: httpHeaders),
          extra: {'customError': ''},
          data: queryParams,
          shouldRethrow: shouldRethrow);
    } else {
      var httpHeaders = {
        'referer': '$baseUrl/',
        'Accept-Language': Utils.getRandomAcceptedLanguage(),
        'Connection': 'keep-alive',
      };
      resp = await Request().get(queryURL,
          options: Options(headers: httpHeaders),
          shouldRethrow: shouldRethrow,
          extra: {'customError': ''});
    }

    var htmlString = resp.data.toString();
    var htmlElement = parse(htmlString).documentElement!;

    htmlElement.queryXPath(searchList).nodes.forEach((element) {
      try {
        SearchItem searchItem = SearchItem(
          name: element.queryXPath(searchName).node!.text?.trim() ?? '',
          src: element.queryXPath(searchResult).node!.attributes['href'] ?? '',
        );
        searchItems.add(searchItem);
        KazumiLogger().i(
            'Plugin: $name ${element.queryXPath(searchName).node!.text ?? ''} $baseUrl${element.queryXPath(searchResult).node!.attributes['href'] ?? ''}');
      } catch (_) {}
    });
    PluginSearchResponse pluginSearchResponse =
        PluginSearchResponse(pluginName: name, data: searchItems);
    return pluginSearchResponse;
  }

  Future<List<Road>> querychapterRoads(String url,
      {CancelToken? cancelToken}) async {
    if (isNodeSource) {
      return _queryNodeRoads(url, cancelToken: cancelToken);
    }
    List<Road> roadList = [];
    // 预处理
    if (!url.contains('https')) {
      url = url.replaceAll('http', 'https');
    }
    String queryURL = '';
    if (url.contains(baseUrl)) {
      queryURL = url;
    } else {
      queryURL = baseUrl + url;
    }
    var httpHeaders = {
      'referer': '$baseUrl/',
      'Accept-Language': Utils.getRandomAcceptedLanguage(),
      'Connection': 'keep-alive',
    };
    try {
      var resp = await Request().get(queryURL,
          options: Options(headers: httpHeaders), cancelToken: cancelToken);
      var htmlString = resp.data.toString();
      var htmlElement = parse(htmlString).documentElement!;
      int count = 1;
      htmlElement.queryXPath(chapterRoads).nodes.forEach((element) {
        try {
          List<String> chapterUrlList = [];
          List<String> chapterNameList = [];
          element.queryXPath(chapterResult).nodes.forEach((item) {
            String itemUrl = item.node.attributes['href'] ?? '';
            String itemName = item.node.text ?? '';
            chapterUrlList.add(itemUrl);
            chapterNameList.add(itemName.replaceAll(RegExp(r'\s+'), ''));
          });
          if (chapterUrlList.isNotEmpty && chapterNameList.isNotEmpty) {
            Road road = Road(
                name: '播放列表$count',
                data: chapterUrlList,
                identifier: chapterNameList);
            roadList.add(road);
            count++;
          }
        } catch (_) {}
      });
    } catch (_) {}
    return roadList;
  }

  Future<NodePlayInfo?> queryNodePlay(String flag, String id,
      {CancelToken? cancelToken}) async {
    if (!isNodeSource) {
      return null;
    }
    final url = _buildNodeUrl('/play');
    if (url.isEmpty) {
      return null;
    }
    try {
      final resp = await Request().post(url,
          data: {'flag': flag, 'id': id},
          cancelToken: cancelToken,
          shouldRethrow: true);
      final map = _asJsonMap(resp.data);
      if (map == null) {
        return null;
      }
      return NodePlayInfo.fromMap(map);
    } catch (_) {
      return null;
    }
  }

  Future<PluginSearchResponse> _queryNodeBangumi(String keyword,
      {bool shouldRethrow = false}) async {
    final url = _buildNodeUrl('/search');
    List<SearchItem> searchItems = [];
    if (url.isEmpty) {
      return PluginSearchResponse(pluginName: name, data: searchItems);
    }
    try {
      final resp = await Request().post(url,
          data: {'wd': keyword, 'page': 1}, shouldRethrow: shouldRethrow);
      final map = _asJsonMap(resp.data);
      final list = map?['list'];
      if (list is List) {
        for (final item in list) {
          if (item is Map) {
            final itemName = item['vod_name']?.toString() ?? '';
            final itemId = item['vod_id']?.toString() ?? '';
            if (itemName.isEmpty || itemId.isEmpty) {
              continue;
            }
            searchItems.add(SearchItem(name: itemName, src: itemId));
          }
        }
      }
    } catch (_) {}
    return PluginSearchResponse(pluginName: name, data: searchItems);
  }

  Future<List<Road>> _queryNodeRoads(String id,
      {CancelToken? cancelToken}) async {
    final roadList = <Road>[];
    final url = _buildNodeUrl('/detail');
    if (url.isEmpty) {
      return roadList;
    }
    try {
      final resp =
          await Request().post(url, data: {'id': id}, cancelToken: cancelToken);
      final map = _asJsonMap(resp.data);
      final list = map?['list'];
      if (list is! List || list.isEmpty) {
        return roadList;
      }
      final vod = list.first;
      if (vod is! Map) {
        return roadList;
      }
      final fromRaw = vod['vod_play_from']?.toString() ?? '';
      final urlRaw = vod['vod_play_url']?.toString() ?? '';
      if (fromRaw.isEmpty || urlRaw.isEmpty) {
        return roadList;
      }
      final fromList = fromRaw.split(r'$$$');
      final urlList = urlRaw.split(r'$$$');
      final count =
          fromList.length < urlList.length ? fromList.length : urlList.length;
      int index = 1;
      for (int i = 0; i < count; i++) {
        final flag = fromList[i];
        final episodes = urlList[i].split('#');
        final data = <String>[];
        final identifiers = <String>[];
        int episodeIndex = 1;
        for (final episode in episodes) {
          final trimmed = episode.trim();
          if (trimmed.isEmpty) {
            continue;
          }
          final parts = trimmed.split(r'$');
          if (parts.length < 2) {
            continue;
          }
          final episodeName = parts.first.trim().isEmpty
              ? '第$episodeIndex集'
              : parts.first.trim();
          final episodeId = parts.sublist(1).join(r'$').trim();
          if (episodeId.isEmpty) {
            continue;
          }
          identifiers.add(episodeName);
          data.add(NodeEpisodePayload(flag: flag, id: episodeId).encode());
          episodeIndex++;
        }
        if (data.isNotEmpty) {
          roadList.add(Road(
            name: '播放列表$index',
            data: data,
            identifier: identifiers,
          ));
          index++;
        }
      }
    } catch (_) {}
    return roadList;
  }

  String _buildNodeUrl(String path) {
    if (!isNodeSource) {
      return '';
    }
    var server = nodeServer.trim();
    if (server.isEmpty) {
      server = NodeRuntimeManager.instance.serverUrl.trim();
    }
    if (server.isEmpty) {
      return '';
    }
    if (!server.startsWith('http://') && !server.startsWith('https://')) {
      server = 'http://$server';
    }
    if (server.endsWith('/')) {
      server = server.substring(0, server.length - 1);
    }
    var api = nodeApi.trim();
    if (api.isEmpty) {
      return '';
    }
    if (!api.startsWith('/')) {
      api = '/$api';
    }
    var suffix = path.trim();
    if (!suffix.startsWith('/')) {
      suffix = '/$suffix';
    }
    return '$server$api$suffix';
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

  Future<String> testSearchRequest(String keyword,
      {bool shouldRethrow = false, CancelToken? cancelToken}) async {
    String queryURL = searchURL.replaceAll('@keyword', keyword);
    dynamic resp;
    if (usePost) {
      Uri uri = Uri.parse(queryURL);
      Map<String, String> queryParams = uri.queryParameters;
      Uri postUri = Uri(
        scheme: uri.scheme,
        host: uri.host,
        path: uri.path,
      );
      var httpHeaders = {
        'referer': '$baseUrl/',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept-Language': Utils.getRandomAcceptedLanguage(),
        'Connection': 'keep-alive',
      };
      resp = await Request().post(postUri.toString(),
          options: Options(headers: httpHeaders),
          extra: {'customError': ''},
          data: queryParams,
          shouldRethrow: shouldRethrow,
          cancelToken: cancelToken);
    } else {
      var httpHeaders = {
        'referer': '$baseUrl/',
        'Accept-Language': Utils.getRandomAcceptedLanguage(),
        'Connection': 'keep-alive',
      };
      resp = await Request().get(queryURL,
          options: Options(
            headers: httpHeaders,
          ),
          shouldRethrow: shouldRethrow,
          extra: {'customError': ''},
          cancelToken: cancelToken);
    }

    return resp.data.toString();
  }

  PluginSearchResponse testQueryBangumi(String htmlString) {
    List<SearchItem> searchItems = [];
    var htmlElement = parse(htmlString).documentElement!;
    htmlElement.queryXPath(searchList).nodes.forEach((element) {
      try {
        SearchItem searchItem = SearchItem(
          name: element.queryXPath(searchName).node!.text?.trim() ?? '',
          src: element.queryXPath(searchResult).node!.attributes['href'] ?? '',
        );
        searchItems.add(searchItem);
        KazumiLogger().i(
            'Plugin: $name ${element.queryXPath(searchName).node!.text ?? ''} $baseUrl${element.queryXPath(searchResult).node!.attributes['href'] ?? ''}');
      } catch (_) {}
    });
    PluginSearchResponse pluginSearchResponse =
        PluginSearchResponse(pluginName: name, data: searchItems);
    return pluginSearchResponse;
  }
}

class NodeEpisodePayload {
  final String flag;
  final String id;

  const NodeEpisodePayload({required this.flag, required this.id});

  String encode() {
    return jsonEncode({'flag': flag, 'id': id});
  }

  static NodeEpisodePayload? decode(String raw) {
    if (raw.trim().isEmpty) {
      return null;
    }
    try {
      final decoded = jsonDecode(raw);
      if (decoded is Map) {
        final flag = decoded['flag']?.toString() ?? '';
        final id = decoded['id']?.toString() ?? '';
        if (flag.isEmpty || id.isEmpty) {
          return null;
        }
        return NodeEpisodePayload(flag: flag, id: id);
      }
    } catch (_) {
      return null;
    }
    return null;
  }
}

class NodePlayInfo {
  final int parse;
  final dynamic url;
  final Map<String, dynamic> header;

  NodePlayInfo({
    required this.parse,
    required this.url,
    required this.header,
  });

  factory NodePlayInfo.fromMap(Map<String, dynamic> map) {
    final parseRaw = map['parse'];
    final parse = parseRaw is int
        ? parseRaw
        : int.tryParse(parseRaw?.toString() ?? '') ?? 0;
    final headerRaw = map['header'] ?? map['headers'];
    final header = headerRaw is Map
        ? Map<String, dynamic>.from(headerRaw as Map)
        : <String, dynamic>{};

    dynamic url = map['url'];
    if (url is String && url.trim().isEmpty) {
      url = null;
    } else if (url is List && url.isEmpty) {
      url = null;
    }
    url ??= map['urls'];
    return NodePlayInfo(parse: parse, url: url, header: header);
  }

  String? resolveUrl() {
    if (url == null) {
      return null;
    }
    if (url is String) {
      final direct = (url as String).trim();
      return direct.isEmpty ? null : direct;
    }
    if (url is List) {
      final list = url as List;
      if (list.length >= 2 && list[1] is String) {
        final candidate = (list[1] as String).trim();
        if (candidate.isNotEmpty) {
          return candidate;
        }
      }
      for (final item in list) {
        if (item is String) {
          final candidate = item.trim();
          if (candidate.isNotEmpty) {
            return candidate;
          }
        }
      }
    }
    return null;
  }
}
