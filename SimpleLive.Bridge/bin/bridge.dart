import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'package:simple_live_core/simple_live_core.dart';
import 'douyu_stream.dart';

const port = 17865;
const coreRevision = 'ba828e6783b176ea5709fcd09f0eb01dfaceeb51';
final sites = <String, LiveSite>{'douyu': DouyuSite()};
final sessions = <String, Session>{};
DateTime lastActivity = DateTime.now();

class Session {
  Session(this.site, this.detail);
  final LiveSite site;
  final LiveRoomDetail detail;
  final created = DateTime.now();
  List<LivePlayQuality> qualities = [];
}

String newId() => List.generate(24, (_) => Random.secure().nextInt(256))
    .map((n) => n.toRadixString(16).padLeft(2, '0')).join();

Map<String, Object?> room(LiveRoomItem r) => {
  'RoomID': r.roomId, 'Title': r.title, 'Cover': r.cover,
  'UserName': r.userName, 'Online': r.online,
};
Map<String, Object?> sub(LiveSubCategory c) => {
  'ID': c.id, 'ParentID': c.parentId, 'Name': c.name, 'Pic': c.pic,
};

Future<Object?> rpc(Map<String, dynamic> p) async {
  final site = sites[p['site']];
  if (site == null) throw ArgumentError('Unknown platform');
  final args = (p['args'] as Map?)?.cast<String, dynamic>() ?? {};
  final page = args['page'] as int? ?? 1;
  switch (p['method']) {
    case 'categories':
      return (await site.getCategores()).map((c) => {
        'ID': c.id, 'Name': c.name, 'Children': c.children.map(sub).toList(),
      }).toList();
    case 'recommend':
      final r = await site.getRecommendRooms(page: page);
      return {'HasMore': r.hasMore, 'Rooms': r.items.map(room).toList()};
    case 'categoryRooms':
      final c = args['category'] as Map<String, dynamic>;
      final r = await site.getCategoryRooms(LiveSubCategory(
        id: c['ID'], parentId: c['ParentID'], name: c['Name'], pic: c['Pic']), page: page);
      return {'HasMore': r.hasMore, 'Rooms': r.items.map(room).toList()};
    case 'search':
      final r = await site.searchRooms(args['keyword'], page: page);
      return {'HasMore': r.hasMore, 'Rooms': r.items.map(room).toList()};
    case 'status':
      return site.getLiveStatus(roomId: args['roomId']);
    case 'detail':
      sessions.removeWhere((_, s) => DateTime.now().difference(s.created).inMinutes > 30);
      if (sessions.length >= 128) sessions.remove(sessions.keys.first);
      final d = await site.getRoomDetail(roomId: args['roomId']);
      final id = newId();
      sessions[id] = Session(site, d);
      return {
        'RoomID': d.roomId, 'Title': d.title, 'Cover': d.cover,
        'UserName': d.userName, 'UserAvatar': d.userAvatar, 'Online': d.online,
        'Introduction': d.introduction, 'Notice': d.notice, 'Status': d.status,
        'Url': d.url, 'IsRecord': d.isRecord, 'Data': id, 'DanmakuData': id,
      };
    case 'qualities':
      final s = session(args['session'], site);
      s.qualities = await site.getPlayQualites(detail: s.detail);
      return [for (var i = 0; i < s.qualities.length; i++)
        {'Quality': s.qualities[i].quality, 'Data': i, 'Sort': i}];
    case 'urls':
      final s = session(args['session'], site);
      final index = args['quality'] as int;
      if (index < 0 || index >= s.qualities.length) throw ArgumentError('Invalid quality');
      final r = await site.getPlayUrls(detail: s.detail, quality: s.qualities[index]);
      return {'urls': [for (var i = 0; i < r.urls.length; i++)
        'http://127.0.0.1:$port/stream/${args['session']}/$index/$i'], 'headers': <String, String>{}};
    case 'superChat':
      return (await site.getSuperChatMessage(roomId: args['roomId'])).map((m) => {
        'UserName': m.userName, 'Face': m.face, 'Message': m.message, 'Price': m.price,
        'StartTime': m.startTime.toIso8601String(), 'EndTime': m.endTime.toIso8601String(),
        'BackgroundColor': m.backgroundColor, 'BackgroundBottomColor': m.backgroundBottomColor,
      }).toList();
    default: throw ArgumentError('Unknown method');
  }
}

Session session(String? id, LiveSite site) {
  final s = sessions[id];
  if (s == null || !identical(s.site, site)) throw StateError('Room expired; reopen it');
  return s;
}

Future<void> danmaku(HttpRequest request) async {
  final s = sessions[request.uri.queryParameters['session']];
  if (s == null) { request.response.statusCode = 404; await request.response.close(); return; }
  final ws = await WebSocketTransformer.upgrade(request);
  final dm = s.site.getDanmaku();
  dm.onMessage = (m) {
    if (ws.readyState == WebSocket.open) ws.add(jsonEncode({
      'Type': m.type.index, 'UserName': m.userName, 'Message': m.message,
      'Data': m.type == LiveMessageType.online ? int.tryParse('${m.data}') ?? 0 : null,
      'Color': m.color.toString(),
    }));
  };
  dm.onClose = (message) { ws.close(WebSocketStatus.goingAway, 'Platform connection closed'); };
  final heartbeat = Timer.periodic(Duration(seconds: 10), (_) { lastActivity = DateTime.now(); });
  // Simple Live owns platform heartbeat timers and protocol decoding.
  ws.listen((_) {}, onDone: () { heartbeat.cancel(); dm.stop(); },
      onError: (Object _) { heartbeat.cancel(); dm.stop(); });
  try { await dm.start(s.detail.danmakuData); }
  catch (_) { await ws.close(WebSocketStatus.internalServerError, 'Danmaku startup failed'); }
}

Future<void> main() async {
  HttpServer server;
  try { server = await HttpServer.bind(InternetAddress.loopbackIPv4, port); }
  on SocketException { return; } // Repeated UWP launches reuse the existing helper.
  Timer.periodic(Duration(minutes: 1), (t) {
    if (DateTime.now().difference(lastActivity).inMinutes >= 10) {
      t.cancel(); server.close(force: true).then((_) => exit(0));
    }
  });
  await for (final request in server) {
    lastActivity = DateTime.now();
    unawaited(handle(request));
  }
}

Future<void> handle(HttpRequest request) async {
  var websocketRequest = false;
  var streamingRequest = false;
  try {
    // Browser pages cannot call the helper; no CORS or generic proxy endpoint.
    if (request.headers.value('origin') != null) {
      request.response.statusCode = 403; await request.response.close(); return;
    }
    if (request.uri.path == '/danmaku' && WebSocketTransformer.isUpgradeRequest(request)) {
      websocketRequest = true;
      await danmaku(request); return;
    }
    final path = request.uri.pathSegments;
    if (request.method == 'GET' && path.length == 4 && path.first == 'stream') {
      final s = session(path[1], sites['douyu']!);
      final quality = int.parse(path[2]);
      final line = int.parse(path[3]);
      if (quality < 0 || quality >= s.qualities.length || line < 0) throw ArgumentError('Invalid stream');
      streamingRequest = true;
      final heartbeat = Timer.periodic(const Duration(seconds: 10), (_) { lastActivity = DateTime.now(); });
      try { await serveDouyuStream(request, s.site, s.detail, s.qualities[quality], line); }
      finally { heartbeat.cancel(); }
      return;
    }
    request.response.headers.contentType = ContentType.json;
    if (request.method == 'GET' && request.uri.path == '/health') {
      request.response.write(jsonEncode({'protocol': 1, 'scope': 'douyu', 'coreRevision': coreRevision}));
    } else if (request.method == 'POST' && request.uri.path == '/rpc') {
      final bytes = <int>[];
      await for (final chunk in request) {
        bytes.addAll(chunk);
        if (bytes.length > 1024 * 1024) throw ArgumentError('Request too large');
      }
      final result = await rpc(jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>);
      request.response.write(jsonEncode({'result': result}));
    } else { request.response.statusCode = 404; }
  } catch (e) {
    if (streamingRequest) return; // A player that closed its input needs no JSON response.
    request.response.statusCode = 502;
    // Deliberately omit network exception bodies, which may contain cookies or signed URLs.
    request.response.write(jsonEncode({'error': 'Simple Live request failed (${e.runtimeType})'}));
  } finally { if (!websocketRequest && !streamingRequest) await request.response.close(); }
}
