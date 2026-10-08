import 'package:test/test.dart';
import 'package:simple_live_core/simple_live_core.dart';
import '../bin/bridge.dart' as bridge;

class FakeSite extends LiveSite {
  @override Future<LiveRoomDetail> getRoomDetail({required String roomId}) async =>
      LiveRoomDetail(roomId: roomId, title: 'Test', cover: '', online: 1,
        userName: 'Anchor', userAvatar: '', status: true, url: 'https://example.com');
  @override Future<List<LivePlayQuality>> getPlayQualites({required LiveRoomDetail detail}) async =>
      [LivePlayQuality(quality: 'Original', data: 'private platform object')];
  @override Future<LivePlayUrl> getPlayUrls({required LiveRoomDetail detail, required LivePlayQuality quality}) async =>
      LivePlayUrl(urls: ['https://example.com/live.flv'], headers: {'user-agent': 'test'});
}
void main() {
  setUp(() { bridge.sites['test'] = FakeSite(); bridge.sessions.clear(); });
  test('room sessions retain opaque platform objects and playback headers', () async {
    final detail = await bridge.rpc({'site': 'test', 'method': 'detail', 'args': {'roomId': '123'}}) as Map;
    final id = detail['Data'];
    expect(detail['DanmakuData'], id);
    expect(id, isA<String>());
    final q = await bridge.rpc({'site': 'test', 'method': 'qualities', 'args': {'session': id}}) as List;
    expect(q.first['Data'], 0);
    final urls = await bridge.rpc({'site': 'test', 'method': 'urls', 'args': {'session': id, 'quality': 0}}) as Map;
    expect(urls['headers'], isEmpty);
    expect((urls['urls'] as List).single, startsWith('http://127.0.0.1:17865/stream/$id/0/'));
  });
  test('rejects missing sessions, cross-platform sessions and invalid quality', () async {
    await expectLater(bridge.rpc({'site': 'test', 'method': 'qualities', 'args': {'session': 'missing'}}), throwsStateError);
    final detail = await bridge.rpc({'site': 'test', 'method': 'detail', 'args': {'roomId': '123'}}) as Map;
    await expectLater(bridge.rpc({'site': 'douyu', 'method': 'qualities', 'args': {'session': detail['Data']}}), throwsStateError);
    await expectLater(bridge.rpc({'site': 'test', 'method': 'urls', 'args': {'session': detail['Data'], 'quality': -1}}), throwsArgumentError);
  });
}
