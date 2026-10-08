import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:simple_live_core/simple_live_core.dart';

// Keep lease renewal outside the UI/player. A renewed FLV contributes tags,
// never a second file header, and its media timestamps continue the first feed.
class FlvContinuity {
  bool started = false;
  int lastTimestamp = 0;

  Stream<List<int>> append(Stream<List<int>> input) async* {
    final reader = _Reader(input);
    try {
      final header = await reader.read(9);
      if (header == null || header[0] != 70 || header[1] != 76 || header[2] != 86) {
        throw const FormatException('Expected FLV');
      }
      final headerSize = ByteData.sublistView(header).getUint32(5);
      if (headerSize < 9 || headerSize > 4096) throw const FormatException('Invalid FLV header');
      final extra = await reader.read(headerSize - 9 + 4);
      if (extra == null) throw const FormatException('Truncated FLV header');
      final continuing = started;
      if (!started) {
        yield [...header, ...extra];
        started = true;
      }
      int? offset;
      while (true) {
        final tag = await reader.read(11);
        if (tag == null) return;
        final size = (tag[1] << 16) | (tag[2] << 8) | tag[3];
        if (size > 16 * 1024 * 1024) throw const FormatException('FLV tag too large');
        final body = await reader.read(size + 4);
        if (body == null) throw const FormatException('Truncated FLV tag');
        final timestamp = (tag[7] << 24) | (tag[4] << 16) | (tag[5] << 8) | tag[6];
        final media = tag[0] == 8 || tag[0] == 9;
        final configuration = size >= 2 && body[1] == 0 &&
            ((tag[0] == 8 && (body[0] >> 4) == 10) || (tag[0] == 9 && (body[0] & 15) == 7));
        if (continuing) {
          if (media && !configuration) offset ??= lastTimestamp + 1 - timestamp;
          final adjusted = offset == null ? lastTimestamp + 1 : timestamp + offset;
          final value = adjusted < 0 ? 0 : adjusted;
          tag[4] = (value >> 16) & 255;
          tag[5] = (value >> 8) & 255;
          tag[6] = value & 255;
          tag[7] = (value >> 24) & 255;
          if (media && !configuration && value > lastTimestamp) lastTimestamp = value;
        } else if (media && !configuration && timestamp > lastTimestamp) {
          lastTimestamp = timestamp;
        }
        yield [...tag, ...body];
      }
    } finally {
      await reader.close();
    }
  }
}

class _Reader {
  _Reader(Stream<List<int>> input) : iterator = StreamIterator(input);
  final StreamIterator<List<int>> iterator;
  List<int> chunk = const [];
  int position = 0;
  Future<Uint8List?> read(int length) async {
    final result = Uint8List(length);
    int written = 0;
    while (written < length) {
      if (position == chunk.length) {
        if (!await iterator.moveNext()) {
          if (written == 0) return null;
          throw const FormatException('Truncated FLV');
        }
        chunk = iterator.current;
        position = 0;
        if (chunk.isEmpty) continue;
      }
      final available = chunk.length - position;
      final count = available < length - written ? available : length - written;
      result.setRange(written, written + count, chunk, position);
      written += count;
      position += count;
    }
    return result;
  }
  Future<void> close() => iterator.cancel();
}

Future<void> serveDouyuStream(HttpRequest request, LiveSite site,
    LiveRoomDetail detail, LivePlayQuality quality, int preferred) async {
  final client = HttpClient()..connectionTimeout = const Duration(seconds: 15);
  final continuity = FlvContinuity();
  int failures = 0;
  var first = true;
  try {
    request.response.headers.contentType = ContentType('video', 'x-flv');
    request.response.headers.set('Cache-Control', 'no-store');
    while (failures < 3) {
      if (!first) {
        detail = await site.getRoomDetail(roomId: detail.roomId);
        if (!detail.status) break;
        final qualities = await site.getPlayQualites(detail: detail);
        quality = qualities.firstWhere((q) => q.quality == quality.quality,
            orElse: () => qualities.first);
      }
      first = false;
      final urls = await site.getPlayUrls(detail: detail, quality: quality);
      var progressed = false;
      for (int i = 0; i < urls.urls.length; i++) {
        final index = (preferred + i) % urls.urls.length;
        final connectionStart = DateTime.now();
        try {
          final upstream = await client.getUrl(Uri.parse(urls.urls[index]));
          urls.headers.forEach(upstream.headers.set);
          final response = await upstream.close().timeout(const Duration(seconds: 15));
          if (response.statusCode != 200) { await response.drain<void>(); continue; }
          await for (final bytes in continuity.append(response.timeout(const Duration(seconds: 20)))) {
            request.response.add(bytes);
            await request.response.flush();
          }
          progressed = DateTime.now().difference(connectionStart).inSeconds >= 15;
          if (progressed) { preferred = index; break; }
        } on SocketException { continue; }
          on HttpException { continue; }
          on TimeoutException { continue; }
          on FormatException { continue; }
      }
      failures = progressed ? 0 : failures + 1;
      if (!progressed) await Future<void>.delayed(const Duration(seconds: 1));
    }
  } catch (_) {
    // Closing a room cancels its response; signed addresses stay out of logs.
  } finally {
    client.close(force: true);
    await request.response.close();
  }
}
