import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:simple_live_core/simple_live_core.dart';

class _Tag {
  _Tag(this.bytes);
  final Uint8List bytes;
  int get type => bytes[0];
  int get timestamp =>
      (bytes[7] << 24) | (bytes[4] << 16) | (bytes[5] << 8) | bytes[6];
  bool get configuration =>
      bytes.length >= 17 &&
      bytes[12] == 0 &&
      ((type == 8 && bytes[11] >> 4 == 10) ||
          (type == 9 && bytes[11] & 15 == 7));
  bool get media => (type == 8 || type == 9) && !configuration;
  bool get keyframe => type == 9 && media && bytes[11] >> 4 == 1;
  List<int> shifted(int offset) {
    final result = Uint8List.fromList(bytes);
    final value = (timestamp + offset).clamp(0, 0xffffffff);
    result[4] = value >> 16 & 255;
    result[5] = value >> 8 & 255;
    result[6] = value & 255;
    result[7] = value >> 24 & 255;
    return result;
  }
}

class _Source {
  _Source(Stream<List<int>> input) : reader = _Reader(input);
  final _Reader reader;
  late List<int> header;
  Future<void> initialize() async {
    final first = await reader.read(9);
    if (first == null || first[0] != 70 || first[1] != 76 || first[2] != 86) {
      throw const FormatException('Expected FLV');
    }
    final size = ByteData.sublistView(first).getUint32(5);
    if (size < 9 || size > 4096)
      throw const FormatException('Invalid FLV header');
    final extra = await reader.read(size - 9 + 4);
    if (extra == null) throw const FormatException('Truncated FLV header');
    header = [...first, ...extra];
  }

  Future<_Tag?> next() async {
    final tag = await reader.read(11);
    if (tag == null) return null;
    final size = (tag[1] << 16) | (tag[2] << 8) | tag[3];
    if (size > 16 * 1024 * 1024)
      throw const FormatException('FLV tag too large');
    final body = await reader.read(size + 4);
    if (body == null) throw const FormatException('Truncated FLV tag');
    return _Tag(Uint8List.fromList([...tag, ...body]));
  }

  Future<void> close() => reader.close();
}

class _Prepared {
  _Prepared(this.source, this.keyframe, this.prefix, this.offset);
  final _Source source;
  final _Tag keyframe;
  final List<_Tag> prefix;
  final int offset;
}

/// Open the next lease while the old one still plays. Select a new keyframe
/// on the source timeline, rather than retimestamping the CDN's cached GOP.
Stream<List<int>> spliceDouyuFlv(
  Future<Stream<List<int>>> Function() open, {
  Duration renewAfter = const Duration(seconds: 255),
  Duration retryAfter = const Duration(seconds: 5),
  Duration handoverWait = const Duration(milliseconds: 250),
}) async* {
  _Source? active;
  _Source? warming;
  Future<_Prepared?>? preparation;
  _Prepared? ready;
  var preparationFinished = false;
  var closed = false;
  int lastVideo = -1, lastAudio = -1, offset = 0;
  final configurations = <int, List<int>>{};
  var opened = DateTime.now();
  var nextAttempt = opened.add(renewAfter);

  Future<_Prepared?> prepare() async {
    _Source? source;
    try {
      source = _Source(await open());
      warming = source;
      if (closed) {
        await source.close();
        return null;
      }
      await source.initialize();
      final prefix = <_Tag>[];
      final configs = <int, _Tag>{};
      while (!closed) {
        final tag = await source.next();
        if (tag == null) break;
        if (tag.configuration) configs[tag.type] = tag;
        if (tag.type == 8 && tag.media) {
          prefix.add(tag);
          if (prefix.length > 512) prefix.removeAt(0);
        }
        if (!tag.keyframe) continue;
        // A restarted broadcaster may have a new clock; normal lease renewals
        // retain the original timestamps, including their video/audio spacing.
        final newClock =
            lastVideo >= 0 &&
            (tag.timestamp + offset - lastVideo).abs() > 60000;
        final shift = newClock
            ? (lastVideo > lastAudio ? lastVideo : lastAudio) +
                  1 -
                  tag.timestamp
            : offset;
        if (tag.timestamp + shift <= lastVideo) continue;
        return _Prepared(source, tag, [...configs.values, ...prefix], shift);
      }
    } catch (_) {
      // A failed warmup must not interrupt the healthy connection.
    }
    await source?.close();
    return null;
  }

  void startPreparation() {
    preparationFinished = false;
    preparation = prepare().then((value) {
      ready = value;
      preparationFinished = true;
      return value;
    });
  }

  try {
    active = _Source(await open());
    await active.initialize();
    yield active.header;
    opened = DateTime.now();
    nextAttempt = opened.add(renewAfter);
    while (!closed) {
      if (preparation == null && !DateTime.now().isBefore(nextAttempt))
        startPreparation();
      _Tag? tag;
      try {
        tag = await active!.next();
      } on FormatException {
        rethrow;
      } catch (_) {
        tag = null;
      }
      // Briefly hold the old keyframe so a slightly slower CDN can catch up.
      // Playback keeps draining its existing buffer while this happens.
      if (preparation != null &&
          warming != null &&
          ready == null &&
          (tag == null || tag.keyframe)) {
        await preparation!.timeout(handoverWait, onTimeout: () => null);
      }
      if (ready != null &&
          (tag == null ||
              (tag.media &&
                  tag.timestamp + offset >=
                      ready!.keyframe.timestamp + ready!.offset))) {
        final replacement = ready!;
        await active!.close();
        active = replacement.source;
        warming = null;
        offset = replacement.offset;
        for (final item in replacement.prefix) {
          if (item.configuration) {
            final payload = item.bytes.sublist(11, item.bytes.length - 4);
            final previous = configurations[item.type];
            if (previous != null &&
                previous.length == payload.length &&
                List.generate(
                  payload.length,
                  (i) => previous[i] == payload[i],
                ).every((v) => v))
              continue;
            configurations[item.type] = payload;
            yield item.shifted(
              replacement.keyframe.timestamp + offset - item.timestamp,
            );
          } else if (item.timestamp + offset > lastAudio) {
            lastAudio = item.timestamp + offset;
            yield item.shifted(offset);
          }
        }
        tag = replacement.keyframe;
        ready = null;
        preparation = null;
        opened = DateTime.now();
        nextAttempt = opened.add(renewAfter);
      } else if (preparation != null && ready == null) {
        if (preparationFinished) {
          preparation = null;
          warming = null;
          nextAttempt = DateTime.now().add(retryAfter);
        }
      }
      if (tag == null) {
        if (preparation == null) startPreparation();
        await preparation;
        if (ready == null) return;
        continue;
      }
      if (tag.configuration)
        configurations[tag.type] = tag.bytes.sublist(11, tag.bytes.length - 4);
      if (tag.media) {
        final timestamp = tag.timestamp + offset;
        if (tag.type == 9) {
          if (timestamp <= lastVideo) continue;
          lastVideo = timestamp;
        } else {
          if (timestamp <= lastAudio) continue;
          lastAudio = timestamp;
        }
      }
      yield tag.shifted(offset);
    }
  } finally {
    closed = true;
    await active?.close();
    await warming?.close();
    if (ready != null && ready!.source != active) await ready!.source.close();
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

Future<void> serveDouyuStream(
  HttpRequest request,
  LiveSite site,
  LiveRoomDetail detail,
  LivePlayQuality quality,
  int preferred,
) async {
  final client = HttpClient()..connectionTimeout = const Duration(seconds: 15);
  var first = true;
  Future<Stream<List<int>>> open() async {
    if (!first) {
      detail = await site.getRoomDetail(roomId: detail.roomId);
      if (!detail.status) throw const HttpException('Room offline');
      final qualities = await site.getPlayQualites(detail: detail);
      quality = qualities.firstWhere(
        (q) => q.quality == quality.quality,
        orElse: () => qualities.first,
      );
    }
    first = false;
    final urls = await site.getPlayUrls(detail: detail, quality: quality);
    for (var i = 0; i < urls.urls.length; i++) {
      final index = (preferred + i) % urls.urls.length;
      try {
        final upstream = await client.getUrl(Uri.parse(urls.urls[index]));
        urls.headers?.forEach(upstream.headers.set);
        final response = await upstream.close().timeout(
          const Duration(seconds: 15),
        );
        if (response.statusCode != 200) {
          await response.drain<void>();
          continue;
        }
        preferred = index;
        return response.timeout(const Duration(seconds: 20));
      } on SocketException {
        continue;
      } on HttpException {
        continue;
      } on TimeoutException {
        continue;
      }
    }
    throw const HttpException('No working Douyu connection');
  }

  try {
    request.response.headers.contentType = ContentType('video', 'x-flv');
    request.response.headers.set('Cache-Control', 'no-store');
    await for (final bytes in spliceDouyuFlv(open)) {
      request.response.add(bytes);
      await request.response.flush();
    }
  } catch (_) {
    // Closing a room cancels its response; signed addresses stay out of logs.
  } finally {
    client.close(force: true);
    await request.response.close();
  }
}
