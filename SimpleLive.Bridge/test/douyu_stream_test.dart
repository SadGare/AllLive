import 'dart:async';
import 'dart:typed_data';
import 'package:test/test.dart';
import '../bin/douyu_stream.dart';

const header = [70, 76, 86, 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
List<int> tag(int t, {int type = 9, bool key = true, bool config = false}) {
  final b = Uint8List(11);
  b[0] = type;
  b[3] = 6;
  b[4] = t >> 16 & 255;
  b[5] = t >> 8 & 255;
  b[6] = t & 255;
  b[7] = t >> 24 & 255;
  return [
    ...b,
    type == 8 ? 0xaf : (key ? 0x17 : 0x27),
    config ? 0 : 1,
    0,
    0,
    0,
    0x65,
    0,
    0,
    0,
    17,
  ];
}

List<int> feed(List<int> times) => [
  ...header,
  for (final t in times) ...tag(t),
];
List<int> timestamps(List<List<int>> chunks, int type) => chunks
    .where((c) => c[0] == type && c[12] == 1)
    .map((c) => (c[7] << 24) | (c[4] << 16) | (c[5] << 8) | c[6])
    .toList();
Future<List<List<int>>> join(List<List<int>> feeds) {
  var i = 0;
  return spliceDouyuFlv(() async {
    if (i == feeds.length) throw StateError('Offline');
    return Stream.fromIterable(feeds[i++].map((b) => [b]));
  }).toList();
}

void main() {
  test(
    'renewal drops overlapping cached frames and retains source timing',
    () async {
      final r = await join([
        feed([1000, 1500]),
        feed([500, 1000, 1500, 2000, 2500]),
      ]);
      expect(r.where((c) => c[0] == 70).length, 1);
      expect(timestamps(r, 9), [1000, 1500, 2000, 2500]);
    },
  );
  test(
    'unseen keyframe preserves audio spacing and unchanged configs',
    () async {
      final r = await join([
        [
          ...header,
          ...tag(0, config: true),
          ...tag(0, type: 8, config: true),
          ...tag(1000),
          ...tag(1490, type: 8),
          ...tag(1500),
        ],
        [
          ...header,
          ...tag(0, config: true),
          ...tag(0, type: 8, config: true),
          ...tag(1000),
          ...tag(1490, type: 8),
          ...tag(1800, key: false),
          ...tag(1990, type: 8),
          ...tag(2000),
          ...tag(2020, type: 8),
          ...tag(2033, key: false),
        ],
      ]);
      expect(timestamps(r, 9), [1000, 1500, 2000, 2033]);
      expect(timestamps(r, 8), [1490, 1990, 2020]);
      expect(r.where((c) => c[0] != 70 && c[12] == 0).length, 2);
    },
  );
  test('fresh connection opens before old connection ends', () async {
    var opens = 0;
    var oldEnded = false;
    Stream<List<int>> old() async* {
      yield header;
      for (final t in [1000, 1500, 2000, 2500, 3000]) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
        yield tag(t);
      }
      oldEnded = true;
    }

    final r = await spliceDouyuFlv(() async {
      if (++opens == 1) return old();
      if (opens == 2) {
        expect(oldEnded, false);
        return Stream.value(feed([1000, 1500, 2000, 2500, 3000, 3500]));
      }
      throw StateError('Offline');
    }, renewAfter: const Duration(milliseconds: 25)).toList();
    expect(timestamps(r, 9), [1000, 1500, 2000, 2500, 3000, 3500]);
  });
  test('failed warmup leaves old media intact', () async {
    var opens = 0;
    final r = await spliceDouyuFlv(() async {
      if (++opens != 1) throw StateError('Temporary failure');
      return Stream.value(feed([1000, 1500, 2000, 2500]));
    }, renewAfter: Duration.zero).toList();
    expect(timestamps(r, 9), [1000, 1500, 2000, 2500]);
  });
  test('slow address resolution does not hold old keyframes', () async {
    var opens = 0;
    var oldEnded = false;
    Stream<List<int>> old() async* {
      yield header;
      for (final t in [1000, 1500, 2000, 2500]) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
        yield tag(t);
      }
      oldEnded = true;
    }

    final r = await spliceDouyuFlv(() async {
      if (++opens == 1) return old();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(oldEnded, true);
      throw StateError('Unavailable');
    }, renewAfter: Duration.zero).toList();
    expect(timestamps(r, 9), [1000, 1500, 2000, 2500]);
  });
  test('broadcast clock reset maps once at the first keyframe', () async {
    final r = await join([
      feed([100000, 100500]),
      feed([0, 500]),
    ]);
    expect(timestamps(r, 9), [100000, 100500, 100501, 101001]);
  });
  test('truncated media cannot become a corrupt tag', () async {
    await expectLater(
      join([
        feed([1000])..removeLast(),
      ]),
      throwsFormatException,
    );
  });
  test('HTTP error cannot become FLV', () async {
    await expectLater(join([List.filled(13, 65)]), throwsFormatException);
  });
}
