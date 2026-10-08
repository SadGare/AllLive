import 'dart:typed_data';
import 'package:test/test.dart';
import '../bin/douyu_stream.dart';

List<int> feed(List<int> timestamps) {
  final data = <int>[70, 76, 86, 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
  for (final timestamp in timestamps) {
    final tag = Uint8List(11);
    tag[0] = 9;
    tag[3] = 6;
    tag[4] = timestamp >> 16 & 255;
    tag[5] = timestamp >> 8 & 255;
    tag[6] = timestamp & 255;
    tag[7] = timestamp >> 24 & 255;
    data.addAll([...tag, 0x17, 1, 0, 0, 0, 0x65, 0, 0, 0, 17]);
  }
  return data;
}

void main() {
  test('fragmented renewed FLV has one header and continuous media timestamps', () async {
    final continuity = FlvContinuity();
    final result = <int>[];
    for (final input in [feed([1000, 1500]), feed([5000, 5500])]) {
      await for (final chunk in continuity.append(Stream.fromIterable(input.map((byte) => [byte])))) {
        result.addAll(chunk);
      }
    }
    expect(result.length, 13 + 4 * 21);
    final timestamps = <int>[];
    for (var offset = 13; offset < result.length; offset += 21) {
      expect(result[offset], 9);
      timestamps.add((result[offset + 7] << 24) | (result[offset + 4] << 16) |
          (result[offset + 5] << 8) | result[offset + 6]);
    }
    expect(timestamps, [1000, 1500, 1501, 2001]);
  });
  test('truncated media cannot be joined into a corrupt next tag', () async {
    final data = feed([1000])..removeLast();
    await expectLater(FlvContinuity().append(Stream.value(data)).toList(), throwsFormatException);
  });
  test('an HTTP error body cannot become an FLV stream', () async {
    await expectLater(FlvContinuity().append(Stream.value(List.filled(13, 65))).toList(), throwsFormatException);
  });
}
