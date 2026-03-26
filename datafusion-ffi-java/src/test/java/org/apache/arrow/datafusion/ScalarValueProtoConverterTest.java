package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.Arrays;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.junit.jupiter.api.Test;

/** Tests for {@link ScalarValueProtoConverter} round-trip serialization. */
public class ScalarValueProtoConverterTest {

  // -- Primitives --

  @Test
  void testBooleanRoundTrip() {
    assertRoundTrip(new ScalarValue.BooleanValue(true));
    assertRoundTrip(new ScalarValue.BooleanValue(false));
  }

  @Test
  void testInt8RoundTrip() {
    assertRoundTrip(new ScalarValue.Int8((byte) 0));
    assertRoundTrip(new ScalarValue.Int8((byte) 127));
    assertRoundTrip(new ScalarValue.Int8((byte) -128));
  }

  @Test
  void testInt16RoundTrip() {
    assertRoundTrip(new ScalarValue.Int16((short) 0));
    assertRoundTrip(new ScalarValue.Int16((short) 32767));
    assertRoundTrip(new ScalarValue.Int16((short) -32768));
  }

  @Test
  void testInt32RoundTrip() {
    assertRoundTrip(new ScalarValue.Int32(0));
    assertRoundTrip(new ScalarValue.Int32(Integer.MAX_VALUE));
    assertRoundTrip(new ScalarValue.Int32(Integer.MIN_VALUE));
  }

  @Test
  void testInt64RoundTrip() {
    assertRoundTrip(new ScalarValue.Int64(0L));
    assertRoundTrip(new ScalarValue.Int64(Long.MAX_VALUE));
    assertRoundTrip(new ScalarValue.Int64(Long.MIN_VALUE));
  }

  @Test
  void testUInt8RoundTrip() {
    assertRoundTrip(new ScalarValue.UInt8((short) 0));
    assertRoundTrip(new ScalarValue.UInt8((short) 255));
  }

  @Test
  void testUInt16RoundTrip() {
    assertRoundTrip(new ScalarValue.UInt16(0));
    assertRoundTrip(new ScalarValue.UInt16(65535));
  }

  @Test
  void testUInt32RoundTrip() {
    assertRoundTrip(new ScalarValue.UInt32(0L));
    assertRoundTrip(new ScalarValue.UInt32(4294967295L));
  }

  @Test
  void testUInt64RoundTrip() {
    assertRoundTrip(new ScalarValue.UInt64(BigInteger.ZERO));
    assertRoundTrip(new ScalarValue.UInt64(BigInteger.valueOf(Long.MAX_VALUE)));
    // Large unsigned value that would be negative as a signed long
    assertRoundTrip(new ScalarValue.UInt64(new BigInteger("18446744073709551615")));
  }

  @Test
  void testFloat32RoundTrip() {
    assertRoundTrip(new ScalarValue.Float32(0.0f));
    assertRoundTrip(new ScalarValue.Float32(3.14f));
    assertRoundTrip(new ScalarValue.Float32(-1.0f));
  }

  @Test
  void testFloat64RoundTrip() {
    assertRoundTrip(new ScalarValue.Float64(0.0));
    assertRoundTrip(new ScalarValue.Float64(3.141592653589793));
    assertRoundTrip(new ScalarValue.Float64(-1.0));
  }

  // -- Strings --

  @Test
  void testUtf8RoundTrip() {
    assertRoundTrip(new ScalarValue.Utf8("hello"));
    assertRoundTrip(new ScalarValue.Utf8(""));
  }

  @Test
  void testLargeUtf8RoundTrip() {
    assertRoundTrip(new ScalarValue.LargeUtf8("hello large"));
    assertRoundTrip(new ScalarValue.LargeUtf8(""));
  }

  @Test
  void testUtf8ViewRoundTrip() {
    assertRoundTrip(new ScalarValue.Utf8View("hello view"));
    assertRoundTrip(new ScalarValue.Utf8View(""));
  }

  // -- Binary --

  @Test
  void testBinaryRoundTrip() {
    assertBinaryRoundTrip(new ScalarValue.Binary(new byte[] {1, 2, 3}));
    assertBinaryRoundTrip(new ScalarValue.Binary(new byte[] {}));
  }

  @Test
  void testLargeBinaryRoundTrip() {
    assertBinaryRoundTrip(new ScalarValue.LargeBinary(new byte[] {4, 5, 6}));
  }

  @Test
  void testBinaryViewRoundTrip() {
    assertBinaryRoundTrip(new ScalarValue.BinaryView(new byte[] {7, 8, 9}));
  }

  @Test
  void testFixedSizeBinaryRoundTrip() {
    assertBinaryRoundTrip(new ScalarValue.FixedSizeBinary(new byte[] {10, 11, 12, 13}));
  }

  // -- Dates --

  @Test
  void testDate32RoundTrip() {
    assertRoundTrip(new ScalarValue.Date32(0));
    assertRoundTrip(new ScalarValue.Date32(19000));
  }

  @Test
  void testDate64RoundTrip() {
    assertRoundTrip(new ScalarValue.Date64(0L));
    assertRoundTrip(new ScalarValue.Date64(1641024000000L));
  }

  // -- Times --

  @Test
  void testTime32SecondRoundTrip() {
    assertRoundTrip(new ScalarValue.Time32Second(0));
    assertRoundTrip(new ScalarValue.Time32Second(86399));
  }

  @Test
  void testTime32MillisecondRoundTrip() {
    assertRoundTrip(new ScalarValue.Time32Millisecond(0));
    assertRoundTrip(new ScalarValue.Time32Millisecond(86399999));
  }

  @Test
  void testTime64MicrosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.Time64Microsecond(0L));
    assertRoundTrip(new ScalarValue.Time64Microsecond(86399999999L));
  }

  @Test
  void testTime64NanosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.Time64Nanosecond(0L));
    assertRoundTrip(new ScalarValue.Time64Nanosecond(86399999999999L));
  }

  // -- Timestamps --

  @Test
  void testTimestampSecondRoundTrip() {
    assertRoundTrip(new ScalarValue.TimestampSecond(1234567890L, "UTC"));
    assertRoundTrip(new ScalarValue.TimestampSecond(1234567890L, null));
  }

  @Test
  void testTimestampMillisecondRoundTrip() {
    assertRoundTrip(new ScalarValue.TimestampMillisecond(1234567890000L, "UTC"));
    assertRoundTrip(new ScalarValue.TimestampMillisecond(1234567890000L, null));
  }

  @Test
  void testTimestampMicrosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.TimestampMicrosecond(1234567890000000L, "UTC"));
    assertRoundTrip(new ScalarValue.TimestampMicrosecond(1234567890000000L, null));
  }

  @Test
  void testTimestampNanosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.TimestampNanosecond(1234567890000000000L, "UTC"));
    assertRoundTrip(new ScalarValue.TimestampNanosecond(1234567890000000000L, null));
  }

  // -- Intervals --

  @Test
  void testIntervalYearMonthRoundTrip() {
    assertRoundTrip(new ScalarValue.IntervalYearMonth(0));
    assertRoundTrip(new ScalarValue.IntervalYearMonth(14));
    assertRoundTrip(new ScalarValue.IntervalYearMonth(-6));
  }

  @Test
  void testIntervalDayTimeRoundTrip() {
    assertRoundTrip(new ScalarValue.IntervalDayTime(5, 3600000));
    assertRoundTrip(new ScalarValue.IntervalDayTime(0, 0));
  }

  @Test
  void testIntervalMonthDayNanoRoundTrip() {
    assertRoundTrip(new ScalarValue.IntervalMonthDayNano(2, 15, 1000000000L));
    assertRoundTrip(new ScalarValue.IntervalMonthDayNano(0, 0, 0L));
  }

  // -- Durations --

  @Test
  void testDurationSecondRoundTrip() {
    assertRoundTrip(new ScalarValue.DurationSecond(3600L));
    assertRoundTrip(new ScalarValue.DurationSecond(0L));
  }

  @Test
  void testDurationMillisecondRoundTrip() {
    assertRoundTrip(new ScalarValue.DurationMillisecond(3600000L));
  }

  @Test
  void testDurationMicrosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.DurationMicrosecond(3600000000L));
  }

  @Test
  void testDurationNanosecondRoundTrip() {
    assertRoundTrip(new ScalarValue.DurationNanosecond(3600000000000L));
  }

  // -- Decimals --

  @Test
  void testDecimal32RoundTrip() {
    assertRoundTrip(new ScalarValue.Decimal32(12345, 9, 2));
    assertRoundTrip(new ScalarValue.Decimal32(-99999, 9, 4));
    assertRoundTrip(new ScalarValue.Decimal32(0, 5, 0));
  }

  @Test
  void testDecimal64RoundTrip() {
    assertRoundTrip(new ScalarValue.Decimal64(123456789012L, 18, 6));
    assertRoundTrip(new ScalarValue.Decimal64(-999999999999L, 18, 2));
    assertRoundTrip(new ScalarValue.Decimal64(0L, 10, 0));
  }

  @Test
  void testDecimal128RoundTrip() {
    assertRoundTrip(new ScalarValue.Decimal128(BigInteger.valueOf(12345), 10, 2));
    assertRoundTrip(new ScalarValue.Decimal128(BigInteger.valueOf(-999999), 38, 6));
    assertRoundTrip(new ScalarValue.Decimal128(BigInteger.ZERO, 38, 0));
    // Large value
    assertRoundTrip(
        new ScalarValue.Decimal128(
            new BigInteger("99999999999999999999999999999999999999"), 38, 0));
  }

  @Test
  void testDecimal256RoundTrip() {
    assertRoundTrip(new ScalarValue.Decimal256(BigInteger.valueOf(12345), 10, 2));
    assertRoundTrip(new ScalarValue.Decimal256(BigInteger.valueOf(-999999), 76, 6));
    assertRoundTrip(new ScalarValue.Decimal256(BigInteger.ZERO, 76, 0));
  }

  // -- Null --

  @Test
  void testNullRoundTrip() {
    assertRoundTrip(new ScalarValue.Null());
  }

  @Test
  void testNullProtoReturnsNull() {
    ScalarValue result = ScalarValueProtoConverter.fromProto(null);
    assertInstanceOf(ScalarValue.Null.class, result);
  }

  @Test
  void testValueNotSetReturnsNull() {
    var proto = org.apache.arrow.datafusion.proto.ScalarValue.getDefaultInstance();
    ScalarValue result = ScalarValueProtoConverter.fromProto(proto);
    assertInstanceOf(ScalarValue.Null.class, result);
  }

  // -- Helpers --

  private static void assertRoundTrip(ScalarValue original) {
    var proto = ScalarValueProtoConverter.toProto(original);
    ScalarValue result = ScalarValueProtoConverter.fromProto(proto);
    assertEquals(original, result, "Round-trip failed for " + original.getClass().getSimpleName());
  }

  /** Binary types use byte arrays which don't have value equality in records. */
  private static void assertBinaryRoundTrip(ScalarValue original) {
    var proto = ScalarValueProtoConverter.toProto(original);
    ScalarValue result = ScalarValueProtoConverter.fromProto(proto);
    assertEquals(original.getClass(), result.getClass());
    byte[] origBytes = extractBytes(original);
    byte[] resultBytes = extractBytes(result);
    assertTrue(
        Arrays.equals(origBytes, resultBytes),
        "Binary data mismatch for " + original.getClass().getSimpleName());
  }

  private static byte[] extractBytes(ScalarValue sv) {
    return switch (sv) {
      case ScalarValue.Binary v -> v.value();
      case ScalarValue.LargeBinary v -> v.value();
      case ScalarValue.BinaryView v -> v.value();
      case ScalarValue.FixedSizeBinary v -> v.value();
      default -> throw new AssertionError("Not a binary type: " + sv);
    };
  }
}
