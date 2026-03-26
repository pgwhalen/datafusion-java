package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

/** Tests for {@link ArrowTypeProtoConverter} round-trip serialization. */
public class ArrowTypeProtoConverterTest {

  @Test
  void testNullType() {
    assertRoundTrip(ArrowType.Null.INSTANCE);
  }

  @Test
  void testBoolType() {
    assertRoundTrip(ArrowType.Bool.INSTANCE);
  }

  // -- Integers --

  @Test
  void testInt8Signed() {
    assertRoundTrip(new ArrowType.Int(8, true));
  }

  @Test
  void testInt8Unsigned() {
    assertRoundTrip(new ArrowType.Int(8, false));
  }

  @Test
  void testInt16Signed() {
    assertRoundTrip(new ArrowType.Int(16, true));
  }

  @Test
  void testInt16Unsigned() {
    assertRoundTrip(new ArrowType.Int(16, false));
  }

  @Test
  void testInt32Signed() {
    assertRoundTrip(new ArrowType.Int(32, true));
  }

  @Test
  void testInt32Unsigned() {
    assertRoundTrip(new ArrowType.Int(32, false));
  }

  @Test
  void testInt64Signed() {
    assertRoundTrip(new ArrowType.Int(64, true));
  }

  @Test
  void testInt64Unsigned() {
    assertRoundTrip(new ArrowType.Int(64, false));
  }

  // -- Floats --

  @Test
  void testFloatHalf() {
    assertRoundTrip(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF));
  }

  @Test
  void testFloatSingle() {
    assertRoundTrip(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
  }

  @Test
  void testFloatDouble() {
    assertRoundTrip(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
  }

  // -- Strings --

  @Test
  void testUtf8() {
    assertRoundTrip(ArrowType.Utf8.INSTANCE);
  }

  @Test
  void testUtf8View() {
    assertRoundTrip(new ArrowType.Utf8View());
  }

  @Test
  void testLargeUtf8() {
    assertRoundTrip(ArrowType.LargeUtf8.INSTANCE);
  }

  // -- Binary --

  @Test
  void testBinary() {
    assertRoundTrip(ArrowType.Binary.INSTANCE);
  }

  @Test
  void testBinaryView() {
    assertRoundTrip(new ArrowType.BinaryView());
  }

  @Test
  void testFixedSizeBinary() {
    assertRoundTrip(new ArrowType.FixedSizeBinary(16));
  }

  @Test
  void testLargeBinary() {
    assertRoundTrip(ArrowType.LargeBinary.INSTANCE);
  }

  // -- Dates --

  @Test
  void testDate32() {
    assertRoundTrip(new ArrowType.Date(DateUnit.DAY));
  }

  @Test
  void testDate64() {
    assertRoundTrip(new ArrowType.Date(DateUnit.MILLISECOND));
  }

  // -- Duration --

  @Test
  void testDurationSecond() {
    assertRoundTrip(new ArrowType.Duration(TimeUnit.SECOND));
  }

  @Test
  void testDurationMillisecond() {
    assertRoundTrip(new ArrowType.Duration(TimeUnit.MILLISECOND));
  }

  @Test
  void testDurationMicrosecond() {
    assertRoundTrip(new ArrowType.Duration(TimeUnit.MICROSECOND));
  }

  @Test
  void testDurationNanosecond() {
    assertRoundTrip(new ArrowType.Duration(TimeUnit.NANOSECOND));
  }

  // -- Timestamps --

  @Test
  void testTimestampSecondWithTz() {
    assertRoundTrip(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"));
  }

  @Test
  void testTimestampMillisecondNoTz() {
    assertRoundTrip(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null));
  }

  @Test
  void testTimestampMicrosecond() {
    assertRoundTrip(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "America/New_York"));
  }

  @Test
  void testTimestampNanosecond() {
    assertRoundTrip(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));
  }

  // -- Time --

  @Test
  void testTime32Second() {
    assertRoundTrip(new ArrowType.Time(TimeUnit.SECOND, 32));
  }

  @Test
  void testTime32Millisecond() {
    assertRoundTrip(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
  }

  @Test
  void testTime64Microsecond() {
    assertRoundTrip(new ArrowType.Time(TimeUnit.MICROSECOND, 64));
  }

  @Test
  void testTime64Nanosecond() {
    assertRoundTrip(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
  }

  // -- Interval --

  @Test
  void testIntervalYearMonth() {
    assertRoundTrip(new ArrowType.Interval(IntervalUnit.YEAR_MONTH));
  }

  @Test
  void testIntervalDayTime() {
    assertRoundTrip(new ArrowType.Interval(IntervalUnit.DAY_TIME));
  }

  @Test
  void testIntervalMonthDayNano() {
    assertRoundTrip(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));
  }

  // -- Decimals --

  @Test
  void testDecimal32() {
    assertRoundTrip(new ArrowType.Decimal(9, 2, 32));
  }

  @Test
  void testDecimal64() {
    assertRoundTrip(new ArrowType.Decimal(18, 6, 64));
  }

  @Test
  void testDecimal128() {
    assertRoundTrip(new ArrowType.Decimal(38, 10, 128));
  }

  @Test
  void testDecimal256() {
    assertRoundTrip(new ArrowType.Decimal(76, 20, 256));
  }

  // -- Edge cases --

  @Test
  void testNullInputReturnsNull() {
    assertNull(ArrowTypeProtoConverter.toProto(null));
  }

  @Test
  void testNullProtoReturnsNull() {
    assertNull(ArrowTypeProtoConverter.fromProto(null));
  }

  @Test
  void testDefaultProtoReturnsNull() {
    var proto = org.apache.arrow.datafusion.proto.ArrowType.getDefaultInstance();
    assertNull(ArrowTypeProtoConverter.fromProto(proto));
  }

  // -- Helper --

  private static void assertRoundTrip(ArrowType original) {
    var proto = ArrowTypeProtoConverter.toProto(original);
    assertNotNull(proto, "toProto should not return null for " + original);
    ArrowType result = ArrowTypeProtoConverter.fromProto(proto);
    assertEquals(original, result, "Round-trip failed for " + original);
  }
}
