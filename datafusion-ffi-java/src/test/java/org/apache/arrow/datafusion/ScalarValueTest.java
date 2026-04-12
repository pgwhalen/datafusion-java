package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;
import java.util.Map;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.vector.PeriodDuration;
import org.junit.jupiter.api.Test;

/** Tests for ScalarValue types, getObject() semantics, and subinterfaces. */
public class ScalarValueTest {

  // -- getObject() tests --

  @Test
  void testGetObjectPrimitiveTypes() {
    assertEquals((byte) 42, new ScalarValue.Int8((byte) 42).getObject());
    assertEquals((short) 1234, new ScalarValue.Int16((short) 1234).getObject());
    assertEquals(123456, new ScalarValue.Int32(123456).getObject());
    assertEquals(1234567890L, new ScalarValue.Int64(1234567890L).getObject());
    assertEquals((short) 200, new ScalarValue.UInt8((short) 200).getObject());
    assertEquals(60000, new ScalarValue.UInt16(60000).getObject());
    assertEquals(3000000000L, new ScalarValue.UInt32(3000000000L).getObject());
    assertEquals(
        new BigInteger("18000000000000000000"),
        new ScalarValue.UInt64(new BigInteger("18000000000000000000")).getObject());
    assertEquals(3.14f, (float) new ScalarValue.Float16(3.14f).getObject(), 0.01f);
    assertEquals(3.14f, new ScalarValue.Float32(3.14f).getObject());
    assertEquals(2.718, new ScalarValue.Float64(2.718).getObject());
    assertEquals(true, new ScalarValue.BooleanValue(true).getObject());
    assertNull(new ScalarValue.Null().getObject());
  }

  @Test
  void testGetObjectStringTypes() {
    assertEquals("hello", new ScalarValue.Utf8("hello").getObject());
    assertEquals("large", new ScalarValue.LargeUtf8("large").getObject());
    assertEquals("view", new ScalarValue.Utf8View("view").getObject());
  }

  @Test
  void testGetObjectBinaryTypes() {
    byte[] data = {1, 2, 3};
    assertSame(data, new ScalarValue.Binary(data).getObject());
    assertSame(data, new ScalarValue.LargeBinary(data).getObject());
    assertSame(data, new ScalarValue.BinaryView(data).getObject());
    assertSame(data, new ScalarValue.FixedSizeBinary(data).getObject());
  }

  @Test
  void testGetObjectDateTypes() {
    // Date32: days since epoch. 19000 days = 2022-01-15
    assertEquals(LocalDate.ofEpochDay(19000), new ScalarValue.Date32(19000).getObject());

    // Date64: milliseconds since epoch. 1640995200000 ms = 2022-01-01T00:00:00Z
    assertEquals(
        LocalDate.ofEpochDay(1640995200000L / 86_400_000L),
        new ScalarValue.Date64(1640995200000L).getObject());
  }

  @Test
  void testGetObjectTimeTypes() {
    // Time32Second(3600) = 01:00:00
    assertEquals(LocalTime.of(1, 0), new ScalarValue.Time32Second(3600).getObject());

    // Time32Millisecond(3600000) = 01:00:00
    assertEquals(LocalTime.of(1, 0), new ScalarValue.Time32Millisecond(3600000).getObject());

    // Time64Microsecond(3600000000) = 01:00:00
    assertEquals(LocalTime.of(1, 0), new ScalarValue.Time64Microsecond(3600000000L).getObject());

    // Time64Nanosecond(3600000000000) = 01:00:00
    assertEquals(LocalTime.of(1, 0), new ScalarValue.Time64Nanosecond(3600000000000L).getObject());
  }

  @Test
  void testGetObjectTimestampTypes() {
    // All timestamp test values = 2022-01-01T00:00:00Z (1640995200 seconds since epoch)
    LocalDateTime expected = LocalDateTime.of(2022, 1, 1, 0, 0, 0);

    assertEquals(expected, new ScalarValue.TimestampSecond(1640995200L, null).getObject());
    assertEquals(expected, new ScalarValue.TimestampMillisecond(1640995200000L, null).getObject());
    assertEquals(
        expected, new ScalarValue.TimestampMicrosecond(1640995200000000L, null).getObject());
    assertEquals(
        expected, new ScalarValue.TimestampNanosecond(1640995200000000000L, null).getObject());
  }

  @Test
  void testGetObjectTimestampTzAware() {
    // When tz is non-null, getObject() should return Instant
    Instant expected = Instant.ofEpochSecond(1640995200L);

    Object secObj = new ScalarValue.TimestampSecond(1640995200L, "UTC").getObject();
    assertInstanceOf(Instant.class, secObj);
    assertEquals(expected, secObj);

    Object msObj =
        new ScalarValue.TimestampMillisecond(1640995200000L, "America/New_York").getObject();
    assertInstanceOf(Instant.class, msObj);
    assertEquals(expected, msObj);

    Object usObj =
        new ScalarValue.TimestampMicrosecond(1640995200000000L, "Europe/London").getObject();
    assertInstanceOf(Instant.class, usObj);
    assertEquals(expected, usObj);

    Object nsObj = new ScalarValue.TimestampNanosecond(1640995200000000000L, "UTC").getObject();
    assertInstanceOf(Instant.class, nsObj);
    assertEquals(expected, nsObj);
  }

  @Test
  void testGetObjectDurationTypes() {
    // All duration test values = 1 hour (3600 seconds)
    Duration oneHour = Duration.ofHours(1);

    assertEquals(oneHour, new ScalarValue.DurationSecond(3600L).getObject());
    assertEquals(oneHour, new ScalarValue.DurationMillisecond(3600000L).getObject());
    assertEquals(oneHour, new ScalarValue.DurationMicrosecond(3600000000L).getObject());
    assertEquals(oneHour, new ScalarValue.DurationNanosecond(3600000000000L).getObject());
  }

  @Test
  void testGetObjectIntervalTypes() {
    // IntervalYearMonth(14) = 1 year 2 months
    assertEquals(Period.ofMonths(14), new ScalarValue.IntervalYearMonth(14).getObject());

    // IntervalDayTime(5 days, 3600000 ms) = 5 days + 1 hour as Duration
    assertEquals(
        Duration.ofDays(5).plusHours(1), new ScalarValue.IntervalDayTime(5, 3600000).getObject());

    // IntervalMonthDayNano(2 months, 10 days, 500000000 nanos) = PeriodDuration
    Object mdn = new ScalarValue.IntervalMonthDayNano(2, 10, 500000000L).getObject();
    assertInstanceOf(PeriodDuration.class, mdn);
    PeriodDuration pd = (PeriodDuration) mdn;
    assertEquals(Period.of(0, 2, 10), pd.getPeriod());
    assertEquals(Duration.ofNanos(500000000L), pd.getDuration());
  }

  @Test
  void testGetObjectDecimalTypes() {
    // Decimal32(12345, 7, 2) = 123.45
    assertEquals(new BigDecimal("123.45"), new ScalarValue.Decimal32(12345, 7, 2).getObject());

    // Decimal64(123456789, 12, 3) = 123456.789
    assertEquals(
        new BigDecimal("123456.789"), new ScalarValue.Decimal64(123456789L, 12, 3).getObject());

    // Decimal128: unscaled 1234567890123456789, scale 5 = 12345678901234.56789
    assertEquals(
        new BigDecimal("12345678901234.56789"),
        new ScalarValue.Decimal128(new BigInteger("1234567890123456789"), 20, 5).getObject());

    // Decimal256: unscaled 9876543210123456789, scale 10 = 987654321.0123456789
    assertEquals(
        new BigDecimal("987654321.0123456789"),
        new ScalarValue.Decimal256(new BigInteger("9876543210123456789"), 30, 10).getObject());
  }

  @Test
  void testGetObjectListValue() {
    var list =
        new ScalarValue.ListValue(List.of(new ScalarValue.Int32(1), new ScalarValue.Int32(2)));
    assertEquals(List.of(1, 2), list.getObject());
  }

  @Test
  void testGetObjectLargeListValue() {
    var list =
        new ScalarValue.LargeListValue(
            List.of(new ScalarValue.Utf8("a"), new ScalarValue.Utf8("b")));
    assertEquals(List.of("a", "b"), list.getObject());
  }

  @Test
  void testGetObjectFixedSizeListValue() {
    var list =
        new ScalarValue.FixedSizeListValue(
            List.of(new ScalarValue.Float64(1.0), new ScalarValue.Float64(2.0)), 2);
    assertEquals(List.of(1.0, 2.0), list.getObject());
    assertEquals(2, list.listSize());
  }

  @Test
  void testGetObjectStructValue() {
    var struct =
        new ScalarValue.StructValue(
            Map.of("name", new ScalarValue.Utf8("Alice"), "age", new ScalarValue.Int32(30)));
    @SuppressWarnings("unchecked")
    var result = (Map<String, Object>) struct.getObject();
    assertEquals("Alice", result.get("name"));
    assertEquals(30, result.get("age"));
  }

  @Test
  void testGetObjectMapValue() {
    var map =
        new ScalarValue.MapValue(
            List.of(
                Map.entry(new ScalarValue.Utf8("key1"), new ScalarValue.Int32(10)),
                Map.entry(new ScalarValue.Utf8("key2"), new ScalarValue.Int32(20))));
    @SuppressWarnings("unchecked")
    var result = (Map<Object, Object>) map.getObject();
    assertEquals(10, result.get("key1"));
    assertEquals(20, result.get("key2"));
  }

  @Test
  void testGetObjectUnionValue() {
    var union = new ScalarValue.UnionValue(0, new ScalarValue.Int32(42));
    assertEquals(42, union.getObject());
    assertEquals(0, union.typeId());
  }

  @Test
  void testGetObjectDictionaryValue() {
    var dict = new ScalarValue.DictionaryValue(new ScalarValue.Utf8("hello"));
    assertEquals("hello", dict.getObject());
  }

  @Test
  void testGetObjectRunEndEncodedValue() {
    var ree = new ScalarValue.RunEndEncodedValue(new ScalarValue.Int64(100L));
    assertEquals(100L, ree.getObject());
  }

  // -- Subinterface tests --

  @Test
  void testDecimalValueSubinterface() {
    ScalarValue.DecimalValue d32 = new ScalarValue.Decimal32(12345, 7, 2);
    assertEquals(new BigDecimal("123.45"), d32.toBigDecimal());
    assertEquals(7, d32.precision());
    assertEquals(2, d32.scale());

    ScalarValue.DecimalValue d64 = new ScalarValue.Decimal64(123456789L, 12, 3);
    assertEquals(new BigDecimal("123456.789"), d64.toBigDecimal());
    assertEquals(12, d64.precision());
    assertEquals(3, d64.scale());

    ScalarValue.DecimalValue d128 =
        new ScalarValue.Decimal128(new BigInteger("1234567890123456789"), 20, 5);
    assertEquals(new BigDecimal("12345678901234.56789"), d128.toBigDecimal());
    assertEquals(20, d128.precision());
    assertEquals(5, d128.scale());

    ScalarValue.DecimalValue d256 =
        new ScalarValue.Decimal256(new BigInteger("9876543210123456789"), 30, 10);
    assertEquals(new BigDecimal("987654321.0123456789"), d256.toBigDecimal());
    assertEquals(30, d256.precision());
    assertEquals(10, d256.scale());
  }

  @Test
  void testDurationValueSubinterface() {
    Duration oneHour = Duration.ofHours(1);

    ScalarValue.DurationValue ds = new ScalarValue.DurationSecond(3600L);
    assertEquals(oneHour, ds.toDuration());

    ScalarValue.DurationValue dms = new ScalarValue.DurationMillisecond(3600000L);
    assertEquals(oneHour, dms.toDuration());

    ScalarValue.DurationValue dus = new ScalarValue.DurationMicrosecond(3600000000L);
    assertEquals(oneHour, dus.toDuration());

    ScalarValue.DurationValue dns = new ScalarValue.DurationNanosecond(3600000000000L);
    assertEquals(oneHour, dns.toDuration());

    // IntervalDayTime also implements DurationValue
    ScalarValue.DurationValue idt = new ScalarValue.IntervalDayTime(5, 3600000);
    assertEquals(Duration.ofDays(5).plusHours(1), idt.toDuration());
  }

  @Test
  void testTimestampValueSubinterface() {
    Instant expected = Instant.ofEpochSecond(1640995200L);

    ScalarValue.TimestampValue ts = new ScalarValue.TimestampSecond(1640995200L, "UTC");
    assertEquals(expected, ts.toInstant());
    assertEquals("UTC", ts.tz());

    ScalarValue.TimestampValue tms = new ScalarValue.TimestampMillisecond(1640995200000L, null);
    assertEquals(expected, tms.toInstant());
    assertNull(tms.tz());

    ScalarValue.TimestampValue tus = new ScalarValue.TimestampMicrosecond(1640995200000000L, null);
    assertEquals(expected, tus.toInstant());

    ScalarValue.TimestampValue tns =
        new ScalarValue.TimestampNanosecond(1640995200000000000L, null);
    assertEquals(expected, tns.toInstant());
  }

  @Test
  void testDateValueSubinterface() {
    ScalarValue.DateValue d32 = new ScalarValue.Date32(19000);
    assertEquals(LocalDate.ofEpochDay(19000), d32.toLocalDate());

    ScalarValue.DateValue d64 = new ScalarValue.Date64(1640995200000L);
    assertEquals(LocalDate.of(2022, 1, 1), d64.toLocalDate());
  }

  @Test
  void testTimeValueSubinterface() {
    LocalTime oneAm = LocalTime.of(1, 0);

    ScalarValue.TimeValue t32s = new ScalarValue.Time32Second(3600);
    assertEquals(oneAm, t32s.toLocalTime());

    ScalarValue.TimeValue t32ms = new ScalarValue.Time32Millisecond(3600000);
    assertEquals(oneAm, t32ms.toLocalTime());

    ScalarValue.TimeValue t64us = new ScalarValue.Time64Microsecond(3600000000L);
    assertEquals(oneAm, t64us.toLocalTime());

    ScalarValue.TimeValue t64ns = new ScalarValue.Time64Nanosecond(3600000000000L);
    assertEquals(oneAm, t64ns.toLocalTime());
  }

  @Test
  void testSubinterfaceInstanceof() {
    // DecimalValue
    assertInstanceOf(ScalarValue.DecimalValue.class, new ScalarValue.Decimal32(1, 5, 2));
    assertInstanceOf(ScalarValue.DecimalValue.class, new ScalarValue.Decimal64(1L, 10, 3));
    assertInstanceOf(
        ScalarValue.DecimalValue.class, new ScalarValue.Decimal128(BigInteger.ONE, 20, 5));
    assertInstanceOf(
        ScalarValue.DecimalValue.class, new ScalarValue.Decimal256(BigInteger.TEN, 30, 10));

    // DurationValue
    assertInstanceOf(ScalarValue.DurationValue.class, new ScalarValue.DurationSecond(1L));
    assertInstanceOf(ScalarValue.DurationValue.class, new ScalarValue.DurationMillisecond(1L));
    assertInstanceOf(ScalarValue.DurationValue.class, new ScalarValue.DurationMicrosecond(1L));
    assertInstanceOf(ScalarValue.DurationValue.class, new ScalarValue.DurationNanosecond(1L));
    assertInstanceOf(ScalarValue.DurationValue.class, new ScalarValue.IntervalDayTime(1, 0));

    // TimestampValue
    assertInstanceOf(ScalarValue.TimestampValue.class, new ScalarValue.TimestampSecond(0L, null));
    assertInstanceOf(
        ScalarValue.TimestampValue.class, new ScalarValue.TimestampMillisecond(0L, null));
    assertInstanceOf(
        ScalarValue.TimestampValue.class, new ScalarValue.TimestampMicrosecond(0L, null));
    assertInstanceOf(
        ScalarValue.TimestampValue.class, new ScalarValue.TimestampNanosecond(0L, null));

    // DateValue
    assertInstanceOf(ScalarValue.DateValue.class, new ScalarValue.Date32(0));
    assertInstanceOf(ScalarValue.DateValue.class, new ScalarValue.Date64(0L));

    // TimeValue
    assertInstanceOf(ScalarValue.TimeValue.class, new ScalarValue.Time32Second(0));
    assertInstanceOf(ScalarValue.TimeValue.class, new ScalarValue.Time32Millisecond(0));
    assertInstanceOf(ScalarValue.TimeValue.class, new ScalarValue.Time64Microsecond(0L));
    assertInstanceOf(ScalarValue.TimeValue.class, new ScalarValue.Time64Nanosecond(0L));

    // IntegerValue
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.Int8((byte) 1));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.Int16((short) 1));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.Int32(1));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.Int64(1L));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.UInt8((short) 1));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.UInt16(1));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.UInt32(1L));
    assertInstanceOf(ScalarValue.IntegerValue.class, new ScalarValue.UInt64(BigInteger.ONE));

    // FloatValue
    assertInstanceOf(ScalarValue.FloatValue.class, new ScalarValue.Float16(1.0f));
    assertInstanceOf(ScalarValue.FloatValue.class, new ScalarValue.Float32(1.0f));
    assertInstanceOf(ScalarValue.FloatValue.class, new ScalarValue.Float64(1.0));

    // StringValue
    assertInstanceOf(ScalarValue.StringValue.class, new ScalarValue.Utf8("a"));
    assertInstanceOf(ScalarValue.StringValue.class, new ScalarValue.LargeUtf8("a"));
    assertInstanceOf(ScalarValue.StringValue.class, new ScalarValue.Utf8View("a"));

    // BinaryValue
    assertInstanceOf(ScalarValue.BinaryValue.class, new ScalarValue.Binary(new byte[] {}));
    assertInstanceOf(ScalarValue.BinaryValue.class, new ScalarValue.LargeBinary(new byte[] {}));
    assertInstanceOf(ScalarValue.BinaryValue.class, new ScalarValue.BinaryView(new byte[] {}));
    assertInstanceOf(ScalarValue.BinaryValue.class, new ScalarValue.FixedSizeBinary(new byte[] {}));

    // Non-subinterface types are prevented from implementing subinterfaces at compile time
    // by the sealed interface hierarchy (e.g., Int32 cannot be cast to DecimalValue).
  }

  @Test
  void testIntegerValueSubinterface() {
    ScalarValue.IntegerValue i8 = new ScalarValue.Int8((byte) 42);
    assertEquals(42L, i8.toLong());
    assertEquals(BigInteger.valueOf(42), i8.toBigInteger());

    ScalarValue.IntegerValue i16 = new ScalarValue.Int16((short) -1234);
    assertEquals(-1234L, i16.toLong());
    assertEquals(BigInteger.valueOf(-1234), i16.toBigInteger());

    ScalarValue.IntegerValue i32 = new ScalarValue.Int32(123456);
    assertEquals(123456L, i32.toLong());
    assertEquals(BigInteger.valueOf(123456), i32.toBigInteger());

    ScalarValue.IntegerValue i64 = new ScalarValue.Int64(9876543210L);
    assertEquals(9876543210L, i64.toLong());
    assertEquals(BigInteger.valueOf(9876543210L), i64.toBigInteger());

    ScalarValue.IntegerValue u8 = new ScalarValue.UInt8((short) 200);
    assertEquals(200L, u8.toLong());
    assertEquals(BigInteger.valueOf(200), u8.toBigInteger());

    ScalarValue.IntegerValue u16 = new ScalarValue.UInt16(60000);
    assertEquals(60000L, u16.toLong());
    assertEquals(BigInteger.valueOf(60000), u16.toBigInteger());

    ScalarValue.IntegerValue u32 = new ScalarValue.UInt32(3000000000L);
    assertEquals(3000000000L, u32.toLong());
    assertEquals(BigInteger.valueOf(3000000000L), u32.toBigInteger());

    // UInt64 within long range
    ScalarValue.IntegerValue u64 = new ScalarValue.UInt64(BigInteger.valueOf(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, u64.toLong());
    assertEquals(BigInteger.valueOf(Long.MAX_VALUE), u64.toBigInteger());

    // UInt64 exceeding long range — toBigInteger() works, toLong() throws
    ScalarValue.IntegerValue u64Large =
        new ScalarValue.UInt64(new BigInteger("18000000000000000000"));
    assertEquals(new BigInteger("18000000000000000000"), u64Large.toBigInteger());
    assertThrows(ArithmeticException.class, u64Large::toLong);
  }

  @Test
  void testFloatValueSubinterface() {
    ScalarValue.FloatValue f16 = new ScalarValue.Float16(3.14f);
    assertEquals(3.14f, f16.toDouble(), 0.01);

    ScalarValue.FloatValue f32 = new ScalarValue.Float32(2.718f);
    assertEquals(2.718f, f32.toDouble(), 0.001);

    ScalarValue.FloatValue f64 = new ScalarValue.Float64(1.41421356);
    assertEquals(1.41421356, f64.toDouble(), 0.00000001);
  }

  @Test
  void testStringValueSubinterface() {
    ScalarValue.StringValue utf8 = new ScalarValue.Utf8("hello");
    assertEquals("hello", utf8.value());

    ScalarValue.StringValue largeUtf8 = new ScalarValue.LargeUtf8("world");
    assertEquals("world", largeUtf8.value());

    ScalarValue.StringValue utf8View = new ScalarValue.Utf8View("view");
    assertEquals("view", utf8View.value());
  }

  @Test
  void testBinaryValueSubinterface() {
    byte[] data = {1, 2, 3};

    ScalarValue.BinaryValue bin = new ScalarValue.Binary(data);
    assertSame(data, bin.value());

    ScalarValue.BinaryValue largeBin = new ScalarValue.LargeBinary(data);
    assertSame(data, largeBin.value());

    ScalarValue.BinaryValue binView = new ScalarValue.BinaryView(data);
    assertSame(data, binView.value());

    ScalarValue.BinaryValue fixedBin = new ScalarValue.FixedSizeBinary(data);
    assertSame(data, fixedBin.value());
  }

  @Test
  void testListScalarValueSubinterface() {
    ScalarValue.ListScalarValue list =
        new ScalarValue.ListValue(List.of(new ScalarValue.Int32(1), new ScalarValue.Int32(2)));
    assertEquals(List.of(new ScalarValue.Int32(1), new ScalarValue.Int32(2)), list.values());
    assertEquals(List.of(1, 2), list.toList());

    ScalarValue.ListScalarValue largeList =
        new ScalarValue.LargeListValue(List.of(new ScalarValue.Utf8("a")));
    assertEquals(List.of(new ScalarValue.Utf8("a")), largeList.values());
    assertEquals(List.of("a"), largeList.toList());

    ScalarValue.ListScalarValue fixedList =
        new ScalarValue.FixedSizeListValue(
            List.of(new ScalarValue.Float64(1.0), new ScalarValue.Float64(2.0)), 2);
    assertEquals(
        List.of(new ScalarValue.Float64(1.0), new ScalarValue.Float64(2.0)), fixedList.values());
    assertEquals(List.of(1.0, 2.0), fixedList.toList());
  }

  @Test
  void testMapScalarValueSubinterface() {
    ScalarValue.MapScalarValue struct =
        new ScalarValue.StructValue(Map.of("x", new ScalarValue.Int32(1)));
    assertInstanceOf(ScalarValue.MapScalarValue.class, struct);

    ScalarValue.MapScalarValue map =
        new ScalarValue.MapValue(
            List.of(Map.entry(new ScalarValue.Utf8("k"), new ScalarValue.Int32(10))));
    assertInstanceOf(ScalarValue.MapScalarValue.class, map);
  }

  @Test
  void testTimestampWithSubSecondPrecision() {
    // TimestampMillisecond with fractional seconds: 1640995200123 ms
    ScalarValue.TimestampMillisecond tms =
        new ScalarValue.TimestampMillisecond(1640995200123L, null);
    assertEquals(Instant.ofEpochMilli(1640995200123L), tms.toInstant());
    assertEquals(123_000_000, tms.toInstant().getNano());

    // TimestampMicrosecond: 1640995200000456 us
    ScalarValue.TimestampMicrosecond tus =
        new ScalarValue.TimestampMicrosecond(1640995200000456L, null);
    assertEquals(Instant.ofEpochSecond(1640995200L, 456_000L), tus.toInstant());
    assertEquals(456_000, tus.toInstant().getNano());

    // TimestampNanosecond: 1640995200000000789 ns
    ScalarValue.TimestampNanosecond tns =
        new ScalarValue.TimestampNanosecond(1640995200000000789L, null);
    assertEquals(Instant.ofEpochSecond(1640995200L, 789L), tns.toInstant());
    assertEquals(789, tns.toInstant().getNano());
  }
}
