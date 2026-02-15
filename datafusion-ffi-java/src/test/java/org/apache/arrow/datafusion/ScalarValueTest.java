package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.arrow.vector.PeriodDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for ScalarValue FFI round-trip serialization. */
public class ScalarValueTest {

  // VarHandles for writing type_tag/is_null in testComplexTypesThrow
  private static final VarHandle VH_TYPE_TAG =
      ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT.varHandle(
          java.lang.foreign.MemoryLayout.PathElement.groupElement("type_tag"));
  private static final VarHandle VH_IS_NULL =
      ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT.varHandle(
          java.lang.foreign.MemoryLayout.PathElement.groupElement("is_null"));

  static Stream<Arguments> simpleTypeTagsAndExpectedTypes() {
    return Stream.of(
        Arguments.of(ScalarValueFfi.TAG_NULL, ScalarValue.Null.class),
        Arguments.of(ScalarValueFfi.TAG_BOOLEAN, ScalarValue.BooleanValue.class),
        Arguments.of(ScalarValueFfi.TAG_INT8, ScalarValue.Int8.class),
        Arguments.of(ScalarValueFfi.TAG_INT16, ScalarValue.Int16.class),
        Arguments.of(ScalarValueFfi.TAG_INT32, ScalarValue.Int32.class),
        Arguments.of(ScalarValueFfi.TAG_INT64, ScalarValue.Int64.class),
        Arguments.of(ScalarValueFfi.TAG_UINT8, ScalarValue.UInt8.class),
        Arguments.of(ScalarValueFfi.TAG_UINT16, ScalarValue.UInt16.class),
        Arguments.of(ScalarValueFfi.TAG_UINT32, ScalarValue.UInt32.class),
        Arguments.of(ScalarValueFfi.TAG_UINT64, ScalarValue.UInt64.class),
        Arguments.of(ScalarValueFfi.TAG_FLOAT16, ScalarValue.Float16.class),
        Arguments.of(ScalarValueFfi.TAG_FLOAT32, ScalarValue.Float32.class),
        Arguments.of(ScalarValueFfi.TAG_FLOAT64, ScalarValue.Float64.class),
        Arguments.of(ScalarValueFfi.TAG_UTF8, ScalarValue.Utf8.class),
        Arguments.of(ScalarValueFfi.TAG_LARGE_UTF8, ScalarValue.LargeUtf8.class),
        Arguments.of(ScalarValueFfi.TAG_UTF8_VIEW, ScalarValue.Utf8View.class),
        Arguments.of(ScalarValueFfi.TAG_BINARY, ScalarValue.Binary.class),
        Arguments.of(ScalarValueFfi.TAG_LARGE_BINARY, ScalarValue.LargeBinary.class),
        Arguments.of(ScalarValueFfi.TAG_BINARY_VIEW, ScalarValue.BinaryView.class),
        Arguments.of(ScalarValueFfi.TAG_FIXED_SIZE_BINARY, ScalarValue.FixedSizeBinary.class),
        Arguments.of(ScalarValueFfi.TAG_DATE32, ScalarValue.Date32.class),
        Arguments.of(ScalarValueFfi.TAG_DATE64, ScalarValue.Date64.class),
        Arguments.of(ScalarValueFfi.TAG_TIME32_SECOND, ScalarValue.Time32Second.class),
        Arguments.of(ScalarValueFfi.TAG_TIME32_MILLISECOND, ScalarValue.Time32Millisecond.class),
        Arguments.of(ScalarValueFfi.TAG_TIME64_MICROSECOND, ScalarValue.Time64Microsecond.class),
        Arguments.of(ScalarValueFfi.TAG_TIME64_NANOSECOND, ScalarValue.Time64Nanosecond.class),
        Arguments.of(ScalarValueFfi.TAG_TIMESTAMP_SECOND, ScalarValue.TimestampSecond.class),
        Arguments.of(
            ScalarValueFfi.TAG_TIMESTAMP_MILLISECOND, ScalarValue.TimestampMillisecond.class),
        Arguments.of(
            ScalarValueFfi.TAG_TIMESTAMP_MICROSECOND, ScalarValue.TimestampMicrosecond.class),
        Arguments.of(
            ScalarValueFfi.TAG_TIMESTAMP_NANOSECOND, ScalarValue.TimestampNanosecond.class),
        Arguments.of(ScalarValueFfi.TAG_DURATION_SECOND, ScalarValue.DurationSecond.class),
        Arguments.of(
            ScalarValueFfi.TAG_DURATION_MILLISECOND, ScalarValue.DurationMillisecond.class),
        Arguments.of(
            ScalarValueFfi.TAG_DURATION_MICROSECOND, ScalarValue.DurationMicrosecond.class),
        Arguments.of(ScalarValueFfi.TAG_DURATION_NANOSECOND, ScalarValue.DurationNanosecond.class),
        Arguments.of(ScalarValueFfi.TAG_INTERVAL_YEAR_MONTH, ScalarValue.IntervalYearMonth.class),
        Arguments.of(ScalarValueFfi.TAG_INTERVAL_DAY_TIME, ScalarValue.IntervalDayTime.class),
        Arguments.of(
            ScalarValueFfi.TAG_INTERVAL_MONTH_DAY_NANO, ScalarValue.IntervalMonthDayNano.class),
        Arguments.of(ScalarValueFfi.TAG_DECIMAL32, ScalarValue.Decimal32.class),
        Arguments.of(ScalarValueFfi.TAG_DECIMAL64, ScalarValue.Decimal64.class),
        Arguments.of(ScalarValueFfi.TAG_DECIMAL128, ScalarValue.Decimal128.class),
        Arguments.of(ScalarValueFfi.TAG_DECIMAL256, ScalarValue.Decimal256.class));
  }

  @ParameterizedTest
  @MethodSource("simpleTypeTagsAndExpectedTypes")
  void testRoundTrip(int typeTag, Class<?> expectedType) {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment scalarOut = arena.allocate(ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT);

      NativeUtil.call(
          arena,
          "Test scalar value for tag " + typeTag,
          errorOut ->
              (int) ScalarValueFfi.TEST_SCALAR_VALUE.invokeExact(typeTag, scalarOut, errorOut));

      ScalarValue result = ScalarValueFfi.fromFfi(scalarOut);
      assertNotNull(result, "Result should not be null for type tag " + typeTag);
      assertInstanceOf(
          expectedType,
          result,
          "Type tag " + typeTag + " should produce " + expectedType.getSimpleName());
    }
  }

  @Test
  void testSpecificValues() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment scalarOut = arena.allocate(ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT);

      // Test Boolean
      callTestScalar(arena, ScalarValueFfi.TAG_BOOLEAN, scalarOut);
      assertInstanceOf(ScalarValue.BooleanValue.class, ScalarValueFfi.fromFfi(scalarOut));
      assertEquals(true, ((ScalarValue.BooleanValue) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test Int32
      callTestScalar(arena, ScalarValueFfi.TAG_INT32, scalarOut);
      assertEquals(123456, ((ScalarValue.Int32) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test Int64
      callTestScalar(arena, ScalarValueFfi.TAG_INT64, scalarOut);
      assertEquals(1234567890L, ((ScalarValue.Int64) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test UInt8
      callTestScalar(arena, ScalarValueFfi.TAG_UINT8, scalarOut);
      assertEquals(200, ((ScalarValue.UInt8) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test UInt64
      callTestScalar(arena, ScalarValueFfi.TAG_UINT64, scalarOut);
      assertEquals(
          new BigInteger("18000000000000000000"),
          ((ScalarValue.UInt64) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test Float64
      callTestScalar(arena, ScalarValueFfi.TAG_FLOAT64, scalarOut);
      assertEquals(
          2.718281828, ((ScalarValue.Float64) ScalarValueFfi.fromFfi(scalarOut)).value(), 1e-9);

      // Test Utf8
      callTestScalar(arena, ScalarValueFfi.TAG_UTF8, scalarOut);
      assertEquals("hello", ((ScalarValue.Utf8) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test Binary
      callTestScalar(arena, ScalarValueFfi.TAG_BINARY, scalarOut);
      assertArrayEquals(
          new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
          ((ScalarValue.Binary) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test Date32
      callTestScalar(arena, ScalarValueFfi.TAG_DATE32, scalarOut);
      assertEquals(19000, ((ScalarValue.Date32) ScalarValueFfi.fromFfi(scalarOut)).value());

      // Test IntervalDayTime
      callTestScalar(arena, ScalarValueFfi.TAG_INTERVAL_DAY_TIME, scalarOut);
      ScalarValue.IntervalDayTime idt =
          (ScalarValue.IntervalDayTime) ScalarValueFfi.fromFfi(scalarOut);
      assertEquals(5, idt.days());
      assertEquals(3600000, idt.millis());

      // Test IntervalMonthDayNano
      callTestScalar(arena, ScalarValueFfi.TAG_INTERVAL_MONTH_DAY_NANO, scalarOut);
      ScalarValue.IntervalMonthDayNano imdn =
          (ScalarValue.IntervalMonthDayNano) ScalarValueFfi.fromFfi(scalarOut);
      assertEquals(2, imdn.months());
      assertEquals(10, imdn.days());
      assertEquals(500000000, imdn.nanos());
    }
  }

  static Stream<Arguments> complexTypeTags() {
    return IntStream.of(
            ScalarValueFfi.TAG_LIST,
            ScalarValueFfi.TAG_LARGE_LIST,
            ScalarValueFfi.TAG_FIXED_SIZE_LIST,
            ScalarValueFfi.TAG_STRUCT,
            ScalarValueFfi.TAG_MAP,
            ScalarValueFfi.TAG_UNION,
            ScalarValueFfi.TAG_DICTIONARY,
            ScalarValueFfi.TAG_RUN_END_ENCODED)
        .mapToObj(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("complexTypeTags")
  void testComplexTypesThrow(int typeTag) {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment scalarOut = arena.allocate(ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT);
      // Manually write a struct with the complex type tag and is_null=0
      VH_TYPE_TAG.set(scalarOut, 0L, typeTag);
      VH_IS_NULL.set(scalarOut, 0L, 0);

      assertThrows(
          UnsupportedOperationException.class,
          () -> ScalarValueFfi.fromFfi(scalarOut),
          "Complex type tag " + typeTag + " should throw UnsupportedOperationException");
    }
  }

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
  void testGetObjectComplexTypesThrow() {
    assertThrows(
        UnsupportedOperationException.class, () -> new ScalarValue.ListValue().getObject());
    assertThrows(
        UnsupportedOperationException.class, () -> new ScalarValue.LargeListValue().getObject());
    assertThrows(
        UnsupportedOperationException.class,
        () -> new ScalarValue.FixedSizeListValue().getObject());
    assertThrows(
        UnsupportedOperationException.class, () -> new ScalarValue.StructValue().getObject());
    assertThrows(UnsupportedOperationException.class, () -> new ScalarValue.MapValue().getObject());
    assertThrows(
        UnsupportedOperationException.class, () -> new ScalarValue.UnionValue().getObject());
    assertThrows(
        UnsupportedOperationException.class, () -> new ScalarValue.DictionaryValue().getObject());
    assertThrows(
        UnsupportedOperationException.class,
        () -> new ScalarValue.RunEndEncodedValue().getObject());
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
    LocalDateTime expected = LocalDateTime.of(2022, 1, 1, 0, 0, 0);

    ScalarValue.TimestampValue ts = new ScalarValue.TimestampSecond(1640995200L, "UTC");
    assertEquals(expected, ts.toLocalDateTime());
    assertEquals("UTC", ts.tz());

    ScalarValue.TimestampValue tms = new ScalarValue.TimestampMillisecond(1640995200000L, null);
    assertEquals(expected, tms.toLocalDateTime());
    assertNull(tms.tz());

    ScalarValue.TimestampValue tus = new ScalarValue.TimestampMicrosecond(1640995200000000L, null);
    assertEquals(expected, tus.toLocalDateTime());

    ScalarValue.TimestampValue tns =
        new ScalarValue.TimestampNanosecond(1640995200000000000L, null);
    assertEquals(expected, tns.toLocalDateTime());
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

    // Non-subinterface types are prevented from implementing subinterfaces at compile time
    // by the sealed interface hierarchy (e.g., Int32 cannot be cast to DecimalValue).
  }

  @Test
  void testTimestampWithSubSecondPrecision() {
    // TimestampMillisecond with fractional seconds: 1640995200123 ms = 2022-01-01T00:00:00.123
    ScalarValue.TimestampMillisecond tms =
        new ScalarValue.TimestampMillisecond(1640995200123L, null);
    assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0, 0, 123_000_000), tms.toLocalDateTime());

    // TimestampMicrosecond: 1640995200000456 us = 2022-01-01T00:00:00.000456
    ScalarValue.TimestampMicrosecond tus =
        new ScalarValue.TimestampMicrosecond(1640995200000456L, null);
    assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0, 0, 456_000), tus.toLocalDateTime());

    // TimestampNanosecond: 1640995200000000789 ns = 2022-01-01T00:00:00.000000789
    ScalarValue.TimestampNanosecond tns =
        new ScalarValue.TimestampNanosecond(1640995200000000789L, null);
    assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0, 0, 789), tns.toLocalDateTime());
  }

  @Test
  void testGetObjectFfiRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment scalarOut = arena.allocate(ScalarValueFfi.FFI_SCALAR_VALUE_LAYOUT);

      // Date32 via FFI round-trip should return LocalDate
      callTestScalar(arena, ScalarValueFfi.TAG_DATE32, scalarOut);
      Object date32Obj = ScalarValueFfi.fromFfi(scalarOut).getObject();
      assertInstanceOf(LocalDate.class, date32Obj);
      assertEquals(LocalDate.ofEpochDay(19000), date32Obj);

      // TimestampSecond via FFI round-trip should return LocalDateTime
      callTestScalar(arena, ScalarValueFfi.TAG_TIMESTAMP_SECOND, scalarOut);
      Object tsObj = ScalarValueFfi.fromFfi(scalarOut).getObject();
      assertInstanceOf(LocalDateTime.class, tsObj);
      assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0, 0), tsObj);

      // DurationSecond via FFI round-trip should return Duration
      callTestScalar(arena, ScalarValueFfi.TAG_DURATION_SECOND, scalarOut);
      Object durObj = ScalarValueFfi.fromFfi(scalarOut).getObject();
      assertInstanceOf(Duration.class, durObj);
      assertEquals(Duration.ofHours(1), durObj);

      // Decimal32 via FFI round-trip should return BigDecimal
      callTestScalar(arena, ScalarValueFfi.TAG_DECIMAL32, scalarOut);
      Object decObj = ScalarValueFfi.fromFfi(scalarOut).getObject();
      assertInstanceOf(BigDecimal.class, decObj);
      assertEquals(new BigDecimal("123.45"), decObj);

      // IntervalYearMonth via FFI round-trip should return Period
      callTestScalar(arena, ScalarValueFfi.TAG_INTERVAL_YEAR_MONTH, scalarOut);
      Object ivymObj = ScalarValueFfi.fromFfi(scalarOut).getObject();
      assertInstanceOf(Period.class, ivymObj);
      assertEquals(Period.ofMonths(14), ivymObj);

      // IntervalDayTime via FFI round-trip should return Duration (via DurationValue)
      callTestScalar(arena, ScalarValueFfi.TAG_INTERVAL_DAY_TIME, scalarOut);
      ScalarValue idtSv = ScalarValueFfi.fromFfi(scalarOut);
      assertInstanceOf(ScalarValue.DurationValue.class, idtSv);
      Object idtObj = idtSv.getObject();
      assertInstanceOf(Duration.class, idtObj);
      assertEquals(Duration.ofDays(5).plusHours(1), idtObj);

      // Null via FFI round-trip should return null
      callTestScalar(arena, ScalarValueFfi.TAG_NULL, scalarOut);
      assertNull(ScalarValueFfi.fromFfi(scalarOut).getObject());
    }
  }

  private static void callTestScalar(Arena arena, int typeTag, MemorySegment scalarOut) {
    NativeUtil.call(
        arena,
        "Test scalar value",
        errorOut ->
            (int) ScalarValueFfi.TEST_SCALAR_VALUE.invokeExact(typeTag, scalarOut, errorOut));
  }
}
