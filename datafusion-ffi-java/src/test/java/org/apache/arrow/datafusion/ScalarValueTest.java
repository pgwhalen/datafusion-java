package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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

  private static void callTestScalar(Arena arena, int typeTag, MemorySegment scalarOut) {
    NativeUtil.call(
        arena,
        "Test scalar value",
        errorOut ->
            (int) ScalarValueFfi.TEST_SCALAR_VALUE.invokeExact(typeTag, scalarOut, errorOut));
  }
}
