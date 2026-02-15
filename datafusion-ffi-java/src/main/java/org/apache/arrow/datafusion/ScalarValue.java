package org.apache.arrow.datafusion;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A scalar value from DataFusion, representing a single typed value.
 *
 * <p>This sealed interface mirrors DataFusion's {@code ScalarValue} enum. Each variant is a record
 * that holds the typed value. Complex types (List, Struct, Map, etc.) are modeled but will throw
 * {@link UnsupportedOperationException} when encountered in FFI deserialization.
 */
public sealed interface ScalarValue {

  // -- Integers --
  record Int8(byte value) implements ScalarValue {}

  record Int16(short value) implements ScalarValue {}

  record Int32(int value) implements ScalarValue {}

  record Int64(long value) implements ScalarValue {}

  record UInt8(short value) implements ScalarValue {}

  record UInt16(int value) implements ScalarValue {}

  record UInt32(long value) implements ScalarValue {}

  record UInt64(BigInteger value) implements ScalarValue {}

  // -- Floats --
  record Float16(float value) implements ScalarValue {}

  record Float32(float value) implements ScalarValue {}

  record Float64(double value) implements ScalarValue {}

  // -- Strings --
  record Utf8(String value) implements ScalarValue {}

  record LargeUtf8(String value) implements ScalarValue {}

  record Utf8View(String value) implements ScalarValue {}

  // -- Binary --
  record Binary(byte[] value) implements ScalarValue {}

  record LargeBinary(byte[] value) implements ScalarValue {}

  record BinaryView(byte[] value) implements ScalarValue {}

  record FixedSizeBinary(byte[] value) implements ScalarValue {}

  // -- Boolean --
  record BooleanValue(boolean value) implements ScalarValue {}

  // -- Null --
  record Null() implements ScalarValue {}

  // -- Dates --
  record Date32(int value) implements ScalarValue {}

  record Date64(long value) implements ScalarValue {}

  // -- Times --
  record Time32Second(int value) implements ScalarValue {}

  record Time32Millisecond(int value) implements ScalarValue {}

  record Time64Microsecond(long value) implements ScalarValue {}

  record Time64Nanosecond(long value) implements ScalarValue {}

  // -- Timestamps --
  record TimestampSecond(long value, String tz) implements ScalarValue {}

  record TimestampMillisecond(long value, String tz) implements ScalarValue {}

  record TimestampMicrosecond(long value, String tz) implements ScalarValue {}

  record TimestampNanosecond(long value, String tz) implements ScalarValue {}

  // -- Durations --
  record DurationSecond(long value) implements ScalarValue {}

  record DurationMillisecond(long value) implements ScalarValue {}

  record DurationMicrosecond(long value) implements ScalarValue {}

  record DurationNanosecond(long value) implements ScalarValue {}

  // -- Intervals --
  record IntervalYearMonth(int value) implements ScalarValue {}

  record IntervalDayTime(int days, int millis) implements ScalarValue {}

  record IntervalMonthDayNano(int months, int days, long nanos) implements ScalarValue {}

  // -- Decimals --
  record Decimal32(int value, int precision, int scale) implements ScalarValue {}

  record Decimal64(long value, int precision, int scale) implements ScalarValue {}

  record Decimal128(BigDecimal value, int precision, int scale) implements ScalarValue {}

  record Decimal256(BigDecimal value, int precision, int scale) implements ScalarValue {}

  // -- Complex types (modeled but unsupported in FFI) --
  record ListValue() implements ScalarValue {}

  record LargeListValue() implements ScalarValue {}

  record FixedSizeListValue() implements ScalarValue {}

  record StructValue() implements ScalarValue {}

  record MapValue() implements ScalarValue {}

  record UnionValue() implements ScalarValue {}

  record DictionaryValue() implements ScalarValue {}

  record RunEndEncodedValue() implements ScalarValue {}
}
