package org.apache.arrow.datafusion;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import org.apache.arrow.vector.PeriodDuration;

/**
 * A scalar value from DataFusion, representing a single typed value.
 *
 * <p>This sealed interface mirrors DataFusion's {@code ScalarValue} enum. Each variant is a record
 * that holds the typed value. Complex types (List, Struct, Map, etc.) are modeled but will throw
 * {@link UnsupportedOperationException} when encountered in FFI deserialization.
 *
 * <p>Use {@link #getObject()} for untyped access returning a friendly Java type, or use {@code
 * instanceof} with sealed subinterfaces ({@link DecimalValue}, {@link DurationValue}, {@link
 * TimestampValue}, {@link DateValue}, {@link TimeValue}) for typed access without casting.
 */
public sealed interface ScalarValue {

  /**
   * Returns this scalar value as its natural Java type.
   *
   * <p>The mapping follows Arrow Java conventions where consistent, and improves where Arrow
   * returns raw primitives. For example, {@code Date32} returns {@link LocalDate} rather than
   * {@code Integer}.
   *
   * @return the value as a standard Java type, or {@code null} for {@link Null}
   * @throws UnsupportedOperationException for complex types (List, Struct, Map, etc.)
   */
  Object getObject();

  /** Sealed subinterface for decimal variants, providing typed {@link BigDecimal} conversion. */
  sealed interface DecimalValue extends ScalarValue
      permits Decimal32, Decimal64, Decimal128, Decimal256 {
    /** Returns the value as a {@link BigDecimal}. */
    BigDecimal toBigDecimal();

    /** Returns the decimal precision. */
    int precision();

    /** Returns the decimal scale. */
    int scale();
  }

  /**
   * Sealed subinterface for duration variants, providing typed {@link Duration} conversion.
   * Includes {@link IntervalDayTime} since Arrow treats it as a Duration.
   */
  sealed interface DurationValue extends ScalarValue
      permits DurationSecond,
          DurationMillisecond,
          DurationMicrosecond,
          DurationNanosecond,
          IntervalDayTime {
    /** Returns the value as a {@link Duration}. */
    Duration toDuration();
  }

  /**
   * Sealed subinterface for timestamp variants, providing typed {@link Instant} conversion.
   *
   * <p>When {@link #tz()} is non-null, the stored value is a UTC epoch value and {@link
   * #getObject()} returns an {@link Instant}. When {@link #tz()} is null, the timestamp is
   * timezone-naive and {@link #getObject()} returns a {@link LocalDateTime}.
   */
  sealed interface TimestampValue extends ScalarValue
      permits TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond {
    /** Returns the timestamp as an {@link Instant} (interpreting the epoch value as UTC). */
    Instant toInstant();

    /** Returns the timezone string, or {@code null} if timezone-naive. */
    String tz();
  }

  /** Sealed subinterface for date variants, providing typed {@link LocalDate} conversion. */
  sealed interface DateValue extends ScalarValue permits Date32, Date64 {
    /** Returns the value as a {@link LocalDate}. */
    LocalDate toLocalDate();
  }

  /** Sealed subinterface for time-of-day variants, providing typed {@link LocalTime} conversion. */
  sealed interface TimeValue extends ScalarValue
      permits Time32Second, Time32Millisecond, Time64Microsecond, Time64Nanosecond {
    /** Returns the value as a {@link LocalTime}. */
    LocalTime toLocalTime();
  }

  // -- Integers --
  record Int8(byte value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int16(short value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int32(int value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int64(long value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt8(short value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt16(int value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt32(long value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt64(BigInteger value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Floats --
  record Float16(float value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Float32(float value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Float64(double value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Strings --
  record Utf8(String value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record LargeUtf8(String value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Utf8View(String value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Binary --
  record Binary(byte[] value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record LargeBinary(byte[] value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record BinaryView(byte[] value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record FixedSizeBinary(byte[] value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Boolean --
  record BooleanValue(boolean value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Null --
  record Null() implements ScalarValue {
    @Override
    public Object getObject() {
      return null;
    }
  }

  // -- Dates --
  record Date32(int value) implements DateValue {
    @Override
    public LocalDate toLocalDate() {
      return LocalDate.ofEpochDay(value);
    }

    @Override
    public Object getObject() {
      return toLocalDate();
    }
  }

  record Date64(long value) implements DateValue {
    @Override
    public LocalDate toLocalDate() {
      return LocalDate.ofEpochDay(value / 86_400_000L);
    }

    @Override
    public Object getObject() {
      return toLocalDate();
    }
  }

  // -- Times --
  record Time32Second(int value) implements TimeValue {
    @Override
    public LocalTime toLocalTime() {
      return LocalTime.ofSecondOfDay(value);
    }

    @Override
    public Object getObject() {
      return toLocalTime();
    }
  }

  record Time32Millisecond(int value) implements TimeValue {
    @Override
    public LocalTime toLocalTime() {
      return LocalTime.ofNanoOfDay(value * 1_000_000L);
    }

    @Override
    public Object getObject() {
      return toLocalTime();
    }
  }

  record Time64Microsecond(long value) implements TimeValue {
    @Override
    public LocalTime toLocalTime() {
      return LocalTime.ofNanoOfDay(value * 1_000L);
    }

    @Override
    public Object getObject() {
      return toLocalTime();
    }
  }

  record Time64Nanosecond(long value) implements TimeValue {
    @Override
    public LocalTime toLocalTime() {
      return LocalTime.ofNanoOfDay(value);
    }

    @Override
    public Object getObject() {
      return toLocalTime();
    }
  }

  // -- Timestamps --
  record TimestampSecond(long value, String tz) implements TimestampValue {
    @Override
    public Instant toInstant() {
      return Instant.ofEpochSecond(value);
    }

    @Override
    public Object getObject() {
      return tz != null ? toInstant() : LocalDateTime.ofEpochSecond(value, 0, ZoneOffset.UTC);
    }
  }

  record TimestampMillisecond(long value, String tz) implements TimestampValue {
    @Override
    public Instant toInstant() {
      return Instant.ofEpochMilli(value);
    }

    @Override
    public Object getObject() {
      if (tz != null) {
        return toInstant();
      }
      long secs = Math.floorDiv(value, 1_000L);
      int nanos = (int) Math.floorMod(value, 1_000L) * 1_000_000;
      return LocalDateTime.ofEpochSecond(secs, nanos, ZoneOffset.UTC);
    }
  }

  record TimestampMicrosecond(long value, String tz) implements TimestampValue {
    @Override
    public Instant toInstant() {
      long secs = Math.floorDiv(value, 1_000_000L);
      long microsFrac = Math.floorMod(value, 1_000_000L);
      return Instant.ofEpochSecond(secs, microsFrac * 1_000L);
    }

    @Override
    public Object getObject() {
      if (tz != null) {
        return toInstant();
      }
      long secs = Math.floorDiv(value, 1_000_000L);
      int nanos = (int) Math.floorMod(value, 1_000_000L) * 1_000;
      return LocalDateTime.ofEpochSecond(secs, nanos, ZoneOffset.UTC);
    }
  }

  record TimestampNanosecond(long value, String tz) implements TimestampValue {
    @Override
    public Instant toInstant() {
      long secs = Math.floorDiv(value, 1_000_000_000L);
      int nanos = (int) Math.floorMod(value, 1_000_000_000L);
      return Instant.ofEpochSecond(secs, nanos);
    }

    @Override
    public Object getObject() {
      if (tz != null) {
        return toInstant();
      }
      long secs = Math.floorDiv(value, 1_000_000_000L);
      int nanos = (int) Math.floorMod(value, 1_000_000_000L);
      return LocalDateTime.ofEpochSecond(secs, nanos, ZoneOffset.UTC);
    }
  }

  // -- Durations --
  record DurationSecond(long value) implements DurationValue {
    @Override
    public Duration toDuration() {
      return Duration.ofSeconds(value);
    }

    @Override
    public Object getObject() {
      return toDuration();
    }
  }

  record DurationMillisecond(long value) implements DurationValue {
    @Override
    public Duration toDuration() {
      return Duration.ofMillis(value);
    }

    @Override
    public Object getObject() {
      return toDuration();
    }
  }

  record DurationMicrosecond(long value) implements DurationValue {
    @Override
    public Duration toDuration() {
      return Duration.ofNanos(value * 1_000L);
    }

    @Override
    public Object getObject() {
      return toDuration();
    }
  }

  record DurationNanosecond(long value) implements DurationValue {
    @Override
    public Duration toDuration() {
      return Duration.ofNanos(value);
    }

    @Override
    public Object getObject() {
      return toDuration();
    }
  }

  // -- Intervals --
  record IntervalYearMonth(int value) implements ScalarValue {
    @Override
    public Object getObject() {
      return Period.ofMonths(value);
    }
  }

  record IntervalDayTime(int days, int millis) implements DurationValue {
    @Override
    public Duration toDuration() {
      return Duration.ofDays(days).plusMillis(millis);
    }

    @Override
    public Object getObject() {
      return toDuration();
    }
  }

  record IntervalMonthDayNano(int months, int days, long nanos) implements ScalarValue {
    @Override
    public Object getObject() {
      return new PeriodDuration(Period.of(0, months, days), Duration.ofNanos(nanos));
    }
  }

  // -- Decimals --
  record Decimal32(int value, int precision, int scale) implements DecimalValue {
    @Override
    public BigDecimal toBigDecimal() {
      return BigDecimal.valueOf(value, scale);
    }

    @Override
    public Object getObject() {
      return toBigDecimal();
    }
  }

  record Decimal64(long value, int precision, int scale) implements DecimalValue {
    @Override
    public BigDecimal toBigDecimal() {
      return BigDecimal.valueOf(value, scale);
    }

    @Override
    public Object getObject() {
      return toBigDecimal();
    }
  }

  record Decimal128(BigInteger unscaledValue, int precision, int scale) implements DecimalValue {
    @Override
    public BigDecimal toBigDecimal() {
      return new BigDecimal(unscaledValue, scale);
    }

    @Override
    public Object getObject() {
      return toBigDecimal();
    }
  }

  record Decimal256(BigInteger unscaledValue, int precision, int scale) implements DecimalValue {
    @Override
    public BigDecimal toBigDecimal() {
      return new BigDecimal(unscaledValue, scale);
    }

    @Override
    public Object getObject() {
      return toBigDecimal();
    }
  }

  // -- Complex types (modeled but unsupported in FFI) --
  record ListValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("ListValue is not supported in FFI");
    }
  }

  record LargeListValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("LargeListValue is not supported in FFI");
    }
  }

  record FixedSizeListValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("FixedSizeListValue is not supported in FFI");
    }
  }

  record StructValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("StructValue is not supported in FFI");
    }
  }

  record MapValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("MapValue is not supported in FFI");
    }
  }

  record UnionValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("UnionValue is not supported in FFI");
    }
  }

  record DictionaryValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("DictionaryValue is not supported in FFI");
    }
  }

  record RunEndEncodedValue() implements ScalarValue {
    @Override
    public Object getObject() {
      throw new UnsupportedOperationException("RunEndEncodedValue is not supported in FFI");
    }
  }
}
