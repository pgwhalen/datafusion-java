package org.apache.arrow.datafusion.common;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.PeriodDuration;

/**
 * A scalar value from DataFusion, representing a single typed value.
 *
 * <p>This sealed interface mirrors DataFusion's {@code ScalarValue} enum. Each variant is a record
 * that holds the typed value, including complex types (List, Struct, Map, Dictionary, Union, and
 * RunEndEncoded).
 *
 * <p>Use {@link #getObject()} for untyped access returning a friendly Java type, or use {@code
 * instanceof} with sealed subinterfaces ({@link IntegerValue}, {@link FloatValue}, {@link
 * StringValue}, {@link BinaryValue}, {@link DecimalValue}, {@link DurationValue}, {@link
 * TimestampValue}, {@link DateValue}, {@link TimeValue}) for typed access without casting.
 *
 * <p>Example:
 *
 * {@snippet :
 * ScalarValue intVal = new ScalarValue.Int32(42);
 * ScalarValue strVal = new ScalarValue.Utf8("hello");
 * ScalarValue boolVal = new ScalarValue.BooleanValue(true);
 * Object obj = intVal.getObject(); // Integer(42)
 * if (strVal instanceof ScalarValue.Utf8 utf8) {
 *     String s = utf8.value(); // "hello"
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/53.1.0/datafusion_common/scalar/enum.ScalarValue.html">Rust
 *     DataFusion: ScalarValue</a>
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

  /** Sealed subinterface for integer variants, providing widening numeric conversions. */
  sealed interface IntegerValue extends ScalarValue
      permits Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64 {
    /** Returns the value as a {@code long}. Throws {@link ArithmeticException} for UInt64 overflow. */
    long toLong();

    /** Returns the value as a {@link BigInteger}. Always safe for all integer types. */
    BigInteger toBigInteger();
  }

  /** Sealed subinterface for floating-point variants, providing widening {@code double} conversion. */
  sealed interface FloatValue extends ScalarValue permits Float16, Float32, Float64 {
    /** Returns the value as a {@code double}. */
    double toDouble();
  }

  /** Sealed subinterface for string variants, providing typed {@link String} access. */
  sealed interface StringValue extends ScalarValue permits Utf8, LargeUtf8, Utf8View {
    /** Returns the string value. */
    String value();
  }

  /** Sealed subinterface for binary variants, providing typed {@code byte[]} access. */
  sealed interface BinaryValue extends ScalarValue
      permits Binary, LargeBinary, BinaryView, FixedSizeBinary {
    /** Returns the binary value. */
    byte[] value();
  }

  /**
   * Sealed subinterface for list-like variants (List, LargeList, FixedSizeList), providing typed
   * {@link List} access to the contained {@link ScalarValue} elements.
   */
  sealed interface ListScalarValue extends ScalarValue
      permits ListValue, LargeListValue, FixedSizeListValue {
    /** Returns the list elements as {@link ScalarValue} instances. */
    List<ScalarValue> values();

    /** Returns the list elements with each value converted via {@link ScalarValue#getObject()}. */
    List<Object> toList();
  }

  /**
   * Sealed subinterface for map-like variants (Struct, Map), providing typed {@link Map} access to
   * key-value data. Use the concrete record types for the specific key/value types.
   */
  sealed interface MapScalarValue extends ScalarValue permits StructValue, MapValue {}

  // -- Integers --
  record Int8(byte value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int16(short value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int32(int value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record Int64(long value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt8(short value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt16(int value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt32(long value) implements IntegerValue {
    @Override
    public long toLong() {
      return value;
    }

    @Override
    public BigInteger toBigInteger() {
      return BigInteger.valueOf(value);
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record UInt64(BigInteger value) implements IntegerValue {
    @Override
    public long toLong() {
      return value.longValueExact();
    }

    @Override
    public BigInteger toBigInteger() {
      return value;
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Floats --
  record Float16(float value) implements FloatValue {
    @Override
    public double toDouble() {
      return value;
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record Float32(float value) implements FloatValue {
    @Override
    public double toDouble() {
      return value;
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  record Float64(double value) implements FloatValue {
    @Override
    public double toDouble() {
      return value;
    }

    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Strings --
  record Utf8(String value) implements StringValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record LargeUtf8(String value) implements StringValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record Utf8View(String value) implements StringValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  // -- Binary --
  record Binary(byte[] value) implements BinaryValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record LargeBinary(byte[] value) implements BinaryValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record BinaryView(byte[] value) implements BinaryValue {
    @Override
    public Object getObject() {
      return value;
    }
  }

  record FixedSizeBinary(byte[] value) implements BinaryValue {
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

  // -- Complex types --

  /** A list scalar value containing ordered elements. */
  record ListValue(List<ScalarValue> values) implements ListScalarValue {
    public ListValue {
      values = List.copyOf(values);
    }

    @Override
    public List<Object> toList() {
      return values.stream().map(ScalarValue::getObject).toList();
    }

    @Override
    public Object getObject() {
      return toList();
    }
  }

  /** A large list scalar value (64-bit offsets) containing ordered elements. */
  record LargeListValue(List<ScalarValue> values) implements ListScalarValue {
    public LargeListValue {
      values = List.copyOf(values);
    }

    @Override
    public List<Object> toList() {
      return values.stream().map(ScalarValue::getObject).toList();
    }

    @Override
    public Object getObject() {
      return toList();
    }
  }

  /** A fixed-size list scalar value where every list has the same number of elements. */
  record FixedSizeListValue(List<ScalarValue> values, int listSize) implements ListScalarValue {
    public FixedSizeListValue {
      values = List.copyOf(values);
    }

    @Override
    public List<Object> toList() {
      return values.stream().map(ScalarValue::getObject).toList();
    }

    @Override
    public Object getObject() {
      return toList();
    }
  }

  /** A struct scalar value with named fields. The field order is preserved. */
  record StructValue(Map<String, ScalarValue> fields) implements MapScalarValue {
    public StructValue {
      fields = Map.copyOf(fields);
    }

    /** Returns the struct fields with each value converted via {@link #getObject()}. */
    public Map<String, Object> toMap() {
      var result = new LinkedHashMap<String, Object>();
      fields.forEach((k, v) -> result.put(k, v.getObject()));
      return result;
    }

    @Override
    public Object getObject() {
      return toMap();
    }
  }

  /**
   * A map scalar value containing key-value entries. Entries are ordered and may contain duplicate
   * keys (per the Arrow MapArray specification).
   */
  record MapValue(List<Map.Entry<ScalarValue, ScalarValue>> entries) implements MapScalarValue {
    public MapValue {
      entries = List.copyOf(entries);
    }

    /** Returns the entries as a map with keys and values converted via {@link #getObject()}. */
    public Map<Object, Object> toMap() {
      var result = new LinkedHashMap<Object, Object>();
      for (var entry : entries) {
        result.put(entry.getKey().getObject(), entry.getValue().getObject());
      }
      return result;
    }

    @Override
    public Object getObject() {
      return toMap();
    }
  }

  /**
   * A union scalar value representing one variant of a union type.
   *
   * @param typeId the union type ID of the active variant (128 indicates null)
   * @param value the scalar value of the active variant
   */
  record UnionValue(int typeId, ScalarValue value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value.getObject();
    }
  }

  /**
   * A dictionary-encoded scalar value. The value is the looked-up dictionary entry, not the index.
   */
  record DictionaryValue(ScalarValue value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value.getObject();
    }
  }

  /** A run-end encoded scalar value. The value is the decoded scalar, not the encoding metadata. */
  record RunEndEncodedValue(ScalarValue value) implements ScalarValue {
    @Override
    public Object getObject() {
      return value.getObject();
    }
  }
}
