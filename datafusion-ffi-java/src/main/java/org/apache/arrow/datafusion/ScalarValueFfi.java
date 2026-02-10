package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * FFI deserialization logic for {@link ScalarValue}.
 *
 * <p>This class owns all FFI struct layout constants and the {@code fromFfi} method that
 * deserializes an {@code FFI_ScalarValue} struct into a {@link ScalarValue} instance.
 */
final class ScalarValueFfi {

  private ScalarValueFfi() {}

  // -- FFI type tag constants --
  // These must match the Rust type_tag values in guarantee.rs
  static final int TAG_NULL = 0;
  static final int TAG_BOOLEAN = 1;
  static final int TAG_INT8 = 2;
  static final int TAG_INT16 = 3;
  static final int TAG_INT32 = 4;
  static final int TAG_INT64 = 5;
  static final int TAG_UINT8 = 6;
  static final int TAG_UINT16 = 7;
  static final int TAG_UINT32 = 8;
  static final int TAG_UINT64 = 9;
  static final int TAG_FLOAT16 = 10;
  static final int TAG_FLOAT32 = 11;
  static final int TAG_FLOAT64 = 12;
  static final int TAG_UTF8 = 13;
  static final int TAG_LARGE_UTF8 = 14;
  static final int TAG_UTF8_VIEW = 15;
  static final int TAG_BINARY = 16;
  static final int TAG_LARGE_BINARY = 17;
  static final int TAG_BINARY_VIEW = 18;
  static final int TAG_FIXED_SIZE_BINARY = 19;
  static final int TAG_DATE32 = 20;
  static final int TAG_DATE64 = 21;
  static final int TAG_TIME32_SECOND = 22;
  static final int TAG_TIME32_MILLISECOND = 23;
  static final int TAG_TIME64_MICROSECOND = 24;
  static final int TAG_TIME64_NANOSECOND = 25;
  static final int TAG_TIMESTAMP_SECOND = 26;
  static final int TAG_TIMESTAMP_MILLISECOND = 27;
  static final int TAG_TIMESTAMP_MICROSECOND = 28;
  static final int TAG_TIMESTAMP_NANOSECOND = 29;
  static final int TAG_DURATION_SECOND = 30;
  static final int TAG_DURATION_MILLISECOND = 31;
  static final int TAG_DURATION_MICROSECOND = 32;
  static final int TAG_DURATION_NANOSECOND = 33;
  static final int TAG_INTERVAL_YEAR_MONTH = 34;
  static final int TAG_INTERVAL_DAY_TIME = 35;
  static final int TAG_INTERVAL_MONTH_DAY_NANO = 36;
  static final int TAG_DECIMAL32 = 37;
  static final int TAG_DECIMAL64 = 38;
  static final int TAG_DECIMAL128 = 39;
  static final int TAG_DECIMAL256 = 40;
  static final int TAG_LIST = 41;
  static final int TAG_LARGE_LIST = 42;
  static final int TAG_FIXED_SIZE_LIST = 43;
  static final int TAG_STRUCT = 44;
  static final int TAG_MAP = 45;
  static final int TAG_UNION = 46;
  static final int TAG_DICTIONARY = 47;
  static final int TAG_RUN_END_ENCODED = 48;

  // FFI_ScalarValue struct layout:
  //   offset  0: type_tag (i32, 4 bytes)
  //   offset  4: is_null (i32, 4 bytes)
  //   offset  8: data union (32 bytes)
  //   offset 40: data_ptr (pointer, 8 bytes)
  //   offset 48: data_len (usize/long, 8 bytes)
  //   offset 56: precision (u8, 1 byte)
  //   offset 57: scale (i8, 1 byte)
  //   offset 58: padding (2 bytes)
  //   offset 60: fixed_size (i32, 4 bytes)
  //   Total: 64 bytes

  static final long FFI_STRUCT_SIZE = 64;
  static final long OFFSET_TYPE_TAG = 0;
  static final long OFFSET_IS_NULL = 4;
  static final long OFFSET_DATA = 8;
  static final long OFFSET_DATA_PTR = 40;
  static final long OFFSET_DATA_LEN = 48;
  static final long OFFSET_PRECISION = 56;
  static final long OFFSET_SCALE = 57;
  static final long OFFSET_FIXED_SIZE = 60;

  /**
   * Deserializes a ScalarValue from an FFI_ScalarValue struct.
   *
   * @param struct the memory segment pointing to the FFI_ScalarValue
   * @return the deserialized ScalarValue
   */
  static ScalarValue fromFfi(MemorySegment struct) {
    int typeTag = struct.get(ValueLayout.JAVA_INT, OFFSET_TYPE_TAG);
    int isNull = struct.get(ValueLayout.JAVA_INT, OFFSET_IS_NULL);

    if (isNull != 0) {
      return new ScalarValue.Null();
    }

    return switch (typeTag) {
      case TAG_NULL -> new ScalarValue.Null();
      case TAG_BOOLEAN ->
          new ScalarValue.BooleanValue(struct.get(ValueLayout.JAVA_INT, OFFSET_DATA) != 0);
      case TAG_INT8 -> new ScalarValue.Int8((byte) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_INT16 ->
          new ScalarValue.Int16((short) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_INT32 -> new ScalarValue.Int32((int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_INT64 -> new ScalarValue.Int64(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_UINT8 ->
          new ScalarValue.UInt8((short) (struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA) & 0xFF));
      case TAG_UINT16 ->
          new ScalarValue.UInt16((int) (struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA) & 0xFFFF));
      case TAG_UINT32 ->
          new ScalarValue.UInt32(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA) & 0xFFFFFFFFL);
      case TAG_UINT64 -> {
        long raw = struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA);
        yield new ScalarValue.UInt64(toUnsignedBigInteger(raw));
      }
      case TAG_FLOAT16 -> {
        short bits = struct.get(ValueLayout.JAVA_SHORT, OFFSET_DATA);
        yield new ScalarValue.Float16(Float.float16ToFloat(bits));
      }
      case TAG_FLOAT32 ->
          new ScalarValue.Float32((float) struct.get(ValueLayout.JAVA_DOUBLE, OFFSET_DATA));
      case TAG_FLOAT64 -> new ScalarValue.Float64(struct.get(ValueLayout.JAVA_DOUBLE, OFFSET_DATA));
      case TAG_UTF8 -> new ScalarValue.Utf8(readString(struct));
      case TAG_LARGE_UTF8 -> new ScalarValue.LargeUtf8(readString(struct));
      case TAG_UTF8_VIEW -> new ScalarValue.Utf8View(readString(struct));
      case TAG_BINARY -> new ScalarValue.Binary(readBytes(struct));
      case TAG_LARGE_BINARY -> new ScalarValue.LargeBinary(readBytes(struct));
      case TAG_BINARY_VIEW -> new ScalarValue.BinaryView(readBytes(struct));
      case TAG_FIXED_SIZE_BINARY -> {
        int size = struct.get(ValueLayout.JAVA_INT, OFFSET_FIXED_SIZE);
        yield new ScalarValue.FixedSizeBinary(size, readBytes(struct));
      }
      case TAG_DATE32 ->
          new ScalarValue.Date32((int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_DATE64 -> new ScalarValue.Date64(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_TIME32_SECOND ->
          new ScalarValue.Time32Second((int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_TIME32_MILLISECOND ->
          new ScalarValue.Time32Millisecond((int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_TIME64_MICROSECOND ->
          new ScalarValue.Time64Microsecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_TIME64_NANOSECOND ->
          new ScalarValue.Time64Nanosecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_TIMESTAMP_SECOND ->
          new ScalarValue.TimestampSecond(
              struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA), readTimezone(struct));
      case TAG_TIMESTAMP_MILLISECOND ->
          new ScalarValue.TimestampMillisecond(
              struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA), readTimezone(struct));
      case TAG_TIMESTAMP_MICROSECOND ->
          new ScalarValue.TimestampMicrosecond(
              struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA), readTimezone(struct));
      case TAG_TIMESTAMP_NANOSECOND ->
          new ScalarValue.TimestampNanosecond(
              struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA), readTimezone(struct));
      case TAG_DURATION_SECOND ->
          new ScalarValue.DurationSecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_DURATION_MILLISECOND ->
          new ScalarValue.DurationMillisecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_DURATION_MICROSECOND ->
          new ScalarValue.DurationMicrosecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_DURATION_NANOSECOND ->
          new ScalarValue.DurationNanosecond(struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_INTERVAL_YEAR_MONTH ->
          new ScalarValue.IntervalYearMonth((int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA));
      case TAG_INTERVAL_DAY_TIME -> {
        int days = struct.get(ValueLayout.JAVA_INT, OFFSET_DATA);
        int millis = struct.get(ValueLayout.JAVA_INT, OFFSET_DATA + 4);
        yield new ScalarValue.IntervalDayTime(days, millis);
      }
      case TAG_INTERVAL_MONTH_DAY_NANO -> {
        int months = struct.get(ValueLayout.JAVA_INT, OFFSET_DATA);
        int days = struct.get(ValueLayout.JAVA_INT, OFFSET_DATA + 4);
        long nanos = struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA + 8);
        yield new ScalarValue.IntervalMonthDayNano(months, days, nanos);
      }
      case TAG_DECIMAL32 -> {
        int precision = Byte.toUnsignedInt(struct.get(ValueLayout.JAVA_BYTE, OFFSET_PRECISION));
        int scale = struct.get(ValueLayout.JAVA_BYTE, OFFSET_SCALE);
        int value = (int) struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA);
        yield new ScalarValue.Decimal32(value, precision, scale);
      }
      case TAG_DECIMAL64 -> {
        int precision = Byte.toUnsignedInt(struct.get(ValueLayout.JAVA_BYTE, OFFSET_PRECISION));
        int scale = struct.get(ValueLayout.JAVA_BYTE, OFFSET_SCALE);
        long value = struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA);
        yield new ScalarValue.Decimal64(value, precision, scale);
      }
      case TAG_DECIMAL128 -> {
        int precision = Byte.toUnsignedInt(struct.get(ValueLayout.JAVA_BYTE, OFFSET_PRECISION));
        int scale = struct.get(ValueLayout.JAVA_BYTE, OFFSET_SCALE);
        long low = struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA);
        long high = struct.get(ValueLayout.JAVA_LONG, OFFSET_DATA + 8);
        BigInteger unscaled = toI128(low, high);
        yield new ScalarValue.Decimal128(
            new BigDecimal(unscaled, scale, new MathContext(precision)), precision, scale);
      }
      case TAG_DECIMAL256 -> {
        int precision = Byte.toUnsignedInt(struct.get(ValueLayout.JAVA_BYTE, OFFSET_PRECISION));
        int scale = struct.get(ValueLayout.JAVA_BYTE, OFFSET_SCALE);
        // Read 32 bytes in little-endian order
        byte[] leBytes = new byte[32];
        for (int i = 0; i < 32; i++) {
          leBytes[i] = struct.get(ValueLayout.JAVA_BYTE, OFFSET_DATA + i);
        }
        // Convert LE to BE for BigInteger
        byte[] beBytes = new byte[32];
        for (int i = 0; i < 32; i++) {
          beBytes[i] = leBytes[31 - i];
        }
        BigInteger unscaled = new BigInteger(beBytes);
        yield new ScalarValue.Decimal256(
            new BigDecimal(unscaled, scale, new MathContext(precision)), precision, scale);
      }
      case TAG_LIST,
              TAG_LARGE_LIST,
              TAG_FIXED_SIZE_LIST,
              TAG_STRUCT,
              TAG_MAP,
              TAG_UNION,
              TAG_DICTIONARY,
              TAG_RUN_END_ENCODED ->
          throw new UnsupportedOperationException(
              "Complex ScalarValue type tag " + typeTag + " is not supported in FFI");
      default -> throw new IllegalArgumentException("Unknown ScalarValue type tag: " + typeTag);
    };
  }

  private static String readString(MemorySegment struct_) {
    MemorySegment ptr = struct_.get(ValueLayout.ADDRESS, OFFSET_DATA_PTR);
    long len = struct_.get(ValueLayout.JAVA_LONG, OFFSET_DATA_LEN);
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return "";
    }
    // Read raw bytes and convert to String - Rust strings are NOT null-terminated
    byte[] bytes = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, bytes, 0, (int) len);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] readBytes(MemorySegment struct_) {
    MemorySegment ptr = struct_.get(ValueLayout.ADDRESS, OFFSET_DATA_PTR);
    long len = struct_.get(ValueLayout.JAVA_LONG, OFFSET_DATA_LEN);
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return new byte[0];
    }
    byte[] result = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, result, 0, (int) len);
    return result;
  }

  private static String readTimezone(MemorySegment struct_) {
    MemorySegment ptr = struct_.get(ValueLayout.ADDRESS, OFFSET_DATA_PTR);
    long len = struct_.get(ValueLayout.JAVA_LONG, OFFSET_DATA_LEN);
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return null;
    }
    byte[] bytes = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, bytes, 0, (int) len);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }

  private static BigInteger toUnsignedBigInteger(long value) {
    if (value >= 0) {
      return BigInteger.valueOf(value);
    }
    // Treat as unsigned
    return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL).setBit(63);
  }

  private static BigInteger toI128(long low, long high) {
    BigInteger hi = BigInteger.valueOf(high).shiftLeft(64);
    BigInteger lo = toUnsignedBigInteger(low);
    return hi.add(lo);
  }
}
