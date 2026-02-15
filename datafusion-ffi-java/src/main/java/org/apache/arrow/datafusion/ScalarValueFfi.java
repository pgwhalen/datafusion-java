package org.apache.arrow.datafusion;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;

/**
 * FFI deserialization logic for {@link ScalarValue}.
 *
 * <p>This class owns all FFI struct layout constants and the {@code fromFfi} method that
 * deserializes an {@code FFI_ScalarValue} struct into a {@link ScalarValue} instance.
 */
final class ScalarValueFfi {

  static final MethodHandle FFI_SCALAR_VALUE_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_scalar_value_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static final MethodHandle TEST_SCALAR_VALUE =
      NativeUtil.downcall(
          "datafusion_test_scalar_value",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT.withName("type_tag"),
              ValueLayout.ADDRESS.withName("scalar_out"),
              ValueLayout.ADDRESS.withName("error_out")));

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

  // FFI_ScalarValue struct layout (64 bytes, align 8):
  //
  //   offset  0: type_tag (i32, 4 bytes)
  //   offset  4: is_null (i32, 4 bytes)
  //   offset  8: data union (32 bytes)
  //   offset 40: data_ptr (pointer, 8 bytes)
  //   offset 48: data_len (usize/long, 8 bytes)
  //   offset 56: precision (u8, 1 byte)
  //   offset 57: scale (i8, 1 byte)
  //   offset 58: padding (2 bytes)
  //   offset 60: fixed_size (i32, 4 bytes)

  static final StructLayout FFI_SCALAR_VALUE_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_INT.withName("type_tag"),
          ValueLayout.JAVA_INT.withName("is_null"),
          MemoryLayout.unionLayout(
                  ValueLayout.JAVA_INT.withName("bool_val"),
                  ValueLayout.JAVA_LONG.withName("i64_val"),
                  ValueLayout.JAVA_DOUBLE.withName("f64_val"),
                  ValueLayout.JAVA_SHORT.withName("f16_val"),
                  MemoryLayout.structLayout(
                          ValueLayout.JAVA_INT.withName("days"),
                          ValueLayout.JAVA_INT.withName("millis"))
                      .withName("interval_dt"),
                  MemoryLayout.structLayout(
                          ValueLayout.JAVA_INT.withName("months"),
                          ValueLayout.JAVA_INT.withName("days"),
                          ValueLayout.JAVA_LONG.withName("nanos"))
                      .withName("interval_mdn"),
                  MemoryLayout.structLayout(
                          ValueLayout.JAVA_LONG.withName("low"),
                          ValueLayout.JAVA_LONG.withName("high"))
                      .withName("i128_val"),
                  MemoryLayout.sequenceLayout(32, ValueLayout.JAVA_BYTE).withName("bytes32"))
              .withName("data"),
          ValueLayout.ADDRESS.withName("data_ptr"),
          ValueLayout.JAVA_LONG.withName("data_len"),
          ValueLayout.JAVA_BYTE.withName("precision"),
          ValueLayout.JAVA_BYTE.withName("scale"),
          MemoryLayout.paddingLayout(2),
          ValueLayout.JAVA_INT.withName("fixed_size"));

  // -- Direct field VarHandles --
  private static final VarHandle VH_TYPE_TAG =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("type_tag"));
  private static final VarHandle VH_IS_NULL =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("is_null"));
  private static final VarHandle VH_DATA_PTR =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("data_ptr"));
  private static final VarHandle VH_DATA_LEN =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("data_len"));
  private static final VarHandle VH_PRECISION =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("precision"));
  private static final VarHandle VH_SCALE =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(PathElement.groupElement("scale"));

  // -- Union member VarHandles --
  private static final VarHandle VH_DATA_BOOL =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"), PathElement.groupElement("bool_val"));
  private static final VarHandle VH_DATA_I64 =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"), PathElement.groupElement("i64_val"));
  private static final VarHandle VH_DATA_F64 =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"), PathElement.groupElement("f64_val"));
  private static final VarHandle VH_DATA_F16 =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"), PathElement.groupElement("f16_val"));

  // -- Nested union struct VarHandles --
  private static final VarHandle VH_INTERVAL_DT_DAYS =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("interval_dt"),
          PathElement.groupElement("days"));
  private static final VarHandle VH_INTERVAL_DT_MILLIS =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("interval_dt"),
          PathElement.groupElement("millis"));
  private static final VarHandle VH_INTERVAL_MDN_MONTHS =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("interval_mdn"),
          PathElement.groupElement("months"));
  private static final VarHandle VH_INTERVAL_MDN_DAYS =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("interval_mdn"),
          PathElement.groupElement("days"));
  private static final VarHandle VH_INTERVAL_MDN_NANOS =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("interval_mdn"),
          PathElement.groupElement("nanos"));
  private static final VarHandle VH_I128_LOW =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("i128_val"),
          PathElement.groupElement("low"));
  private static final VarHandle VH_I128_HIGH =
      FFI_SCALAR_VALUE_LAYOUT.varHandle(
          PathElement.groupElement("data"),
          PathElement.groupElement("i128_val"),
          PathElement.groupElement("high"));

  // Byte offset of the data union, used for Decimal256 raw byte iteration
  private static final long DATA_OFFSET =
      FFI_SCALAR_VALUE_LAYOUT.byteOffset(PathElement.groupElement("data"));

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_SCALAR_VALUE_LAYOUT.byteSize(), FFI_SCALAR_VALUE_SIZE, "FFI_ScalarValue");
  }

  /**
   * Deserializes a ScalarValue from an FFI_ScalarValue struct.
   *
   * @param struct the memory segment pointing to the FFI_ScalarValue
   * @return the deserialized ScalarValue
   */
  static ScalarValue fromFfi(MemorySegment struct) {
    int typeTag = (int) VH_TYPE_TAG.get(struct, 0L);
    int isNull = (int) VH_IS_NULL.get(struct, 0L);

    if (isNull != 0) {
      return new ScalarValue.Null();
    }

    return switch (typeTag) {
      case TAG_NULL -> new ScalarValue.Null();
      case TAG_BOOLEAN -> new ScalarValue.BooleanValue((int) VH_DATA_BOOL.get(struct, 0L) != 0);
      case TAG_INT8 -> new ScalarValue.Int8((byte) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_INT16 -> new ScalarValue.Int16((short) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_INT32 -> new ScalarValue.Int32((int) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_INT64 -> new ScalarValue.Int64((long) VH_DATA_I64.get(struct, 0L));
      case TAG_UINT8 -> new ScalarValue.UInt8((short) ((long) VH_DATA_I64.get(struct, 0L) & 0xFF));
      case TAG_UINT16 ->
          new ScalarValue.UInt16((int) ((long) VH_DATA_I64.get(struct, 0L) & 0xFFFF));
      case TAG_UINT32 -> new ScalarValue.UInt32((long) VH_DATA_I64.get(struct, 0L) & 0xFFFFFFFFL);
      case TAG_UINT64 -> {
        long raw = (long) VH_DATA_I64.get(struct, 0L);
        yield new ScalarValue.UInt64(toUnsignedBigInteger(raw));
      }
      case TAG_FLOAT16 -> {
        short bits = (short) VH_DATA_F16.get(struct, 0L);
        yield new ScalarValue.Float16(Float.float16ToFloat(bits));
      }
      case TAG_FLOAT32 -> new ScalarValue.Float32((float) (double) VH_DATA_F64.get(struct, 0L));
      case TAG_FLOAT64 -> new ScalarValue.Float64((double) VH_DATA_F64.get(struct, 0L));
      case TAG_UTF8 -> new ScalarValue.Utf8(readString(struct));
      case TAG_LARGE_UTF8 -> new ScalarValue.LargeUtf8(readString(struct));
      case TAG_UTF8_VIEW -> new ScalarValue.Utf8View(readString(struct));
      case TAG_BINARY -> new ScalarValue.Binary(readBytes(struct));
      case TAG_LARGE_BINARY -> new ScalarValue.LargeBinary(readBytes(struct));
      case TAG_BINARY_VIEW -> new ScalarValue.BinaryView(readBytes(struct));
      case TAG_FIXED_SIZE_BINARY -> new ScalarValue.FixedSizeBinary(readBytes(struct));
      case TAG_DATE32 -> new ScalarValue.Date32((int) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_DATE64 -> new ScalarValue.Date64((long) VH_DATA_I64.get(struct, 0L));
      case TAG_TIME32_SECOND ->
          new ScalarValue.Time32Second((int) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_TIME32_MILLISECOND ->
          new ScalarValue.Time32Millisecond((int) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_TIME64_MICROSECOND ->
          new ScalarValue.Time64Microsecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_TIME64_NANOSECOND ->
          new ScalarValue.Time64Nanosecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_TIMESTAMP_SECOND ->
          new ScalarValue.TimestampSecond((long) VH_DATA_I64.get(struct, 0L), readTimezone(struct));
      case TAG_TIMESTAMP_MILLISECOND ->
          new ScalarValue.TimestampMillisecond(
              (long) VH_DATA_I64.get(struct, 0L), readTimezone(struct));
      case TAG_TIMESTAMP_MICROSECOND ->
          new ScalarValue.TimestampMicrosecond(
              (long) VH_DATA_I64.get(struct, 0L), readTimezone(struct));
      case TAG_TIMESTAMP_NANOSECOND ->
          new ScalarValue.TimestampNanosecond(
              (long) VH_DATA_I64.get(struct, 0L), readTimezone(struct));
      case TAG_DURATION_SECOND ->
          new ScalarValue.DurationSecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_DURATION_MILLISECOND ->
          new ScalarValue.DurationMillisecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_DURATION_MICROSECOND ->
          new ScalarValue.DurationMicrosecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_DURATION_NANOSECOND ->
          new ScalarValue.DurationNanosecond((long) VH_DATA_I64.get(struct, 0L));
      case TAG_INTERVAL_YEAR_MONTH ->
          new ScalarValue.IntervalYearMonth((int) (long) VH_DATA_I64.get(struct, 0L));
      case TAG_INTERVAL_DAY_TIME -> {
        int days = (int) VH_INTERVAL_DT_DAYS.get(struct, 0L);
        int millis = (int) VH_INTERVAL_DT_MILLIS.get(struct, 0L);
        yield new ScalarValue.IntervalDayTime(days, millis);
      }
      case TAG_INTERVAL_MONTH_DAY_NANO -> {
        int months = (int) VH_INTERVAL_MDN_MONTHS.get(struct, 0L);
        int days = (int) VH_INTERVAL_MDN_DAYS.get(struct, 0L);
        long nanos = (long) VH_INTERVAL_MDN_NANOS.get(struct, 0L);
        yield new ScalarValue.IntervalMonthDayNano(months, days, nanos);
      }
      case TAG_DECIMAL32 -> {
        int precision = Byte.toUnsignedInt((byte) VH_PRECISION.get(struct, 0L));
        int scale = (byte) VH_SCALE.get(struct, 0L);
        int value = (int) (long) VH_DATA_I64.get(struct, 0L);
        yield new ScalarValue.Decimal32(value, precision, scale);
      }
      case TAG_DECIMAL64 -> {
        int precision = Byte.toUnsignedInt((byte) VH_PRECISION.get(struct, 0L));
        int scale = (byte) VH_SCALE.get(struct, 0L);
        long value = (long) VH_DATA_I64.get(struct, 0L);
        yield new ScalarValue.Decimal64(value, precision, scale);
      }
      case TAG_DECIMAL128 -> {
        int precision = Byte.toUnsignedInt((byte) VH_PRECISION.get(struct, 0L));
        int scale = (byte) VH_SCALE.get(struct, 0L);
        long low = (long) VH_I128_LOW.get(struct, 0L);
        long high = (long) VH_I128_HIGH.get(struct, 0L);
        BigInteger unscaled = toI128(low, high);
        yield new ScalarValue.Decimal128(unscaled, precision, scale);
      }
      case TAG_DECIMAL256 -> {
        int precision = Byte.toUnsignedInt((byte) VH_PRECISION.get(struct, 0L));
        int scale = (byte) VH_SCALE.get(struct, 0L);
        // Read 32 bytes in little-endian order
        byte[] leBytes = new byte[32];
        for (int i = 0; i < 32; i++) {
          leBytes[i] = struct.get(ValueLayout.JAVA_BYTE, DATA_OFFSET + i);
        }
        // Convert LE to BE for BigInteger
        byte[] beBytes = new byte[32];
        for (int i = 0; i < 32; i++) {
          beBytes[i] = leBytes[31 - i];
        }
        BigInteger unscaled = new BigInteger(beBytes);
        yield new ScalarValue.Decimal256(unscaled, precision, scale);
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
    MemorySegment ptr = (MemorySegment) VH_DATA_PTR.get(struct_, 0L);
    long len = (long) VH_DATA_LEN.get(struct_, 0L);
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return "";
    }
    // Read raw bytes and convert to String - Rust strings are NOT null-terminated
    byte[] bytes = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, bytes, 0, (int) len);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] readBytes(MemorySegment struct_) {
    MemorySegment ptr = (MemorySegment) VH_DATA_PTR.get(struct_, 0L);
    long len = (long) VH_DATA_LEN.get(struct_, 0L);
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return new byte[0];
    }
    byte[] result = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, result, 0, (int) len);
    return result;
  }

  private static String readTimezone(MemorySegment struct_) {
    MemorySegment ptr = (MemorySegment) VH_DATA_PTR.get(struct_, 0L);
    long len = (long) VH_DATA_LEN.get(struct_, 0L);
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
