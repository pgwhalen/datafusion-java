package org.apache.arrow.datafusion;

import com.google.protobuf.ByteString;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Converts between proto {@code ScalarValue} messages and Java {@link ScalarValue} records.
 *
 * <p>Handles all primitive types, strings, binary, dates, times, timestamps, durations, intervals,
 * and decimals. Complex types (List, Struct, Map, etc.) are not supported and will throw.
 */
final class ScalarValueProtoConverter {
  private ScalarValueProtoConverter() {}

  static ScalarValue fromProto(org.apache.arrow.datafusion.proto.ScalarValue proto) {
    if (proto == null
        || proto.getValueCase()
            == org.apache.arrow.datafusion.proto.ScalarValue.ValueCase.VALUE_NOT_SET) {
      return new ScalarValue.Null();
    }
    return switch (proto.getValueCase()) {
      case BOOL_VALUE -> new ScalarValue.BooleanValue(proto.getBoolValue());
      case INT8_VALUE -> new ScalarValue.Int8((byte) proto.getInt8Value());
      case INT16_VALUE -> new ScalarValue.Int16((short) proto.getInt16Value());
      case INT32_VALUE -> new ScalarValue.Int32(proto.getInt32Value());
      case INT64_VALUE -> new ScalarValue.Int64(proto.getInt64Value());
      case UINT8_VALUE -> new ScalarValue.UInt8((short) (proto.getUint8Value() & 0xFF));
      case UINT16_VALUE -> new ScalarValue.UInt16(proto.getUint16Value() & 0xFFFF);
      case UINT32_VALUE -> new ScalarValue.UInt32(proto.getUint32Value() & 0xFFFFFFFFL);
      case UINT64_VALUE ->
          BigInteger.valueOf(proto.getUint64Value()).signum() < 0
              ? new ScalarValue.UInt64(
                  BigInteger.valueOf(proto.getUint64Value()).add(BigInteger.TWO.pow(64)))
              : new ScalarValue.UInt64(BigInteger.valueOf(proto.getUint64Value()));
      case FLOAT32_VALUE -> new ScalarValue.Float32(proto.getFloat32Value());
      case FLOAT64_VALUE -> new ScalarValue.Float64(proto.getFloat64Value());
      case UTF8_VALUE -> new ScalarValue.Utf8(proto.getUtf8Value());
      case LARGE_UTF8_VALUE -> new ScalarValue.LargeUtf8(proto.getLargeUtf8Value());
      case UTF8_VIEW_VALUE -> new ScalarValue.Utf8View(proto.getUtf8ViewValue());
      case BINARY_VALUE -> new ScalarValue.Binary(proto.getBinaryValue().toByteArray());
      case LARGE_BINARY_VALUE ->
          new ScalarValue.LargeBinary(proto.getLargeBinaryValue().toByteArray());
      case BINARY_VIEW_VALUE ->
          new ScalarValue.BinaryView(proto.getBinaryViewValue().toByteArray());
      case FIXED_SIZE_BINARY_VALUE ->
          new ScalarValue.FixedSizeBinary(
              proto.getFixedSizeBinaryValue().getValues().toByteArray());
      case DATE_32_VALUE -> new ScalarValue.Date32(proto.getDate32Value());
      case DATE_64_VALUE -> new ScalarValue.Date64(proto.getDate64Value());
      case TIME32_VALUE -> {
        var t32 = proto.getTime32Value();
        yield switch (t32.getValueCase()) {
          case TIME32_SECOND_VALUE -> new ScalarValue.Time32Second(t32.getTime32SecondValue());
          case TIME32_MILLISECOND_VALUE ->
              new ScalarValue.Time32Millisecond(t32.getTime32MillisecondValue());
          default -> new ScalarValue.Null();
        };
      }
      case TIME64_VALUE -> {
        var t64 = proto.getTime64Value();
        yield switch (t64.getValueCase()) {
          case TIME64_MICROSECOND_VALUE ->
              new ScalarValue.Time64Microsecond(t64.getTime64MicrosecondValue());
          case TIME64_NANOSECOND_VALUE ->
              new ScalarValue.Time64Nanosecond(t64.getTime64NanosecondValue());
          default -> new ScalarValue.Null();
        };
      }
      case TIMESTAMP_VALUE -> {
        var ts = proto.getTimestampValue();
        String tz = ts.getTimezone().isEmpty() ? null : ts.getTimezone();
        yield switch (ts.getValueCase()) {
          case TIME_SECOND_VALUE -> new ScalarValue.TimestampSecond(ts.getTimeSecondValue(), tz);
          case TIME_MILLISECOND_VALUE ->
              new ScalarValue.TimestampMillisecond(ts.getTimeMillisecondValue(), tz);
          case TIME_MICROSECOND_VALUE ->
              new ScalarValue.TimestampMicrosecond(ts.getTimeMicrosecondValue(), tz);
          case TIME_NANOSECOND_VALUE ->
              new ScalarValue.TimestampNanosecond(ts.getTimeNanosecondValue(), tz);
          default -> new ScalarValue.Null();
        };
      }
      case INTERVAL_YEARMONTH_VALUE ->
          new ScalarValue.IntervalYearMonth(proto.getIntervalYearmonthValue());
      case INTERVAL_DAYTIME_VALUE -> {
        var dt = proto.getIntervalDaytimeValue();
        yield new ScalarValue.IntervalDayTime(dt.getDays(), dt.getMilliseconds());
      }
      case INTERVAL_MONTH_DAY_NANO -> {
        var mdn = proto.getIntervalMonthDayNano();
        yield new ScalarValue.IntervalMonthDayNano(mdn.getMonths(), mdn.getDays(), mdn.getNanos());
      }
      case DURATION_SECOND_VALUE -> new ScalarValue.DurationSecond(proto.getDurationSecondValue());
      case DURATION_MILLISECOND_VALUE ->
          new ScalarValue.DurationMillisecond(proto.getDurationMillisecondValue());
      case DURATION_MICROSECOND_VALUE ->
          new ScalarValue.DurationMicrosecond(proto.getDurationMicrosecondValue());
      case DURATION_NANOSECOND_VALUE ->
          new ScalarValue.DurationNanosecond(proto.getDurationNanosecondValue());
      case DECIMAL32_VALUE -> {
        var d = proto.getDecimal32Value();
        int val = decimalBytesToInt(d.getValue());
        yield new ScalarValue.Decimal32(val, (int) d.getP(), (int) d.getS());
      }
      case DECIMAL64_VALUE -> {
        var d = proto.getDecimal64Value();
        long val = decimalBytesToLong(d.getValue());
        yield new ScalarValue.Decimal64(val, (int) d.getP(), (int) d.getS());
      }
      case DECIMAL128_VALUE -> {
        var d = proto.getDecimal128Value();
        BigInteger val = decimalBytesToBigInteger(d.getValue());
        yield new ScalarValue.Decimal128(val, (int) d.getP(), (int) d.getS());
      }
      case DECIMAL256_VALUE -> {
        var d = proto.getDecimal256Value();
        BigInteger val = decimalBytesToBigInteger(d.getValue());
        yield new ScalarValue.Decimal256(val, (int) d.getP(), (int) d.getS());
      }
      case NULL_VALUE -> new ScalarValue.Null();
      default -> new ScalarValue.Null();
    };
  }

  static org.apache.arrow.datafusion.proto.ScalarValue toProto(ScalarValue value) {
    var builder = org.apache.arrow.datafusion.proto.ScalarValue.newBuilder();
    switch (value) {
      case ScalarValue.BooleanValue v -> builder.setBoolValue(v.value());
      case ScalarValue.Int8 v -> builder.setInt8Value(v.value());
      case ScalarValue.Int16 v -> builder.setInt16Value(v.value());
      case ScalarValue.Int32 v -> builder.setInt32Value(v.value());
      case ScalarValue.Int64 v -> builder.setInt64Value(v.value());
      case ScalarValue.UInt8 v -> builder.setUint8Value(v.value());
      case ScalarValue.UInt16 v -> builder.setUint16Value(v.value());
      case ScalarValue.UInt32 v -> builder.setUint32Value((int) v.value());
      case ScalarValue.UInt64 v -> builder.setUint64Value(v.value().longValue());
      case ScalarValue.Float16 v -> builder.setFloat32Value(v.value());
      case ScalarValue.Float32 v -> builder.setFloat32Value(v.value());
      case ScalarValue.Float64 v -> builder.setFloat64Value(v.value());
      case ScalarValue.Utf8 v -> builder.setUtf8Value(v.value());
      case ScalarValue.LargeUtf8 v -> builder.setLargeUtf8Value(v.value());
      case ScalarValue.Utf8View v -> builder.setUtf8ViewValue(v.value());
      case ScalarValue.Binary v -> builder.setBinaryValue(ByteString.copyFrom(v.value()));
      case ScalarValue.LargeBinary v -> builder.setLargeBinaryValue(ByteString.copyFrom(v.value()));
      case ScalarValue.BinaryView v -> builder.setBinaryViewValue(ByteString.copyFrom(v.value()));
      case ScalarValue.FixedSizeBinary v ->
          builder.setFixedSizeBinaryValue(
              org.apache.arrow.datafusion.proto.ScalarFixedSizeBinary.newBuilder()
                  .setValues(ByteString.copyFrom(v.value()))
                  .setLength(v.value().length));
      case ScalarValue.Date32 v -> builder.setDate32Value(v.value());
      case ScalarValue.Date64 v -> builder.setDate64Value(v.value());
      case ScalarValue.Time32Second v ->
          builder.setTime32Value(
              org.apache.arrow.datafusion.proto.ScalarTime32Value.newBuilder()
                  .setTime32SecondValue(v.value()));
      case ScalarValue.Time32Millisecond v ->
          builder.setTime32Value(
              org.apache.arrow.datafusion.proto.ScalarTime32Value.newBuilder()
                  .setTime32MillisecondValue(v.value()));
      case ScalarValue.Time64Microsecond v ->
          builder.setTime64Value(
              org.apache.arrow.datafusion.proto.ScalarTime64Value.newBuilder()
                  .setTime64MicrosecondValue(v.value()));
      case ScalarValue.Time64Nanosecond v ->
          builder.setTime64Value(
              org.apache.arrow.datafusion.proto.ScalarTime64Value.newBuilder()
                  .setTime64NanosecondValue(v.value()));
      case ScalarValue.TimestampSecond v -> {
        var tsBuilder =
            org.apache.arrow.datafusion.proto.ScalarTimestampValue.newBuilder()
                .setTimeSecondValue(v.value());
        if (v.tz() != null) tsBuilder.setTimezone(v.tz());
        builder.setTimestampValue(tsBuilder);
      }
      case ScalarValue.TimestampMillisecond v -> {
        var tsBuilder =
            org.apache.arrow.datafusion.proto.ScalarTimestampValue.newBuilder()
                .setTimeMillisecondValue(v.value());
        if (v.tz() != null) tsBuilder.setTimezone(v.tz());
        builder.setTimestampValue(tsBuilder);
      }
      case ScalarValue.TimestampMicrosecond v -> {
        var tsBuilder =
            org.apache.arrow.datafusion.proto.ScalarTimestampValue.newBuilder()
                .setTimeMicrosecondValue(v.value());
        if (v.tz() != null) tsBuilder.setTimezone(v.tz());
        builder.setTimestampValue(tsBuilder);
      }
      case ScalarValue.TimestampNanosecond v -> {
        var tsBuilder =
            org.apache.arrow.datafusion.proto.ScalarTimestampValue.newBuilder()
                .setTimeNanosecondValue(v.value());
        if (v.tz() != null) tsBuilder.setTimezone(v.tz());
        builder.setTimestampValue(tsBuilder);
      }
      case ScalarValue.IntervalYearMonth v -> builder.setIntervalYearmonthValue(v.value());
      case ScalarValue.IntervalDayTime v ->
          builder.setIntervalDaytimeValue(
              org.apache.arrow.datafusion.proto.IntervalDayTimeValue.newBuilder()
                  .setDays(v.days())
                  .setMilliseconds(v.millis()));
      case ScalarValue.IntervalMonthDayNano v ->
          builder.setIntervalMonthDayNano(
              org.apache.arrow.datafusion.proto.IntervalMonthDayNanoValue.newBuilder()
                  .setMonths(v.months())
                  .setDays(v.days())
                  .setNanos(v.nanos()));
      case ScalarValue.DurationSecond v -> builder.setDurationSecondValue(v.value());
      case ScalarValue.DurationMillisecond v -> builder.setDurationMillisecondValue(v.value());
      case ScalarValue.DurationMicrosecond v -> builder.setDurationMicrosecondValue(v.value());
      case ScalarValue.DurationNanosecond v -> builder.setDurationNanosecondValue(v.value());
      case ScalarValue.Decimal32 v ->
          builder.setDecimal32Value(
              org.apache.arrow.datafusion.proto.Decimal32.newBuilder()
                  .setValue(intToDecimalBytes(v.value()))
                  .setP(v.precision())
                  .setS(v.scale()));
      case ScalarValue.Decimal64 v ->
          builder.setDecimal64Value(
              org.apache.arrow.datafusion.proto.Decimal64.newBuilder()
                  .setValue(longToDecimalBytes(v.value()))
                  .setP(v.precision())
                  .setS(v.scale()));
      case ScalarValue.Decimal128 v ->
          builder.setDecimal128Value(
              org.apache.arrow.datafusion.proto.Decimal128.newBuilder()
                  .setValue(bigIntegerToDecimalBytes(v.unscaledValue(), 16))
                  .setP(v.precision())
                  .setS(v.scale()));
      case ScalarValue.Decimal256 v ->
          builder.setDecimal256Value(
              org.apache.arrow.datafusion.proto.Decimal256.newBuilder()
                  .setValue(bigIntegerToDecimalBytes(v.unscaledValue(), 32))
                  .setP(v.precision())
                  .setS(v.scale()));
      case ScalarValue.Null ignored ->
          builder.setNullValue(org.apache.arrow.datafusion.proto.ArrowType.getDefaultInstance());
      default ->
          throw new UnsupportedOperationException(
              "Unsupported ScalarValue type for proto: " + value.getClass().getSimpleName());
    }
    return builder.build();
  }

  // Decimal bytes use big-endian two's complement, matching datafusion-proto's to_be_bytes().
  private static int decimalBytesToInt(ByteString bytes) {
    return new BigInteger(bytes.toByteArray()).intValue();
  }

  private static long decimalBytesToLong(ByteString bytes) {
    return new BigInteger(bytes.toByteArray()).longValue();
  }

  private static BigInteger decimalBytesToBigInteger(ByteString bytes) {
    return new BigInteger(bytes.toByteArray());
  }

  private static ByteString intToDecimalBytes(int value) {
    byte[] b = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    return ByteString.copyFrom(b);
  }

  private static ByteString longToDecimalBytes(long value) {
    byte[] b = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    return ByteString.copyFrom(b);
  }

  private static ByteString bigIntegerToDecimalBytes(BigInteger value, int byteWidth) {
    byte[] bigEndian = value.toByteArray();
    byte[] result = new byte[byteWidth];
    // Sign-extend: fill with 0xFF if negative, 0x00 if positive
    byte fill = (byte) (value.signum() < 0 ? 0xFF : 0x00);
    java.util.Arrays.fill(result, fill);
    // Copy big-endian bytes into result (right-aligned)
    int srcStart = Math.max(0, bigEndian.length - byteWidth);
    int destStart = Math.max(0, byteWidth - bigEndian.length);
    System.arraycopy(bigEndian, srcStart, result, destStart, bigEndian.length - srcStart);
    return ByteString.copyFrom(result);
  }
}
