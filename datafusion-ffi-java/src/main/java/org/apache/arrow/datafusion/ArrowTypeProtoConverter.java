package org.apache.arrow.datafusion;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Converts between proto {@code ArrowType} messages and Arrow Java {@code ArrowType} instances.
 *
 * <p>Used by {@link ExprProtoConverter} for Cast/TryCast/Placeholder data types, and by {@link
 * ScalarValueProtoConverter} for typed scalars.
 */
final class ArrowTypeProtoConverter {
  private ArrowTypeProtoConverter() {}

  static ArrowType fromProto(org.apache.arrow.datafusion.proto.ArrowType proto) {
    if (proto == null
        || proto.getArrowTypeEnumCase()
            == org.apache.arrow.datafusion.proto.ArrowType.ArrowTypeEnumCase
                .ARROWTYPEENUM_NOT_SET) {
      return null;
    }
    return switch (proto.getArrowTypeEnumCase()) {
      case NONE -> ArrowType.Null.INSTANCE;
      case BOOL -> ArrowType.Bool.INSTANCE;
      case UINT8 -> new ArrowType.Int(8, false);
      case INT8 -> new ArrowType.Int(8, true);
      case UINT16 -> new ArrowType.Int(16, false);
      case INT16 -> new ArrowType.Int(16, true);
      case UINT32 -> new ArrowType.Int(32, false);
      case INT32 -> new ArrowType.Int(32, true);
      case UINT64 -> new ArrowType.Int(64, false);
      case INT64 -> new ArrowType.Int(64, true);
      case FLOAT16 -> new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
      case FLOAT32 -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case FLOAT64 -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case UTF8 -> ArrowType.Utf8.INSTANCE;
      case UTF8_VIEW -> new ArrowType.Utf8View();
      case LARGE_UTF8 -> ArrowType.LargeUtf8.INSTANCE;
      case BINARY -> ArrowType.Binary.INSTANCE;
      case BINARY_VIEW -> new ArrowType.BinaryView();
      case FIXED_SIZE_BINARY -> new ArrowType.FixedSizeBinary(proto.getFIXEDSIZEBINARY());
      case LARGE_BINARY -> ArrowType.LargeBinary.INSTANCE;
      case DATE32 -> new ArrowType.Date(DateUnit.DAY);
      case DATE64 -> new ArrowType.Date(DateUnit.MILLISECOND);
      case DURATION -> new ArrowType.Duration(convertTimeUnit(proto.getDURATION()));
      case TIMESTAMP -> {
        var ts = proto.getTIMESTAMP();
        String tz = ts.getTimezone().isEmpty() ? null : ts.getTimezone();
        yield new ArrowType.Timestamp(convertTimeUnit(ts.getTimeUnit()), tz);
      }
      case TIME32 -> new ArrowType.Time(convertTimeUnit(proto.getTIME32()), 32);
      case TIME64 -> new ArrowType.Time(convertTimeUnit(proto.getTIME64()), 64);
      case INTERVAL -> new ArrowType.Interval(convertIntervalUnit(proto.getINTERVAL()));
      case DECIMAL32 ->
          new ArrowType.Decimal(
              proto.getDECIMAL32().getPrecision(), proto.getDECIMAL32().getScale(), 32);
      case DECIMAL64 ->
          new ArrowType.Decimal(
              proto.getDECIMAL64().getPrecision(), proto.getDECIMAL64().getScale(), 64);
      case DECIMAL128 ->
          new ArrowType.Decimal(
              proto.getDECIMAL128().getPrecision(), proto.getDECIMAL128().getScale(), 128);
      case DECIMAL256 ->
          new ArrowType.Decimal(
              proto.getDECIMAL256().getPrecision(), proto.getDECIMAL256().getScale(), 256);
      default -> null;
    };
  }

  static org.apache.arrow.datafusion.proto.ArrowType toProto(ArrowType arrowType) {
    if (arrowType == null) {
      return null;
    }
    var builder = org.apache.arrow.datafusion.proto.ArrowType.newBuilder();
    var empty = org.apache.arrow.datafusion.proto.EmptyMessage.getDefaultInstance();

    if (arrowType instanceof ArrowType.Null) {
      builder.setNONE(empty);
    } else if (arrowType instanceof ArrowType.Bool) {
      builder.setBOOL(empty);
    } else if (arrowType instanceof ArrowType.Int intType) {
      switch (intType.getBitWidth()) {
        case 8 -> {
          if (intType.getIsSigned()) builder.setINT8(empty);
          else builder.setUINT8(empty);
        }
        case 16 -> {
          if (intType.getIsSigned()) builder.setINT16(empty);
          else builder.setUINT16(empty);
        }
        case 32 -> {
          if (intType.getIsSigned()) builder.setINT32(empty);
          else builder.setUINT32(empty);
        }
        case 64 -> {
          if (intType.getIsSigned()) builder.setINT64(empty);
          else builder.setUINT64(empty);
        }
        default ->
            throw new UnsupportedOperationException(
                "Unsupported int width: " + intType.getBitWidth());
      }
    } else if (arrowType instanceof ArrowType.FloatingPoint fp) {
      switch (fp.getPrecision()) {
        case HALF -> builder.setFLOAT16(empty);
        case SINGLE -> builder.setFLOAT32(empty);
        case DOUBLE -> builder.setFLOAT64(empty);
      }
    } else if (arrowType instanceof ArrowType.Utf8) {
      builder.setUTF8(empty);
    } else if (arrowType instanceof ArrowType.Utf8View) {
      builder.setUTF8VIEW(empty);
    } else if (arrowType instanceof ArrowType.LargeUtf8) {
      builder.setLARGEUTF8(empty);
    } else if (arrowType instanceof ArrowType.Binary) {
      builder.setBINARY(empty);
    } else if (arrowType instanceof ArrowType.BinaryView) {
      builder.setBINARYVIEW(empty);
    } else if (arrowType instanceof ArrowType.FixedSizeBinary fsb) {
      builder.setFIXEDSIZEBINARY(fsb.getByteWidth());
    } else if (arrowType instanceof ArrowType.LargeBinary) {
      builder.setLARGEBINARY(empty);
    } else if (arrowType instanceof ArrowType.Date dateType) {
      switch (dateType.getUnit()) {
        case DAY -> builder.setDATE32(empty);
        case MILLISECOND -> builder.setDATE64(empty);
      }
    } else if (arrowType instanceof ArrowType.Duration durType) {
      builder.setDURATION(toProtoTimeUnit(durType.getUnit()));
    } else if (arrowType instanceof ArrowType.Timestamp tsType) {
      var tsBuilder = org.apache.arrow.datafusion.proto.Timestamp.newBuilder();
      tsBuilder.setTimeUnit(toProtoTimeUnit(tsType.getUnit()));
      if (tsType.getTimezone() != null) {
        tsBuilder.setTimezone(tsType.getTimezone());
      }
      builder.setTIMESTAMP(tsBuilder);
    } else if (arrowType instanceof ArrowType.Time timeType) {
      if (timeType.getBitWidth() == 32) {
        builder.setTIME32(toProtoTimeUnit(timeType.getUnit()));
      } else {
        builder.setTIME64(toProtoTimeUnit(timeType.getUnit()));
      }
    } else if (arrowType instanceof ArrowType.Interval ivType) {
      builder.setINTERVAL(toProtoIntervalUnit(ivType.getUnit()));
    } else if (arrowType instanceof ArrowType.Decimal decType) {
      switch (decType.getBitWidth()) {
        case 32 ->
            builder.setDECIMAL32(
                org.apache.arrow.datafusion.proto.Decimal32Type.newBuilder()
                    .setPrecision(decType.getPrecision())
                    .setScale(decType.getScale()));
        case 64 ->
            builder.setDECIMAL64(
                org.apache.arrow.datafusion.proto.Decimal64Type.newBuilder()
                    .setPrecision(decType.getPrecision())
                    .setScale(decType.getScale()));
        case 128 ->
            builder.setDECIMAL128(
                org.apache.arrow.datafusion.proto.Decimal128Type.newBuilder()
                    .setPrecision(decType.getPrecision())
                    .setScale(decType.getScale()));
        case 256 ->
            builder.setDECIMAL256(
                org.apache.arrow.datafusion.proto.Decimal256Type.newBuilder()
                    .setPrecision(decType.getPrecision())
                    .setScale(decType.getScale()));
        default ->
            throw new UnsupportedOperationException(
                "Unsupported decimal width: " + decType.getBitWidth());
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported ArrowType for proto conversion: " + arrowType);
    }

    return builder.build();
  }

  private static TimeUnit convertTimeUnit(org.apache.arrow.datafusion.proto.TimeUnit proto) {
    return switch (proto) {
      case Second -> TimeUnit.SECOND;
      case Millisecond -> TimeUnit.MILLISECOND;
      case Microsecond -> TimeUnit.MICROSECOND;
      case Nanosecond -> TimeUnit.NANOSECOND;
      default -> TimeUnit.NANOSECOND;
    };
  }

  private static IntervalUnit convertIntervalUnit(
      org.apache.arrow.datafusion.proto.IntervalUnit proto) {
    return switch (proto) {
      case YearMonth -> IntervalUnit.YEAR_MONTH;
      case DayTime -> IntervalUnit.DAY_TIME;
      case MonthDayNano -> IntervalUnit.MONTH_DAY_NANO;
      default -> IntervalUnit.MONTH_DAY_NANO;
    };
  }

  private static org.apache.arrow.datafusion.proto.TimeUnit toProtoTimeUnit(TimeUnit unit) {
    return switch (unit) {
      case SECOND -> org.apache.arrow.datafusion.proto.TimeUnit.Second;
      case MILLISECOND -> org.apache.arrow.datafusion.proto.TimeUnit.Millisecond;
      case MICROSECOND -> org.apache.arrow.datafusion.proto.TimeUnit.Microsecond;
      case NANOSECOND -> org.apache.arrow.datafusion.proto.TimeUnit.Nanosecond;
    };
  }

  private static org.apache.arrow.datafusion.proto.IntervalUnit toProtoIntervalUnit(
      IntervalUnit unit) {
    return switch (unit) {
      case YEAR_MONTH -> org.apache.arrow.datafusion.proto.IntervalUnit.YearMonth;
      case DAY_TIME -> org.apache.arrow.datafusion.proto.IntervalUnit.DayTime;
      case MONTH_DAY_NANO -> org.apache.arrow.datafusion.proto.IntervalUnit.MonthDayNano;
    };
  }
}
