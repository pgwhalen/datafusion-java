package org.apache.arrow.datafusion;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Converts between proto {@code ScalarNestedValue} (IPC-encoded Arrow data) and Java {@link
 * ScalarValue} records for complex types (List, LargeList, FixedSizeList, Struct, Map).
 *
 * <p>Package-private — used only by {@link ScalarValueProtoConverter}.
 */
final class ScalarNestedValueConverter {
  private ScalarNestedValueConverter() {}

  private static final BufferAllocator ALLOCATOR = new RootAllocator();

  // -- Deserialization: proto ScalarNestedValue → ScalarValue --

  static ScalarValue.ListValue deserializeList(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    try (VectorSchemaRoot root = deserializeIpc(nested)) {
      ListVector listVector = (ListVector) root.getVector(0);
      return new ScalarValue.ListValue(extractListElements(listVector, 0));
    }
  }

  static ScalarValue.LargeListValue deserializeLargeList(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    try (VectorSchemaRoot root = deserializeIpc(nested)) {
      LargeListVector listVector = (LargeListVector) root.getVector(0);
      return new ScalarValue.LargeListValue(extractLargeListElements(listVector, 0));
    }
  }

  static ScalarValue.FixedSizeListValue deserializeFixedSizeList(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    try (VectorSchemaRoot root = deserializeIpc(nested)) {
      FixedSizeListVector listVector = (FixedSizeListVector) root.getVector(0);
      int listSize = listVector.getListSize();
      return new ScalarValue.FixedSizeListValue(
          extractFixedSizeListElements(listVector, 0), listSize);
    }
  }

  static ScalarValue.StructValue deserializeStruct(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    try (VectorSchemaRoot root = deserializeIpc(nested)) {
      // For struct scalars, the root itself contains the struct fields as columns
      var fields = new LinkedHashMap<String, ScalarValue>();
      for (FieldVector vector : root.getFieldVectors()) {
        fields.put(vector.getName(), fromVector(vector, 0));
      }
      return new ScalarValue.StructValue(fields);
    }
  }

  static ScalarValue.MapValue deserializeMap(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    try (VectorSchemaRoot root = deserializeIpc(nested)) {
      MapVector mapVector = (MapVector) root.getVector(0);
      return new ScalarValue.MapValue(extractMapEntries(mapVector, 0));
    }
  }

  // -- IPC deserialization core --

  private static VectorSchemaRoot deserializeIpc(
      org.apache.arrow.datafusion.proto.ScalarNestedValue nested) {
    Schema schema = ArrowTypeProtoConverter.schemaFromProto(nested.getSchema());
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOCATOR);
    try {
      byte[] ipcMessage = nested.getIpcMessage().toByteArray();
      byte[] arrowData = nested.getArrowData().toByteArray();

      // Deserialize the RecordBatch message
      ArrowRecordBatch batch = deserializeRecordBatch(ipcMessage, arrowData);
      try {
        VectorLoader loader = new VectorLoader(root);
        loader.load(batch);
      } finally {
        batch.close();
      }
      return root;
    } catch (Exception e) {
      root.close();
      throw new RuntimeException("Failed to deserialize ScalarNestedValue IPC data", e);
    }
  }

  private static ArrowRecordBatch deserializeRecordBatch(byte[] ipcMessage, byte[] arrowData)
      throws IOException {
    // The ipc_message contains the flatbuffer RecordBatch message (continuation + metadata).
    // The arrow_data contains the body buffers.
    // Combine them into a single stream: message followed by body, then use the
    // ReadChannel + allocator overload which reads the message and body together.
    byte[] combined = new byte[ipcMessage.length + arrowData.length];
    System.arraycopy(ipcMessage, 0, combined, 0, ipcMessage.length);
    System.arraycopy(arrowData, 0, combined, ipcMessage.length, arrowData.length);
    return MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(combined))), ALLOCATOR);
  }

  // -- Vector element extraction --

  static ScalarValue fromVector(FieldVector vector, int index) {
    if (vector.isNull(index)) {
      return new ScalarValue.Null();
    }
    return switch (vector) {
      case BitVector v -> new ScalarValue.BooleanValue(v.get(index) != 0);
      case TinyIntVector v -> new ScalarValue.Int8(v.get(index));
      case SmallIntVector v -> new ScalarValue.Int16(v.get(index));
      case IntVector v -> new ScalarValue.Int32(v.get(index));
      case BigIntVector v -> new ScalarValue.Int64(v.get(index));
      case UInt1Vector v -> new ScalarValue.UInt8((short) (v.get(index) & 0xFF));
      case UInt2Vector v -> {
        // UInt2Vector.get() returns a char (unsigned 16-bit)
        char c = v.get(index);
        yield new ScalarValue.UInt16(c);
      }
      case UInt4Vector v -> new ScalarValue.UInt32(Integer.toUnsignedLong(v.get(index)));
      case UInt8Vector v -> {
        long raw = v.get(index);
        BigInteger unsigned =
            raw >= 0
                ? BigInteger.valueOf(raw)
                : BigInteger.valueOf(raw).add(BigInteger.TWO.pow(64));
        yield new ScalarValue.UInt64(unsigned);
      }
      case Float2Vector v -> new ScalarValue.Float16(Float.float16ToFloat(v.get(index)));
      case Float4Vector v -> new ScalarValue.Float32(v.get(index));
      case Float8Vector v -> new ScalarValue.Float64(v.get(index));
      case VarCharVector v -> new ScalarValue.Utf8(new String(v.get(index)));
      case LargeVarCharVector v -> new ScalarValue.LargeUtf8(new String(v.get(index)));
      case ViewVarCharVector v -> new ScalarValue.Utf8View(new String(v.get(index)));
      case VarBinaryVector v -> new ScalarValue.Binary(v.get(index));
      case LargeVarBinaryVector v -> new ScalarValue.LargeBinary(v.get(index));
      case ViewVarBinaryVector v -> new ScalarValue.BinaryView(v.get(index));
      case FixedSizeBinaryVector v -> new ScalarValue.FixedSizeBinary(v.get(index));
      case DateDayVector v -> new ScalarValue.Date32(v.get(index));
      case DateMilliVector v -> new ScalarValue.Date64(v.get(index));
      case TimeSecVector v -> new ScalarValue.Time32Second(v.get(index));
      case TimeMilliVector v -> new ScalarValue.Time32Millisecond(v.get(index));
      case TimeMicroVector v -> new ScalarValue.Time64Microsecond(v.get(index));
      case TimeNanoVector v -> new ScalarValue.Time64Nanosecond(v.get(index));
      case TimeStampSecVector v -> new ScalarValue.TimestampSecond(v.get(index), null);
      case TimeStampSecTZVector v -> new ScalarValue.TimestampSecond(v.get(index), v.getTimeZone());
      case TimeStampMilliVector v -> new ScalarValue.TimestampMillisecond(v.get(index), null);
      case TimeStampMilliTZVector v ->
          new ScalarValue.TimestampMillisecond(v.get(index), v.getTimeZone());
      case TimeStampMicroVector v -> new ScalarValue.TimestampMicrosecond(v.get(index), null);
      case TimeStampMicroTZVector v ->
          new ScalarValue.TimestampMicrosecond(v.get(index), v.getTimeZone());
      case TimeStampNanoVector v -> new ScalarValue.TimestampNanosecond(v.get(index), null);
      case TimeStampNanoTZVector v ->
          new ScalarValue.TimestampNanosecond(v.get(index), v.getTimeZone());
      case DurationVector v -> {
        var unit = ((ArrowType.Duration) v.getField().getType()).getUnit();
        long val = DurationVector.get(v.getDataBuffer(), index);
        yield switch (unit) {
          case SECOND -> new ScalarValue.DurationSecond(val);
          case MILLISECOND -> new ScalarValue.DurationMillisecond(val);
          case MICROSECOND -> new ScalarValue.DurationMicrosecond(val);
          case NANOSECOND -> new ScalarValue.DurationNanosecond(val);
        };
      }
      case IntervalYearVector v -> new ScalarValue.IntervalYearMonth(v.get(index));
      case IntervalDayVector v -> {
        ArrowBuf buf = v.getDataBuffer();
        int days = IntervalDayVector.getDays(buf, index);
        int millis = IntervalDayVector.getMilliseconds(buf, index);
        yield new ScalarValue.IntervalDayTime(days, millis);
      }
      case IntervalMonthDayNanoVector v -> {
        ArrowBuf buf = v.getDataBuffer();
        int months = IntervalMonthDayNanoVector.getMonths(buf, index);
        int days = IntervalMonthDayNanoVector.getDays(buf, index);
        long nanos = IntervalMonthDayNanoVector.getNanoseconds(buf, index);
        yield new ScalarValue.IntervalMonthDayNano(months, days, nanos);
      }
      case DecimalVector v -> {
        var decType = (ArrowType.Decimal) v.getField().getType();
        BigInteger unscaled = v.getObject(index).unscaledValue();
        yield new ScalarValue.Decimal128(unscaled, decType.getPrecision(), decType.getScale());
      }
      case Decimal256Vector v -> {
        var decType = (ArrowType.Decimal) v.getField().getType();
        BigInteger unscaled = v.getObject(index).unscaledValue();
        yield new ScalarValue.Decimal256(unscaled, decType.getPrecision(), decType.getScale());
      }
        // MapVector extends ListVector, so it must come first
      case MapVector v -> new ScalarValue.MapValue(extractMapEntries(v, index));
      case ListVector v -> new ScalarValue.ListValue(extractListElements(v, index));
      case LargeListVector v -> new ScalarValue.LargeListValue(extractLargeListElements(v, index));
      case FixedSizeListVector v ->
          new ScalarValue.FixedSizeListValue(
              extractFixedSizeListElements(v, index), v.getListSize());
      case StructVector v -> {
        var fields = new LinkedHashMap<String, ScalarValue>();
        for (FieldVector child : v.getChildrenFromFields()) {
          fields.put(child.getName(), fromVector(child, index));
        }
        yield new ScalarValue.StructValue(fields);
      }
      case DenseUnionVector v -> {
        int typeId = v.getTypeId(index);
        FieldVector child = (FieldVector) v.getVectorByType((byte) typeId);
        int childIndex = v.getOffset(index);
        ScalarValue childVal = fromVector(child, childIndex);
        yield new ScalarValue.UnionValue(typeId, childVal);
      }
      case NullVector ignored -> new ScalarValue.Null();
      default ->
          throw new UnsupportedOperationException(
              "Cannot convert vector type to ScalarValue: " + vector.getClass().getSimpleName());
    };
  }

  private static List<ScalarValue> extractListElements(ListVector vector, int index) {
    int start = vector.getOffsetBuffer().getInt((long) index * ListVector.OFFSET_WIDTH);
    int end = vector.getOffsetBuffer().getInt((long) (index + 1) * ListVector.OFFSET_WIDTH);
    FieldVector dataVector = vector.getDataVector();
    List<ScalarValue> result = new ArrayList<>(end - start);
    for (int i = start; i < end; i++) {
      result.add(fromVector(dataVector, i));
    }
    return result;
  }

  private static List<ScalarValue> extractLargeListElements(LargeListVector vector, int index) {
    long start = vector.getOffsetBuffer().getLong((long) index * LargeListVector.OFFSET_WIDTH);
    long end = vector.getOffsetBuffer().getLong((long) (index + 1) * LargeListVector.OFFSET_WIDTH);
    FieldVector dataVector = vector.getDataVector();
    List<ScalarValue> result = new ArrayList<>((int) (end - start));
    for (long i = start; i < end; i++) {
      result.add(fromVector(dataVector, (int) i));
    }
    return result;
  }

  private static List<ScalarValue> extractFixedSizeListElements(
      FixedSizeListVector vector, int index) {
    int listSize = vector.getListSize();
    int start = index * listSize;
    FieldVector dataVector = vector.getDataVector();
    List<ScalarValue> result = new ArrayList<>(listSize);
    for (int i = start; i < start + listSize; i++) {
      result.add(fromVector(dataVector, i));
    }
    return result;
  }

  private static List<Map.Entry<ScalarValue, ScalarValue>> extractMapEntries(
      MapVector vector, int index) {
    // MapVector is a ListVector of structs with "key" and "value" fields
    int start = vector.getOffsetBuffer().getInt((long) index * MapVector.OFFSET_WIDTH);
    int end = vector.getOffsetBuffer().getInt((long) (index + 1) * MapVector.OFFSET_WIDTH);
    StructVector entries = (StructVector) vector.getDataVector();
    List<Map.Entry<ScalarValue, ScalarValue>> result = new ArrayList<>(end - start);
    FieldVector keyVector = entries.getChild("key");
    FieldVector valueVector = entries.getChild("value");
    for (int i = start; i < end; i++) {
      ScalarValue key = fromVector(keyVector, i);
      ScalarValue value = fromVector(valueVector, i);
      result.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
    }
    return result;
  }
}
