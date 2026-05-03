package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.generated.DfAggregateUdfTrait;
import org.apache.arrow.datafusion.generated.DfExprBytes;
import org.apache.arrow.datafusion.logical_expr.Accumulator;
import org.apache.arrow.datafusion.logical_expr.AggregateUDF;
import org.apache.arrow.datafusion.proto.LogicalExprList;
import org.apache.arrow.datafusion.proto.LogicalExprNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a user-implemented {@link AggregateUDF} to the Diplomat-generated {@link
 * DfAggregateUdfTrait} interface for FFI callbacks.
 */
public final class DfAggregateUDFAdapter implements DfAggregateUdfTrait {
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;
  private static final long WRAPPED_ARRAY_SIZE = ARROW_ARRAY_SIZE + ARROW_SCHEMA_SIZE;

  private final AggregateUDF udaf;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  private final Map<Long, Accumulator> accumulators = new ConcurrentHashMap<>();
  private final AtomicLong nextAccId = new AtomicLong(1);

  public DfAggregateUDFAdapter(
      AggregateUDF udaf, BufferAllocator allocator, boolean fullStackTrace) {
    this.udaf = udaf;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  // ======== UDAF metadata callbacks ========

  @Override
  public long nameTo(long bufAddr, long bufCap) {
    try {
      byte[] bytes = udaf.name().getBytes(StandardCharsets.UTF_8);
      int len = (int) Math.min(bytes.length, bufCap);
      MemorySegment.ofAddress(bufAddr)
          .reinterpret(bufCap)
          .copyFrom(MemorySegment.ofArray(bytes).asSlice(0, len));
      return len;
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public int volatility() {
    return switch (udaf.signature().volatility()) {
      case IMMUTABLE -> 0;
      case STABLE -> 1;
      case VOLATILE -> 2;
    };
  }

  @Override
  public int returnField(
      long argTypesAddr, long argTypesLen, long outSchemaAddr, long errorAddr, long errorCap) {
    try {
      List<Field> argFields = importFieldArray(argTypesAddr, argTypesLen);
      Field resultField = udaf.returnField(argFields);
      exportFieldToAddress(resultField, outSchemaAddr);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public int stateFieldsCount(long returnFieldAddr, long errorAddr, long errorCap) {
    try {
      Field returnField = importFieldFromAddress(returnFieldAddr);
      return udaf.stateFields(returnField).size();
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public int stateFields(
      long returnFieldAddr, long resultAddr, long resultCap, long errorAddr, long errorCap) {
    try {
      Field returnField = importFieldFromAddress(returnFieldAddr);
      List<Field> fields = udaf.stateFields(returnField);
      for (int i = 0; i < fields.size(); i++) {
        long offset = (long) i * ARROW_SCHEMA_SIZE;
        exportFieldToAddress(fields.get(i), resultAddr + offset);
      }
      return fields.size();
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public int coerceTypes(
      long argTypesAddr,
      long argTypesLen,
      long resultAddr,
      long resultCap,
      long errorAddr,
      long errorCap) {
    try {
      List<Field> argFields = importFieldArray(argTypesAddr, argTypesLen);
      List<Field> coerced = udaf.coerceTypes(argFields);
      int count = (int) Math.min(coerced.size(), resultCap);
      for (int i = 0; i < count; i++) {
        long offset = (long) i * ARROW_SCHEMA_SIZE;
        exportFieldToAddress(coerced.get(i), resultAddr + offset);
      }
      return count;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  // ======== Accumulator lifecycle callbacks ========

  @Override
  public long accumulatorCreate(long errorAddr, long errorCap) {
    try {
      Accumulator acc = udaf.createAccumulator();
      long id = nextAccId.getAndIncrement();
      accumulators.put(id, acc);
      return id;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public void accumulatorDrop(long accId) {
    accumulators.remove(accId);
  }

  // ======== Accumulator operation callbacks ========

  @Override
  public int accumulatorUpdate(
      long accId, long argsAddr, long numArgs, long errorAddr, long errorCap) {
    List<FieldVector> argVectors = new ArrayList<>();
    try {
      Accumulator acc = accumulators.get(accId);
      if (acc == null) {
        Errors.writeError(errorAddr, errorCap, "Unknown accumulator ID: " + accId);
        return -1;
      }

      importArrowArrays(argsAddr, numArgs, argVectors);
      acc.updateBatch(argVectors);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    } finally {
      for (FieldVector v : argVectors) {
        v.close();
      }
    }
  }

  @Override
  public long accumulatorEvaluate(long accId, long errorAddr, long errorCap) {
    try {
      Accumulator acc = accumulators.get(accId);
      if (acc == null) {
        Errors.writeError(errorAddr, errorCap, "Unknown accumulator ID: " + accId);
        return 0;
      }

      ScalarValue result = acc.evaluate();
      byte[] protoBytes = ScalarValueProtoConverter.toProto(result).toByteArray();
      DfExprBytes exprBytes = DfExprBytes.newFromSlice(protoBytes);
      return exprBytes.toRawPtr();
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }

  @Override
  public long accumulatorState(long accId, long errorAddr, long errorCap) {
    try {
      Accumulator acc = accumulators.get(accId);
      if (acc == null) {
        Errors.writeError(errorAddr, errorCap, "Unknown accumulator ID: " + accId);
        return 0;
      }

      LogicalExprList.Builder list = LogicalExprList.newBuilder();
      for (ScalarValue sv : acc.state()) {
        list.addExpr(
            LogicalExprNode.newBuilder().setLiteral(ScalarValueProtoConverter.toProto(sv)).build());
      }

      DfExprBytes exprBytes = DfExprBytes.newFromSlice(list.build().toByteArray());
      return exprBytes.toRawPtr();
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }

  @Override
  public int accumulatorMerge(
      long accId, long statesAddr, long numStates, long errorAddr, long errorCap) {
    List<FieldVector> stateVectors = new ArrayList<>();
    try {
      Accumulator acc = accumulators.get(accId);
      if (acc == null) {
        Errors.writeError(errorAddr, errorCap, "Unknown accumulator ID: " + accId);
        return -1;
      }

      importArrowArrays(statesAddr, numStates, stateVectors);
      acc.mergeBatch(stateVectors);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    } finally {
      for (FieldVector v : stateVectors) {
        v.close();
      }
    }
  }

  @Override
  public long accumulatorSize(long accId) {
    Accumulator acc = accumulators.get(accId);
    if (acc == null) {
      return 0;
    }
    return acc.size();
  }

  // ======== Helper methods ========

  /** Import Arrow arrays from FFI (FFI_ArrowArray + FFI_ArrowSchema pairs) into FieldVectors. */
  private void importArrowArrays(long addr, long count, List<FieldVector> out) {
    if (count > 0 && addr != 0) {
      MemorySegment data = MemorySegment.ofAddress(addr).reinterpret(count * WRAPPED_ARRAY_SIZE);
      for (int i = 0; i < count; i++) {
        long elementOffset = (long) i * WRAPPED_ARRAY_SIZE;
        MemorySegment arraySegment = data.asSlice(elementOffset, ARROW_ARRAY_SIZE);
        MemorySegment schemaSegment =
            data.asSlice(elementOffset + ARROW_ARRAY_SIZE, ARROW_SCHEMA_SIZE);

        try (ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
            ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
          MemorySegment arrayDest =
              MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
          arrayDest.copyFrom(arraySegment);

          MemorySegment schemaDest =
              MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
          schemaDest.copyFrom(schemaSegment);

          // Clear release in Rust source so Rust's Drop is a no-op.
          // ArrowArray: release at offset 64. ArrowSchema: release at offset 56.
          arraySegment.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
          schemaSegment.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL);

          FieldVector vector = Data.importVector(allocator, ffiArray, ffiSchema, null);
          out.add(vector);
        }
      }
    }
  }

  /** Import an array of FFI_ArrowSchema structs as a list of Fields. */
  private List<Field> importFieldArray(long addr, long count) {
    List<Field> fields = new ArrayList<>((int) count);
    if (count > 0 && addr != 0) {
      MemorySegment data = MemorySegment.ofAddress(addr).reinterpret(count * ARROW_SCHEMA_SIZE);
      for (int i = 0; i < count; i++) {
        MemorySegment schemaSegment = data.asSlice((long) i * ARROW_SCHEMA_SIZE, ARROW_SCHEMA_SIZE);
        fields.add(importFieldFromSegment(schemaSegment));
      }
    }
    return fields;
  }

  /** Import a Field from an FFI_ArrowSchema at a given address. */
  private Field importFieldFromAddress(long addr) {
    MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(ARROW_SCHEMA_SIZE);
    return importFieldFromSegment(segment);
  }

  private Field importFieldFromSegment(MemorySegment schemaSegment) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      MemorySegment dest =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      dest.copyFrom(schemaSegment);
      schemaSegment.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL);
      Schema schema = Data.importSchema(allocator, ffiSchema, null);
      return schema.getFields().get(0);
    }
  }

  private void exportFieldToAddress(Field field, long outAddr) {
    Schema wrapperSchema = new Schema(List.of(field));
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, wrapperSchema, null, ffiSchema);
      MemorySegment src =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      MemorySegment dest = MemorySegment.ofAddress(outAddr).reinterpret(ARROW_SCHEMA_SIZE);
      dest.copyFrom(src);
      src.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }
  }
}
