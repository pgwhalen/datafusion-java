package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
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
import org.apache.arrow.datafusion.generated.DfVolatility;
import org.apache.arrow.datafusion.logical_expr.Accumulator;
import org.apache.arrow.datafusion.logical_expr.AggregateUDF;
import org.apache.arrow.datafusion.proto.LogicalExprList;
import org.apache.arrow.datafusion.proto.LogicalExprNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Adapts a user-implemented {@link AggregateUDF} to the Diplomat-generated {@link
 * DfAggregateUdfTrait} interface for FFI callbacks.
 */
public final class DfAggregateUDFAdapter implements DfAggregateUdfTrait {
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
  public long nameRaw() {
    return NativeUtil.toRawStringArray(List.of(udaf.name()));
  }

  @Override
  public DfVolatility volatility() {
    return switch (udaf.signature().volatility()) {
      case IMMUTABLE -> DfVolatility.IMMUTABLE;
      case STABLE -> DfVolatility.STABLE;
      case VOLATILE -> DfVolatility.VOLATILE;
    };
  }

  @Override
  public int returnField(
      long argTypesAddr, long argTypesLen, long outSchemaAddr, long errorAddr, long errorCap) {
    try {
      List<Field> argFields = ArrowFfiUtil.importFieldArray(allocator, argTypesAddr, argTypesLen);
      Field resultField = udaf.returnField(argFields);
      ArrowFfiUtil.exportFieldToAddress(allocator, resultField, outSchemaAddr);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public int stateFieldsCount(long returnFieldAddr, long errorAddr, long errorCap) {
    try {
      Field returnField = ArrowFfiUtil.importFieldFromAddress(allocator, returnFieldAddr);
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
      Field returnField = ArrowFfiUtil.importFieldFromAddress(allocator, returnFieldAddr);
      List<Field> fields = udaf.stateFields(returnField);
      for (int i = 0; i < fields.size(); i++) {
        long offset = (long) i * ArrowFfiUtil.ARROW_SCHEMA_SIZE;
        ArrowFfiUtil.exportFieldToAddress(allocator, fields.get(i), resultAddr + offset);
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
      List<Field> argFields = ArrowFfiUtil.importFieldArray(allocator, argTypesAddr, argTypesLen);
      List<Field> coerced = udaf.coerceTypes(argFields);
      int count = (int) Math.min(coerced.size(), resultCap);
      for (int i = 0; i < count; i++) {
        long offset = (long) i * ArrowFfiUtil.ARROW_SCHEMA_SIZE;
        ArrowFfiUtil.exportFieldToAddress(allocator, coerced.get(i), resultAddr + offset);
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
      MemorySegment data =
          MemorySegment.ofAddress(addr).reinterpret(count * ArrowFfiUtil.WRAPPED_ARRAY_SIZE);
      for (int i = 0; i < count; i++) {
        long elementOffset = (long) i * ArrowFfiUtil.WRAPPED_ARRAY_SIZE;
        MemorySegment arraySegment = data.asSlice(elementOffset, ArrowFfiUtil.ARROW_ARRAY_SIZE);
        MemorySegment schemaSegment =
            data.asSlice(
                elementOffset + ArrowFfiUtil.ARROW_ARRAY_SIZE, ArrowFfiUtil.ARROW_SCHEMA_SIZE);

        try (ArrowArray ffiArray = ArrowFfiUtil.importArrayFromSegment(allocator, arraySegment);
            ArrowSchema ffiSchema =
                ArrowFfiUtil.importSchemaFromSegment(allocator, schemaSegment)) {
          FieldVector vector = Data.importVector(allocator, ffiArray, ffiSchema, null);
          out.add(vector);
        }
      }
    }
  }
}
