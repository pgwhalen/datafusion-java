package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.generated.DfScalarUdfTrait;
import org.apache.arrow.datafusion.generated.DfVolatility;
import org.apache.arrow.datafusion.logical_expr.ScalarUDF;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Adapts a user-implemented {@link ScalarUDF} to the Diplomat-generated {@link DfScalarUdfTrait}
 * interface for FFI callbacks.
 */
public final class DfScalarUDFAdapter implements DfScalarUdfTrait {
  private final ScalarUDF udf;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  public DfScalarUDFAdapter(ScalarUDF udf, BufferAllocator allocator, boolean fullStackTrace) {
    this.udf = udf;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long nameRaw() {
    return NativeUtil.toRawStringArray(List.of(udf.name()));
  }

  @Override
  public DfVolatility volatility() {
    return switch (udf.signature().volatility()) {
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
      Field resultField = udf.returnField(argFields);
      ArrowFfiUtil.exportFieldToAddress(allocator, resultField, outSchemaAddr);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }

  @Override
  public int invoke(
      long argsAddr,
      long numArgs,
      long numRows,
      long argFieldsAddr,
      long returnFieldAddr,
      long outArrayAddr,
      long outSchemaAddr,
      long errorAddr,
      long errorCap) {
    List<FieldVector> argVectors = new ArrayList<>();
    try {
      // Import argument vectors: each is (FFI_ArrowArray(80), FFI_ArrowSchema(72)) pair
      if (numArgs > 0 && argsAddr != 0) {
        MemorySegment argsData =
            MemorySegment.ofAddress(argsAddr)
                .reinterpret(numArgs * ArrowFfiUtil.WRAPPED_ARRAY_SIZE);
        for (int i = 0; i < numArgs; i++) {
          long elementOffset = (long) i * ArrowFfiUtil.WRAPPED_ARRAY_SIZE;
          MemorySegment arraySegment =
              argsData.asSlice(elementOffset, ArrowFfiUtil.ARROW_ARRAY_SIZE);
          MemorySegment schemaSegment =
              argsData.asSlice(
                  elementOffset + ArrowFfiUtil.ARROW_ARRAY_SIZE, ArrowFfiUtil.ARROW_SCHEMA_SIZE);

          try (ArrowArray ffiArray = ArrowFfiUtil.importArrayFromSegment(allocator, arraySegment);
              ArrowSchema ffiSchema =
                  ArrowFfiUtil.importSchemaFromSegment(allocator, schemaSegment)) {
            FieldVector vector = Data.importVector(allocator, ffiArray, ffiSchema, null);
            argVectors.add(vector);
          }
        }
      }

      List<Field> argFields = ArrowFfiUtil.importFieldArray(allocator, argFieldsAddr, numArgs);
      Field returnField = ArrowFfiUtil.importFieldFromAddress(allocator, returnFieldAddr);

      // Call Java UDF
      FieldVector result = udf.invoke(argVectors, argFields, (int) numRows, returnField, allocator);

      // Export result as VectorSchemaRoot (struct wrapping one column) for FFI.
      // Rust expects a struct array and extracts column(0).
      VectorSchemaRoot resultRoot =
          new VectorSchemaRoot(List.of(result.getField()), List.of(result));
      resultRoot.setRowCount(result.getValueCount());

      try {
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, resultRoot, null, outArray, outSchema);
          ArrowFfiUtil.copyArrayToAddress(
              MemorySegment.ofAddress(outArray.memoryAddress())
                  .reinterpret(ArrowFfiUtil.ARROW_ARRAY_SIZE),
              outArrayAddr);
          ArrowFfiUtil.copySchemaToAddress(
              MemorySegment.ofAddress(outSchema.memoryAddress())
                  .reinterpret(ArrowFfiUtil.ARROW_SCHEMA_SIZE),
              outSchemaAddr);
        }
      } finally {
        result.close();
      }

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
  public int coerceTypes(
      long argTypesAddr,
      long argTypesLen,
      long resultAddr,
      long resultCap,
      long errorAddr,
      long errorCap) {
    try {
      List<Field> argFields = ArrowFfiUtil.importFieldArray(allocator, argTypesAddr, argTypesLen);
      List<Field> coerced = udf.coerceTypes(argFields);

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
}
