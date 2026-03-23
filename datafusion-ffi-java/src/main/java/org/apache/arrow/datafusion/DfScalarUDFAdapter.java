package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.generated.DfScalarUdfTrait;
import org.apache.arrow.datafusion.logical_expr.ScalarUDF;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a user-implemented {@link ScalarUDF} to the Diplomat-generated {@link DfScalarUdfTrait}
 * interface for FFI callbacks.
 */
public final class DfScalarUDFAdapter implements DfScalarUdfTrait {
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;
  // (FFI_ArrowArray, FFI_ArrowSchema) pair size
  private static final long WRAPPED_ARRAY_SIZE = ARROW_ARRAY_SIZE + ARROW_SCHEMA_SIZE;

  private final ScalarUDF udf;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  public DfScalarUDFAdapter(ScalarUDF udf, BufferAllocator allocator, boolean fullStackTrace) {
    this.udf = udf;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long nameTo(long bufAddr, long bufCap) {
    try {
      byte[] bytes = udf.name().getBytes(StandardCharsets.UTF_8);
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
    return switch (udf.signature().volatility()) {
      case IMMUTABLE -> 0;
      case STABLE -> 1;
      case VOLATILE -> 2;
    };
  }

  @Override
  public int returnField(
      long argTypesAddr, long argTypesLen, long outSchemaAddr, long errorAddr, long errorCap) {
    try {
      // Import arg types: each is an FFI_ArrowSchema (72 bytes) wrapping a Schema with one field
      List<Field> argFields = importFieldArray(argTypesAddr, argTypesLen);

      // Call Java UDF
      Field resultField = udf.returnField(argFields);

      // Export result Field as FFI_ArrowSchema to the output address
      exportFieldToAddress(resultField, outSchemaAddr);

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
            MemorySegment.ofAddress(argsAddr).reinterpret(numArgs * WRAPPED_ARRAY_SIZE);
        for (int i = 0; i < numArgs; i++) {
          long elementOffset = (long) i * WRAPPED_ARRAY_SIZE;
          MemorySegment arraySegment = argsData.asSlice(elementOffset, ARROW_ARRAY_SIZE);
          MemorySegment schemaSegment =
              argsData.asSlice(elementOffset + ARROW_ARRAY_SIZE, ARROW_SCHEMA_SIZE);

          try (ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
              ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            // Copy FFI data into Arrow Java's structs
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
            argVectors.add(vector);
          }
        }
      }

      // Import arg fields
      List<Field> argFields = importFieldArray(argFieldsAddr, numArgs);

      // Import return field
      Field returnField = importFieldFromAddress(returnFieldAddr);

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

          MemorySegment arraySrc =
              MemorySegment.ofAddress(outArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
          MemorySegment schemaSrc =
              MemorySegment.ofAddress(outSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);

          // Copy to Rust output addresses
          MemorySegment arrayDest =
              MemorySegment.ofAddress(outArrayAddr).reinterpret(ARROW_ARRAY_SIZE);
          arrayDest.copyFrom(arraySrc);

          MemorySegment schemaDest =
              MemorySegment.ofAddress(outSchemaAddr).reinterpret(ARROW_SCHEMA_SIZE);
          schemaDest.copyFrom(schemaSrc);

          // Clear release in Java source - dest now owns it
          arraySrc.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
          schemaSrc.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
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
      // Import arg types
      List<Field> argFields = importFieldArray(argTypesAddr, argTypesLen);

      // Call Java UDF
      List<Field> coerced = udf.coerceTypes(argFields);

      // Export each coerced field to the result buffer
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

  // ======== Helper methods ========

  /**
   * Import an array of FFI_ArrowSchema structs (each wrapping a Schema with one field) as a list of
   * Fields.
   */
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

  /**
   * Import a Field from an FFI_ArrowSchema memory segment. Rust wraps each field in a Schema
   * (format "+s") so we import as Schema and extract the first field.
   *
   * <p>The source segment is Rust-allocated. We clear the release callback (offset 56) in the
   * source so Rust's Drop is a no-op. Data.importSchema calls release on our Java copy to free the
   * data. Note: for Rust-allocated sources, we clear the release callback (offset 56), not
   * private_data (offset 64), because Rust's Drop calls release which dereferences private_data.
   */
  private Field importFieldFromSegment(MemorySegment schemaSegment) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      MemorySegment dest =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      dest.copyFrom(schemaSegment);
      // Clear release callback (offset 56) in Rust source so Rust's Drop is a no-op.
      schemaSegment.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL);
      Schema schema = Data.importSchema(allocator, ffiSchema, null);
      return schema.getFields().get(0);
    }
  }

  /**
   * Export a Field as FFI_ArrowSchema to a given output address. Rust expects each field wrapped in
   * a Schema (format "+s"), so we wrap before exporting.
   *
   * <p>The destination is Rust-allocated. We clear private_data (offset 64) in the Java source
   * after copying. ArrowSchema.close() does not call the release callback, so this is safe. Rust's
   * copy has the original release + private_data and will call release on Drop to free the data.
   */
  private void exportFieldToAddress(Field field, long outAddr) {
    Schema wrapperSchema = new Schema(List.of(field));
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, wrapperSchema, null, ffiSchema);
      MemorySegment src =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      MemorySegment dest = MemorySegment.ofAddress(outAddr).reinterpret(ARROW_SCHEMA_SIZE);
      dest.copyFrom(src);
      // Clear private_data in Java source after copying
      src.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }
  }
}
