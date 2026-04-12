package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.execution.VarProvider;
import org.apache.arrow.datafusion.generated.DfExprBytes;
import org.apache.arrow.datafusion.generated.DfVarProviderTrait;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a user-implemented {@link VarProvider} to the Diplomat-generated {@link
 * DfVarProviderTrait} interface for FFI callbacks.
 */
public final class DfVarProviderAdapter implements DfVarProviderTrait {
  private static final long ARROW_SCHEMA_SIZE = 72;

  private final VarProvider provider;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  public DfVarProviderAdapter(
      VarProvider provider, BufferAllocator allocator, boolean fullStackTrace) {
    this.provider = provider;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long getValue(long varNamesPtr, long errorAddr, long errorCap) {
    try {
      List<String> varNames = NativeUtil.fromRawStringArray(varNamesPtr);
      ScalarValue value = provider.getValue(varNames);
      byte[] protoBytes = ScalarValueProtoConverter.toProto(value).toByteArray();
      // Create a DfExprBytes and return its raw pointer (Rust takes ownership)
      DfExprBytes exprBytes = DfExprBytes.newFromSlice(protoBytes);
      return exprBytes.toRawPtr();
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }

  @Override
  public int getType(long varNamesPtr, long outSchemaAddr, long errorAddr, long errorCap) {
    try {
      List<String> varNames = NativeUtil.fromRawStringArray(varNamesPtr);
      Optional<ArrowType> type = provider.getType(varNames);
      if (type.isEmpty()) {
        return 0;
      }
      // Export the ArrowType as a single-field schema via FFI
      Field field = new Field("var", new FieldType(true, type.get(), null), null);
      Schema wrapperSchema = new Schema(List.of(field));
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, wrapperSchema, null, ffiSchema);
        MemorySegment src =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment dest = MemorySegment.ofAddress(outSchemaAddr).reinterpret(ARROW_SCHEMA_SIZE);
        dest.copyFrom(src);
        // Clear private_data in Java source after copying
        src.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      }
      return 1;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }
}
