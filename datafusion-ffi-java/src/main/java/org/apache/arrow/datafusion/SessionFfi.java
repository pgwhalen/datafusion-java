package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI helper for Session.
 *
 * <p>This class holds the native pointer for a borrowed session and contains all native call logic.
 * It exists in the ffi package so that the public {@link org.apache.arrow.datafusion.Session} class
 * does not need to import any {@code java.lang.foreign} types.
 *
 * <p>This is a borrowed pointer that does not own the underlying memory and has no lifecycle
 * management.
 */
final class SessionFfi {
  private static final MethodHandle SESSION_CREATE_PHYSICAL_EXPR_FROM_PROTO =
      NativeUtil.downcall(
          "datafusion_session_create_physical_expr_from_proto",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("filter_bytes"),
              ValueLayout.JAVA_LONG.withName("filter_len"),
              ValueLayout.ADDRESS.withName("schema_ffi"),
              ValueLayout.ADDRESS.withName("error_out")));

  private final MemorySegment pointer;

  SessionFfi(MemorySegment pointer) {
    this.pointer = pointer;
  }

  /**
   * Creates a physical expression from the given filter expressions using the session state.
   *
   * <p>Serializes the filter expressions to protobuf bytes and passes them to Rust, which
   * deserializes, conjoins with AND, and compiles into a physical expression.
   *
   * @param allocator the buffer allocator for Arrow schema export
   * @param tableSchema the schema of the table being scanned
   * @param filters the filter expressions to compile
   * @return a new PhysicalExpr wrapping the created native physical expression
   * @throws DataFusionException if the physical expression cannot be created
   */
  PhysicalExprFfi createPhysicalExpr(
      BufferAllocator allocator, Schema tableSchema, List<Expr> filters) {
    try (Arena arena = Arena.ofConfined()) {
      // Serialize filter expressions to proto bytes
      byte[] filterBytes = ExprProtoConverter.toProtoBytes(filters);
      MemorySegment filterSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterBytes);

      // Export schema via Arrow C Data Interface
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, tableSchema, null, ffiSchema);
        MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());

        MemorySegment result =
            NativeUtil.callForPointer(
                arena,
                "Create physical expression",
                errorOut ->
                    (MemorySegment)
                        SESSION_CREATE_PHYSICAL_EXPR_FROM_PROTO.invokeExact(
                            filterSegment, (long) filterBytes.length, schemaAddr, errorOut));

        return new PhysicalExprFfi(result);
      }
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create physical expression", e);
    }
  }
}
