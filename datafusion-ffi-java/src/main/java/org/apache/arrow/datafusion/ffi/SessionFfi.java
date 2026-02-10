package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.Expr;
import org.apache.arrow.datafusion.PhysicalExpr;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
public final class SessionFfi {
  private static final long ARROW_SCHEMA_SIZE = 72;

  private final MemorySegment pointer;

  public SessionFfi(MemorySegment pointer) {
    this.pointer = pointer;
  }

  /**
   * Creates a physical expression from the given filter expressions using the session state.
   *
   * @param allocator the buffer allocator for Arrow schema export
   * @param tableSchema the schema of the table being scanned
   * @param filters the filter expressions to compile (borrowed, from the scan callback)
   * @return a new PhysicalExpr wrapping the created native physical expression
   * @throws DataFusionException if the physical expression cannot be created
   */
  public PhysicalExprFfi createPhysicalExpr(
      BufferAllocator allocator, Schema tableSchema, Expr[] filters) {
    try (Arena arena = Arena.ofConfined()) {
      // Build the filter pointer array
      MemorySegment filterPtrArray = arena.allocate(ValueLayout.ADDRESS, (long) filters.length);
      for (int i = 0; i < filters.length; i++) {
        filterPtrArray.setAtIndex(ValueLayout.ADDRESS, i, filters[i].ffi().nativeHandle());
      }

      // Allocate the FFI schema struct in the arena.
      MemorySegment ffiSchemaSegment = arena.allocate(ARROW_SCHEMA_SIZE);

      // Use a separate RootAllocator for the schema export. Data.exportSchema allocates
      // internal buffers that are freed by the ArrowSchema release callback, but
      // ArrowSchema.close() does not always release every byte. Using a separate allocator
      // prevents leak accounting from affecting the caller's allocator.
      BufferAllocator schemaAllocator = new RootAllocator();
      ArrowSchema ffiSchema = ArrowSchema.wrap(ffiSchemaSegment.address());
      Data.exportSchema(schemaAllocator, tableSchema, null, ffiSchema);

      MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchemaSegment.address());

      try {
        MemorySegment result =
            NativeUtil.callForPointer(
                arena,
                "Create physical expression",
                errorOut ->
                    (MemorySegment)
                        DataFusionBindings.SESSION_CREATE_PHYSICAL_EXPR.invokeExact(
                            pointer, filterPtrArray, (long) filters.length, schemaAddr, errorOut));

        return new PhysicalExprFfi(result);
      } finally {
        // Release the exported schema data.
        ffiSchema.close();
        try {
          schemaAllocator.close();
        } catch (IllegalStateException e) {
          // Suppress allocator leak errors from Arrow's schema export.
        }
      }
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create physical expression", e);
    }
  }
}
