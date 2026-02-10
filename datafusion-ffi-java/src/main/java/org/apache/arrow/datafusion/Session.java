package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.ffi.SessionFfi;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A borrowed handle to the DataFusion session available during a {@link TableProvider#scan}
 * callback.
 *
 * <p>This wraps a pointer to a boxed Rust {@code *const dyn Session} fat pointer. It is NOT {@link
 * AutoCloseable} because it does not own the underlying memory. It is only valid during the scan
 * callback.
 */
public class Session {
  private final SessionFfi ffi;
  private final BufferAllocator allocator;

  /** Internal constructor. Users should not create Session instances directly. */
  public Session(SessionFfi ffi, BufferAllocator allocator) {
    this.ffi = ffi;
    this.allocator = allocator;
  }

  /**
   * Creates a physical expression from the given filter expressions using the session state.
   *
   * <p>This conjoins all filter expressions with AND and compiles them into a physical expression
   * using the session's state and the provided table schema. The returned {@link PhysicalExpr} is
   * owned and must be closed when no longer needed.
   *
   * @param tableSchema the schema of the table being scanned
   * @param filters the filter expressions to compile (borrowed, from the scan callback)
   * @return a physical expression representing the conjunction of all filters
   * @throws DataFusionException if the physical expression cannot be created
   */
  public PhysicalExpr createPhysicalExpr(Schema tableSchema, Expr[] filters) {
    if (filters.length == 0) {
      throw new DataFusionException("No filters to create physical expression from");
    }

    return new PhysicalExpr(ffi.createPhysicalExpr(allocator, tableSchema, filters));
  }
}
