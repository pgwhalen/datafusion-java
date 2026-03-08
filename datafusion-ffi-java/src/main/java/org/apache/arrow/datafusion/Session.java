package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A borrowed handle to the DataFusion session available during a {@link TableProvider#scan}
 * callback.
 *
 * <p>This provides access to session-level functionality like creating physical expressions from
 * filters. It is NOT {@link AutoCloseable} because it does not own the underlying memory. It is
 * only valid during the scan callback.
 */
public class Session {
  private final BufferAllocator allocator;

  /** Internal constructor. Users should not create Session instances directly. */
  Session(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Creates a physical expression from the given filter expressions using a default session state.
   *
   * <p>This conjoins all filter expressions with AND and compiles them into a physical expression
   * using the provided table schema. The returned {@link PhysicalExpr} is owned and must be closed
   * when no longer needed.
   *
   * @param tableSchema the schema of the table being scanned
   * @param filters the filter expressions to compile (borrowed, from the scan callback)
   * @return a physical expression representing the conjunction of all filters
   * @throws DataFusionException if the physical expression cannot be created
   */
  public PhysicalExpr createPhysicalExpr(Schema tableSchema, List<Expr> filters) {
    if (filters.isEmpty()) {
      throw new DataFusionException("No filters to create physical expression from");
    }

    PhysicalExprBridge bridge =
        PhysicalExprBridge.fromProtoFilters(allocator, tableSchema, filters);
    return new PhysicalExpr(bridge);
  }
}
