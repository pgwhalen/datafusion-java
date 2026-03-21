package org.apache.arrow.datafusion.catalog;

import java.util.List;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.physical_plan.PhysicalExpr;
import org.apache.arrow.datafusion.physical_plan.PhysicalExprBridge;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A borrowed handle to the DataFusion session available during a {@link TableProvider#scan}
 * callback.
 *
 * <p>This provides access to session-level functionality like creating physical expressions from
 * filters. It is NOT {@link AutoCloseable} because it does not own the underlying memory. It is
 * only valid during the scan callback.
 *
 * @see <a href="https://docs.rs/datafusion/52.1.0/datafusion/catalog/trait.Session.html">Rust
 *     DataFusion: Session</a>
 */
public class Session {
  private final BufferAllocator allocator;

  /** Internal constructor. Users should not create Session instances directly. */
  public Session(BufferAllocator allocator) {
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
   * @throws DataFusionError if the physical expression cannot be created
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/catalog/trait.Session.html#tymethod.create_physical_expr">Rust
   *     DataFusion: Session::create_physical_expr</a>
   */
  public PhysicalExpr createPhysicalExpr(Schema tableSchema, List<Expr> filters) {
    if (filters.isEmpty()) {
      throw new DataFusionError("No filters to create physical expression from");
    }

    PhysicalExprBridge bridge =
        PhysicalExprBridge.fromProtoFilters(allocator, tableSchema, filters);
    return new PhysicalExpr(bridge);
  }
}
