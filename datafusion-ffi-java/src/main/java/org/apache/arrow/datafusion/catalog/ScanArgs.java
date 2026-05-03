package org.apache.arrow.datafusion.catalog;

import java.util.List;
import org.apache.arrow.datafusion.logical_expr.Expr;

/**
 * Arguments for a table scan operation.
 *
 * <p>Contains the filter expressions, column projection, and row limit that DataFusion passes to a
 * {@link TableProvider} when creating an execution plan.
 *
 * <p>Example:
 *
 * {@snippet :
 * @Override
 * public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
 *     List<Integer> proj = args.projection(); // columns to read, or null for all
 *     Long limit = args.limit(); // max rows, or null for no limit
 *     List<Expr> filters = args.filters(); // pushdown filters, or null
 *     return new MyExecutionPlan(schema(), proj, limit);
 * }
 * }
 *
 * @param filters filter expressions for potential pushdown, or {@code null} if no filter
 *     information is available. An empty list means filters were evaluated but none apply.
 * @param projection column indices to include in the scan results, or {@code null} for all columns.
 *     An empty list means zero columns are explicitly requested (e.g., for {@code count(*)}).
 * @param limit maximum number of rows to return, or {@code null} for no limit
 * @see <a
 *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/struct.ScanArgs.html">Rust
 *     DataFusion: ScanArgs</a>
 */
public record ScanArgs(List<Expr> filters, List<Integer> projection, Long limit) {

  /** Creates a default {@link ScanArgs} with all fields set to {@code null}. */
  public ScanArgs() {
    this(null, null, null);
  }

  /**
   * Returns the filter expressions for potential pushdown, or {@code null} if no filter information
   * is available.
   *
   * @return the filter expressions, or {@code null}
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/struct.ScanArgs.html#structfield.filters">Rust
   *     DataFusion: ScanArgs::filters</a>
   */
  @Override
  public List<Expr> filters() {
    return filters;
  }

  /**
   * Returns the column indices to include in the scan results, or {@code null} for all columns.
   *
   * @return the projection indices, or {@code null}
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/struct.ScanArgs.html#structfield.projection">Rust
   *     DataFusion: ScanArgs::projection</a>
   */
  @Override
  public List<Integer> projection() {
    return projection;
  }

  /**
   * Returns the maximum number of rows to return, or {@code null} for no limit.
   *
   * @return the row limit, or {@code null}
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/struct.ScanArgs.html#structfield.limit">Rust
   *     DataFusion: ScanArgs::limit</a>
   */
  @Override
  public Long limit() {
    return limit;
  }
}
