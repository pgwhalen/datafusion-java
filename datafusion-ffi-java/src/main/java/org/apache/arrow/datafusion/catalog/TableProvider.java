package org.apache.arrow.datafusion.catalog;

import java.util.Collections;
import java.util.List;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.logical_expr.TableType;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A table provider that can be registered with a DataFusion session.
 *
 * <p>This interface allows Java code to implement custom table providers that DataFusion can query.
 * When a SQL query references a table backed by this provider, DataFusion calls {@link
 * #scanWithArgs} to get an execution plan for reading the data.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * public class MyTableProvider implements TableProvider {
 *     private final Schema schema;
 *     private final List<VectorSchemaRoot> data;
 *
 *     @Override
 *     public Schema schema() {
 *         return schema;
 *     }
 *
 *     @Override
 *     public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
 *         return new MyExecutionPlan(schema, data, args.projection(), args.limit());
 *     }
 * }
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html">Rust
 *     DataFusion: TableProvider</a>
 */
public interface TableProvider {
  /**
   * Returns the schema of this table.
   *
   * @return The Arrow schema describing the table's columns
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.schema">Rust
   *     DataFusion: TableProvider::schema</a>
   */
  Schema schema();

  /**
   * Returns the type of this table.
   *
   * <p>Default implementation returns {@link TableType#BASE}.
   *
   * @return The table type
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.table_type">Rust
   *     DataFusion: TableProvider::table_type</a>
   */
  default TableType tableType() {
    return TableType.BASE;
  }

  /**
   * Creates an execution plan to scan this table.
   *
   * <p>The session parameter provides access to the DataFusion session state, which can be used to
   * create physical expressions from the provided filters via {@link
   * Session#createPhysicalExpr(Schema, List)}.
   *
   * <p>See {@link ScanArgs} for detailed documentation on the filter, projection, and limit
   * parameters.
   *
   * @param session The session context for this scan (borrowed, valid only during this call)
   * @param args The scan arguments containing filters, projection, and limit
   * @return An execution plan that produces the requested data
   * @throws DataFusionError if creating the scan fails
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.scan_with_args">Rust
   *     DataFusion: TableProvider::scan_with_args</a>
   */
  ExecutionPlan scanWithArgs(Session session, ScanArgs args);

  /**
   * Returns the filter pushdown support for each filter expression.
   *
   * <p>DataFusion calls this method to determine which filters the provider can handle natively.
   * The returned list must have the same size as the input filters list, with one {@link
   * TableProviderFilterPushDown} value per filter.
   *
   * <p>The default implementation returns {@link TableProviderFilterPushDown#INEXACT} for all
   * filters, meaning all filters are passed to {@link #scanWithArgs} but DataFusion will also
   * re-apply them after scan to ensure correctness.
   *
   * @param filters the filter expressions to evaluate (borrowed, valid only during this call)
   * @return a list of pushdown support values, one per filter
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.supports_filters_pushdown">Rust
   *     DataFusion: TableProvider::supports_filters_pushdown</a>
   */
  default List<TableProviderFilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
    return Collections.nCopies(filters.size(), TableProviderFilterPushDown.INEXACT);
  }
}
