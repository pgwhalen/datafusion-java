package org.apache.arrow.datafusion.catalog;

import java.util.Collections;
import java.util.List;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.logical_expr.TableType;
import org.apache.arrow.datafusion.physical_expr.LiteralGuarantee;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.PhysicalExpr;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A table provider that can be registered with a DataFusion session.
 *
 * <p>This interface allows Java code to implement custom table providers that DataFusion can query.
 * When a SQL query references a table backed by this provider, DataFusion calls {@link #scan} to
 * get an execution plan for reading the data.
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
 *     public ExecutionPlan scan(Session session, List<Expr> filters, List<Integer> projection, Long limit) {
 *         return new MyExecutionPlan(schema, data, projection, limit);
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
   * <p>The filters parameter contains the logical filter expressions that DataFusion wants to push
   * down to this table provider. These can be analyzed for literal guarantees using {@link
   * LiteralGuarantee#analyze(PhysicalExpr)}.
   *
   * <p>The projection parameter specifies which columns should be read. {@code null} means no
   * projection constraint (all columns should be included). An empty list means DataFusion
   * explicitly requests zero columns (e.g. for {@code count(*)}); the returned execution plan must
   * have an empty schema in that case. A non-empty list contains the column indices to include. The
   * indices correspond to the column positions in the schema.
   *
   * <p>The limit parameter specifies the maximum number of rows to return. If null, all rows should
   * be returned.
   *
   * @param session The session context for this scan (borrowed, valid only during this call)
   * @param filters Filter expressions for potential pushdown (borrowed, valid only during this
   *     call)
   * @param projection null for all columns, empty list for zero columns, or specific column indices
   * @param limit Maximum number of rows, or null for no limit
   * @return An execution plan that produces the requested data
   * @throws DataFusionError if creating the scan fails
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.scan">Rust
   *     DataFusion: TableProvider::scan</a>
   */
  ExecutionPlan scan(Session session, List<Expr> filters, List<Integer> projection, Long limit);

  /**
   * Returns the filter pushdown support for each filter expression.
   *
   * <p>DataFusion calls this method to determine which filters the provider can handle natively.
   * The returned list must have the same size as the input filters list, with one {@link
   * TableProviderFilterPushDown} value per filter.
   *
   * <p>The default implementation returns {@link TableProviderFilterPushDown#INEXACT} for all
   * filters, meaning all filters are passed to {@link #scan} but DataFusion will also re-apply them
   * after scan to ensure correctness.
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
