package org.apache.arrow.datafusion.catalog;

import java.util.Collections;
import java.util.List;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.ColumnAssignment;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.InsertOp;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.logical_expr.TableType;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
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

  /**
   * Inserts data from the given input into this table.
   *
   * <p>The input is a {@link RecordBatchReader} that provides the data to insert. The reader is
   * lazily evaluated — each call to {@link RecordBatchReader#loadNextBatch()} fetches exactly one
   * batch from the DataFusion execution plan. The caller is responsible for closing the reader.
   *
   * <p>Returns an {@link ExecutionPlan} that produces a single row with a {@code count} column
   * (UInt64) indicating the number of rows inserted.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param session the session context (borrowed, valid only during this call)
   * @param input the data to insert (borrowed, valid only during this call)
   * @param insertOp the insert operation type (append, overwrite, or replace)
   * @return an execution plan producing the insert count
   * @throws UnsupportedOperationException if this table does not support inserts
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.insert_into">Rust
   *     DataFusion: TableProvider::insert_into</a>
   */
  default ExecutionPlan insertInto(Session session, RecordBatchReader input, InsertOp insertOp) {
    throw new UnsupportedOperationException("insert_into not implemented");
  }

  /**
   * Deletes rows matching the filter predicates from this table.
   *
   * <p>Returns an {@link ExecutionPlan} that produces a single row with a {@code count} column
   * (UInt64) indicating the number of rows deleted. An empty filter list deletes all rows.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param session the session context (borrowed, valid only during this call)
   * @param filters the filter predicates for rows to delete (empty means delete all)
   * @return an execution plan producing the delete count
   * @throws UnsupportedOperationException if this table does not support deletes
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.delete_from">Rust
   *     DataFusion: TableProvider::delete_from</a>
   */
  default ExecutionPlan deleteFrom(Session session, List<Expr> filters) {
    throw new UnsupportedOperationException("delete_from not implemented");
  }

  /**
   * Updates rows matching the filter predicates in this table.
   *
   * <p>Returns an {@link ExecutionPlan} that produces a single row with a {@code count} column
   * (UInt64) indicating the number of rows updated. An empty filter list updates all rows.
   *
   * <p>The default implementation throws {@link UnsupportedOperationException}.
   *
   * @param session the session context (borrowed, valid only during this call)
   * @param assignments the column assignments to apply (column name + new value expression)
   * @param filters the filter predicates for rows to update (empty means update all)
   * @return an execution plan producing the update count
   * @throws UnsupportedOperationException if this table does not support updates
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog/trait.TableProvider.html#method.update">Rust
   *     DataFusion: TableProvider::update</a>
   */
  default ExecutionPlan update(
      Session session, List<ColumnAssignment> assignments, List<Expr> filters) {
    throw new UnsupportedOperationException("update not implemented");
  }
}
