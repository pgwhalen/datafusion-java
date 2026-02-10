package org.apache.arrow.datafusion;

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
 *     public ExecutionPlan scan(Session session, Expr[] filters, int[] projection, Long limit) {
 *         return new MyExecutionPlan(schema, data, projection, limit);
 *     }
 * }
 * }</pre>
 */
public interface TableProvider {
  /**
   * Returns the schema of this table.
   *
   * @return The Arrow schema describing the table's columns
   */
  Schema schema();

  /**
   * Returns the type of this table.
   *
   * <p>Default implementation returns {@link TableType#BASE}.
   *
   * @return The table type
   */
  default TableType tableType() {
    return TableType.BASE;
  }

  /**
   * Creates an execution plan to scan this table.
   *
   * <p>The session parameter provides access to the DataFusion session state, which can be used to
   * create physical expressions from the provided filters via {@link
   * Session#createPhysicalExpr(Schema, Expr[])}.
   *
   * <p>The filters parameter contains the logical filter expressions that DataFusion wants to push
   * down to this table provider. These can be analyzed for literal guarantees using {@link
   * LiteralGuarantee#analyze(PhysicalExpr)}.
   *
   * <p>The projection parameter specifies which columns should be read. If null, all columns should
   * be included. The indices correspond to the column positions in the schema.
   *
   * <p>The limit parameter specifies the maximum number of rows to return. If null, all rows should
   * be returned.
   *
   * @param session The session context for this scan (borrowed, valid only during this call)
   * @param filters Filter expressions for potential pushdown (borrowed, valid only during this
   *     call)
   * @param projection Column indices to include, or null for all columns
   * @param limit Maximum number of rows, or null for no limit
   * @return An execution plan that produces the requested data
   * @throws DataFusionException if creating the scan fails
   */
  ExecutionPlan scan(Session session, Expr[] filters, int[] projection, Long limit);
}
