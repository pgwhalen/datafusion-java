package org.apache.arrow.datafusion;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A DataFrame representing the result of a DataFusion query.
 *
 * <p>This class wraps a native DataFusion DataFrame and provides methods for query manipulation and
 * execution. All transformation methods return a new DataFrame without executing (lazy evaluation).
 * Only terminal operations ({@link #executeStream}, {@link #collect}, {@link #show}, {@link
 * #count}) trigger execution.
 *
 * <p><b>Ownership semantics:</b> Transformation methods (filter, select, sort, etc.) consume the
 * source DataFrame, mirroring Rust DataFusion's ownership model where these methods take {@code
 * self}. After calling a transformation, the source DataFrame is closed and must not be reused.
 * Terminal operations do NOT consume the source. This enables fluent method chaining without
 * leaking native resources:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * DataFrame result = ctx.sql("SELECT * FROM employees")
 *     .filter(col("age").gt(lit(30)))
 *     .select(col("name"), col("department"), col("salary"))
 *     .aggregate(
 *         List.of(col("department")),
 *         List.of(avg(col("salary")).alias("avg_salary")))
 *     .sort(col("avg_salary").sortDesc())
 *     .limit(0, 10);
 * }</pre>
 */
public class DataFrame implements AutoCloseable {
  private final DataFrameBridge bridge;

  DataFrame(DataFrameBridge bridge) {
    this.bridge = bridge;
  }

  // ── Projection ──

  /**
   * Select expressions. Equivalent to Rust's {@code df.select(vec![...])}.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame select(Expr... exprs) {
    return select(List.of(exprs));
  }

  /**
   * Select expressions from a list.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame select(List<Expr> exprs) {
    DataFrameBridge result = bridge.select(exprs);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Select columns by name. Equivalent to Rust's {@code df.select_columns(&[...])}.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame selectColumns(String... columns) {
    List<Expr> exprs = Arrays.stream(columns).map(Functions::col).collect(Collectors.toList());
    return select(exprs);
  }

  // ── Filtering ──

  /**
   * Filter rows matching a predicate. Equivalent to Rust's {@code df.filter(expr)}.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame filter(Expr predicate) {
    DataFrameBridge result = bridge.filter(predicate);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Sorting ──

  /**
   * Sort by expressions with explicit sort parameters.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame sort(SortExpr... sortExprs) {
    return sort(List.of(sortExprs));
  }

  /**
   * Sort by expressions with explicit sort parameters.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame sort(List<SortExpr> sortExprs) {
    DataFrameBridge result = bridge.sort(sortExprs);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Limiting ──

  /**
   * Limit the number of rows returned.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param skip number of rows to skip
   * @param fetch maximum number of rows to return, or null for no limit
   */
  public DataFrame limit(int skip, Integer fetch) {
    long fetchValue = (fetch == null) ? -1L : fetch.longValue();
    DataFrameBridge result = bridge.limit(skip, fetchValue);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Aggregation ──

  /**
   * Aggregate with grouping.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param groupExprs GROUP BY expressions (empty list for global aggregation)
   * @param aggrExprs aggregate expressions (e.g., {@code avg(col("salary"))})
   */
  public DataFrame aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    DataFrameBridge result = bridge.aggregate(groupExprs, aggrExprs);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Joins ──

  /**
   * Join with another DataFrame on column names.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call. The right DataFrame is
   * NOT consumed.
   *
   * @param right the right DataFrame
   * @param joinType the join type
   * @param leftCols left join column names
   * @param rightCols right join column names
   */
  public DataFrame join(
      DataFrame right, JoinType joinType, List<String> leftCols, List<String> rightCols) {
    DataFrameBridge result = bridge.join(right.bridge, joinType, leftCols, rightCols, null);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Join with another DataFrame on column names with an additional filter.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call. The right DataFrame is
   * NOT consumed.
   *
   * @param right the right DataFrame
   * @param joinType the join type
   * @param leftCols left join column names
   * @param rightCols right join column names
   * @param filter additional join filter expression
   */
  public DataFrame join(
      DataFrame right,
      JoinType joinType,
      List<String> leftCols,
      List<String> rightCols,
      Expr filter) {
    DataFrameBridge result = bridge.join(right.bridge, joinType, leftCols, rightCols, filter);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Join with arbitrary expressions.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call. The right DataFrame is
   * NOT consumed.
   *
   * @param right the right DataFrame
   * @param joinType the join type
   * @param onExprs join condition expressions
   */
  public DataFrame joinOn(DataFrame right, JoinType joinType, List<Expr> onExprs) {
    DataFrameBridge result = bridge.joinOn(right.bridge, joinType, onExprs);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Set operations ──

  /**
   * Union of this DataFrame and another.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame union(DataFrame other) {
    DataFrameBridge result = bridge.union(other.bridge);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Union distinct of this DataFrame and another.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame unionDistinct(DataFrame other) {
    DataFrameBridge result = bridge.unionDistinct(other.bridge);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Intersect of this DataFrame and another.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame intersect(DataFrame other) {
    DataFrameBridge result = bridge.intersect(other.bridge);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Set difference (EXCEPT) of this DataFrame and another.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame except(DataFrame other) {
    DataFrameBridge result = bridge.except(other.bridge);
    bridge.close();
    return new DataFrame(result);
  }

  // ── Distinct ──

  /**
   * Return distinct rows.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   */
  public DataFrame distinct() {
    DataFrameBridge result = bridge.distinct();
    bridge.close();
    return new DataFrame(result);
  }

  // ── Column manipulation ──

  /**
   * Add or replace a column. Equivalent to Rust's {@code df.with_column(name, expr)}.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param name column name
   * @param expr expression to compute the column value
   */
  public DataFrame withColumn(String name, Expr expr) {
    DataFrameBridge result = bridge.withColumn(name, expr);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Rename a column.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param oldName current column name
   * @param newName new column name
   */
  public DataFrame withColumnRenamed(String oldName, String newName) {
    DataFrameBridge result = bridge.withColumnRenamed(oldName, newName);
    bridge.close();
    return new DataFrame(result);
  }

  /**
   * Drop columns by name.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param columns column names to drop
   */
  public DataFrame dropColumns(String... columns) {
    DataFrameBridge result = bridge.dropColumns(List.of(columns));
    bridge.close();
    return new DataFrame(result);
  }

  // ── Write operations ──

  /**
   * Write results to a Parquet file.
   *
   * @param path path to write the Parquet file to
   * @throws DataFusionException if writing fails
   */
  public void writeParquet(String path) {
    bridge.writeParquet(path);
  }

  /**
   * Write results to a Parquet file with write options.
   *
   * @param path path to write the Parquet file to
   * @param options write options controlling output behavior
   * @throws DataFusionException if writing fails
   */
  public void writeParquet(String path, DataFrameWriteOptions options) {
    bridge.writeParquet(path, options, null);
  }

  /**
   * Write results to a Parquet file with write and format options.
   *
   * @param path path to write the Parquet file to
   * @param options write options controlling output behavior
   * @param parquetOptions Parquet-specific format options
   * @throws DataFusionException if writing fails
   */
  public void writeParquet(
      String path, DataFrameWriteOptions options, ParquetWriteOptions parquetOptions) {
    bridge.writeParquet(path, options, parquetOptions);
  }

  /**
   * Write results to a CSV file.
   *
   * @param path path to write the CSV file to
   * @throws DataFusionException if writing fails
   */
  public void writeCsv(String path) {
    bridge.writeCsv(path);
  }

  /**
   * Write results to a CSV file with write options.
   *
   * @param path path to write the CSV file to
   * @param options write options controlling output behavior
   * @throws DataFusionException if writing fails
   */
  public void writeCsv(String path, DataFrameWriteOptions options) {
    bridge.writeCsv(path, options, null);
  }

  /**
   * Write results to a CSV file with write and format options.
   *
   * @param path path to write the CSV file to
   * @param options write options controlling output behavior
   * @param csvOptions CSV-specific format options
   * @throws DataFusionException if writing fails
   */
  public void writeCsv(String path, DataFrameWriteOptions options, CsvWriteOptions csvOptions) {
    bridge.writeCsv(path, options, csvOptions);
  }

  /**
   * Write results to a JSON file.
   *
   * @param path path to write the JSON file to
   * @throws DataFusionException if writing fails
   */
  public void writeJson(String path) {
    bridge.writeJson(path);
  }

  /**
   * Write results to a JSON file with write options.
   *
   * @param path path to write the JSON file to
   * @param options write options controlling output behavior
   * @throws DataFusionException if writing fails
   */
  public void writeJson(String path, DataFrameWriteOptions options) {
    bridge.writeJson(path, options, null);
  }

  /**
   * Write results to a JSON file with write and format options.
   *
   * @param path path to write the JSON file to
   * @param options write options controlling output behavior
   * @param jsonOptions JSON-specific format options
   * @throws DataFusionException if writing fails
   */
  public void writeJson(String path, DataFrameWriteOptions options, JsonWriteOptions jsonOptions) {
    bridge.writeJson(path, options, jsonOptions);
  }

  // ── Terminal operations ──

  /**
   * Executes the DataFrame and returns a stream of record batches.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A RecordBatchStream for iterating over results
   * @throws DataFusionException if execution fails
   */
  public RecordBatchStream executeStream(BufferAllocator allocator) {
    return bridge.executeStream(allocator);
  }

  /**
   * Execute the query, buffer all results, and return a RecordBatchStream.
   *
   * <p>Unlike {@link #executeStream}, all computation is complete when this method returns. The
   * returned stream lets the caller iterate batches as usual, but the data is already fully
   * materialized in memory.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A RecordBatchStream with all results materialized
   * @throws DataFusionException if execution fails
   */
  public RecordBatchStream collect(BufferAllocator allocator) {
    return bridge.collect(allocator);
  }

  /**
   * Execute and print results to stdout.
   *
   * @throws DataFusionException if execution fails
   */
  public void show() {
    bridge.show();
  }

  /**
   * Execute and return the row count.
   *
   * @return the number of rows
   * @throws DataFusionException if execution fails
   */
  public long count() {
    return bridge.count();
  }

  /**
   * Get the schema of this DataFrame.
   *
   * @return the Arrow schema
   * @throws DataFusionException if the schema cannot be retrieved
   */
  public Schema schema() {
    return bridge.schema();
  }

  @Override
  public void close() {
    bridge.close();
  }
}
