package org.apache.arrow.datafusion.dataframe;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.datafusion.CsvOptions;
import org.apache.arrow.datafusion.Functions;
import org.apache.arrow.datafusion.JsonOptions;
import org.apache.arrow.datafusion.ParquetOptions;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.JoinType;
import org.apache.arrow.datafusion.logical_expr.SortExpr;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
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
 * leaking native resources.
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
 *
 * @see <a href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html">Rust
 *     DataFusion: DataFrame</a>
 */
public class DataFrame implements AutoCloseable {
  private final DataFrameBridge bridge;

  /**
   * Creates a new DataFrame backed by the given bridge.
   *
   * @param bridge the native bridge
   */
  public DataFrame(DataFrameBridge bridge) {
    this.bridge = bridge;
  }

  // ── Projection ──

  /**
   * Select expressions. Equivalent to Rust's {@code df.select(vec![...])}.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param exprs the expressions to select
   * @return a new DataFrame with the selected columns
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.select">Rust
   *     DataFusion: DataFrame::select</a>
   */
  public DataFrame select(Expr... exprs) {
    return select(List.of(exprs));
  }

  /**
   * Select expressions from a list.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param exprs the expressions to select
   * @return a new DataFrame with the selected columns
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.select">Rust
   *     DataFusion: DataFrame::select</a>
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
   *
   * @param columns the column names to select
   * @return a new DataFrame with the selected columns
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.select_columns">Rust
   *     DataFusion: DataFrame::select_columns</a>
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
   *
   * @param predicate the filter expression
   * @return a new DataFrame with matching rows
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.filter">Rust
   *     DataFusion: DataFrame::filter</a>
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
   *
   * @param sortExprs the sort expressions
   * @return a new sorted DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.sort">Rust
   *     DataFusion: DataFrame::sort</a>
   */
  public DataFrame sort(SortExpr... sortExprs) {
    return sort(List.of(sortExprs));
  }

  /**
   * Sort by expressions with explicit sort parameters.
   *
   * <p>Consumes this DataFrame. The source must not be used after this call.
   *
   * @param sortExprs the sort expressions
   * @return a new sorted DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.sort">Rust
   *     DataFusion: DataFrame::sort</a>
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
   * @return a new DataFrame with the limit applied
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.limit">Rust
   *     DataFusion: DataFrame::limit</a>
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
   * @return a new aggregated DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.aggregate">Rust
   *     DataFusion: DataFrame::aggregate</a>
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
   * @return a new joined DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.join">Rust
   *     DataFusion: DataFrame::join</a>
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
   * @return a new joined DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.join">Rust
   *     DataFusion: DataFrame::join</a>
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
   * @return a new joined DataFrame
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.join_on">Rust
   *     DataFusion: DataFrame::join_on</a>
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
   *
   * @param other the other DataFrame
   * @return a new DataFrame containing all rows from both
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.union">Rust
   *     DataFusion: DataFrame::union</a>
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
   *
   * @param other the other DataFrame
   * @return a new DataFrame containing distinct rows from both
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.union_distinct">Rust
   *     DataFusion: DataFrame::union_distinct</a>
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
   *
   * @param other the other DataFrame
   * @return a new DataFrame containing rows present in both
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.intersect">Rust
   *     DataFusion: DataFrame::intersect</a>
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
   *
   * @param other the other DataFrame
   * @return a new DataFrame containing rows in this but not in other
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.except">Rust
   *     DataFusion: DataFrame::except</a>
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
   *
   * @return a new DataFrame with duplicate rows removed
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.distinct">Rust
   *     DataFusion: DataFrame::distinct</a>
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
   * @return a new DataFrame with the added or replaced column
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.with_column">Rust
   *     DataFusion: DataFrame::with_column</a>
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
   * @return a new DataFrame with the renamed column
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.with_column_renamed">Rust
   *     DataFusion: DataFrame::with_column_renamed</a>
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
   * @return a new DataFrame without the specified columns
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.drop_columns">Rust
   *     DataFusion: DataFrame::drop_columns</a>
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
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_parquet">Rust
   *     DataFusion: DataFrame::write_parquet</a>
   */
  public void writeParquet(String path) {
    bridge.writeParquet(path);
  }

  /**
   * Write results to a Parquet file with write options.
   *
   * @param path path to write the Parquet file to
   * @param options write options controlling output behavior
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_parquet">Rust
   *     DataFusion: DataFrame::write_parquet</a>
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
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_parquet">Rust
   *     DataFusion: DataFrame::write_parquet</a>
   */
  public void writeParquet(
      String path, DataFrameWriteOptions options, ParquetOptions parquetOptions) {
    bridge.writeParquet(path, options, parquetOptions);
  }

  /**
   * Write results to a CSV file.
   *
   * @param path path to write the CSV file to
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_csv">Rust
   *     DataFusion: DataFrame::write_csv</a>
   */
  public void writeCsv(String path) {
    bridge.writeCsv(path);
  }

  /**
   * Write results to a CSV file with write options.
   *
   * @param path path to write the CSV file to
   * @param options write options controlling output behavior
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_csv">Rust
   *     DataFusion: DataFrame::write_csv</a>
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
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_csv">Rust
   *     DataFusion: DataFrame::write_csv</a>
   */
  public void writeCsv(String path, DataFrameWriteOptions options, CsvOptions csvOptions) {
    bridge.writeCsv(path, options, csvOptions);
  }

  /**
   * Write results to a JSON file.
   *
   * @param path path to write the JSON file to
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_json">Rust
   *     DataFusion: DataFrame::write_json</a>
   */
  public void writeJson(String path) {
    bridge.writeJson(path);
  }

  /**
   * Write results to a JSON file with write options.
   *
   * @param path path to write the JSON file to
   * @param options write options controlling output behavior
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_json">Rust
   *     DataFusion: DataFrame::write_json</a>
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
   * @throws DataFusionError if writing fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.write_json">Rust
   *     DataFusion: DataFrame::write_json</a>
   */
  public void writeJson(String path, DataFrameWriteOptions options, JsonOptions jsonOptions) {
    bridge.writeJson(path, options, jsonOptions);
  }

  // ── Terminal operations ──

  /**
   * Executes the DataFrame and returns a stream of record batches.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A SendableRecordBatchStream for iterating over results
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.execute_stream">Rust
   *     DataFusion: DataFrame::execute_stream</a>
   */
  public SendableRecordBatchStream executeStream(BufferAllocator allocator) {
    return bridge.executeStream(allocator);
  }

  /**
   * Execute the query, buffer all results, and return a SendableRecordBatchStream.
   *
   * <p>Unlike {@link #executeStream}, all computation is complete when this method returns. The
   * returned stream lets the caller iterate batches as usual, but the data is already fully
   * materialized in memory.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A SendableRecordBatchStream with all results materialized
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.collect">Rust
   *     DataFusion: DataFrame::collect</a>
   */
  public SendableRecordBatchStream collect(BufferAllocator allocator) {
    return bridge.collect(allocator);
  }

  /**
   * Execute and print results to stdout.
   *
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.show">Rust
   *     DataFusion: DataFrame::show</a>
   */
  public void show() {
    bridge.show();
  }

  /**
   * Execute and return the row count.
   *
   * @return the number of rows
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.count">Rust
   *     DataFusion: DataFrame::count</a>
   */
  public long count() {
    return bridge.count();
  }

  /**
   * Get the schema of this DataFrame.
   *
   * @return the Arrow schema
   * @throws DataFusionError if the schema cannot be retrieved
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.schema">Rust
   *     DataFusion: DataFrame::schema</a>
   */
  public Schema schema() {
    return bridge.schema();
  }

  @Override
  public void close() {
    bridge.close();
  }
}
