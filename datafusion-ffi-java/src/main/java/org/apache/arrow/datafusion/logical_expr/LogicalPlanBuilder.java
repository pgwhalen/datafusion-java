package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import org.apache.arrow.datafusion.execution.SessionContext;

/**
 * A builder for constructing {@link LogicalPlan}s programmatically.
 *
 * <p>Transformation methods consume this builder (the source is closed) and return a new
 * LogicalPlanBuilder, mirroring Rust's ownership model where methods take {@code self}. This
 * enables fluent chaining:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * LogicalPlan plan = LogicalPlanBuilder.from(existingPlan, ctx)
 *     .project(List.of(col("a"), col("b")))
 *     .filter(col("a").gt(lit(0)))
 *     .sort(List.of(new SortExpr(col("a"), true, false)))
 *     .limit(0, 10)
 *     .build();
 * DataFrame df = ctx.executeLogicalPlan(plan);
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html">Rust
 *     DataFusion: LogicalPlanBuilder</a>
 */
public class LogicalPlanBuilder implements AutoCloseable {

  private final LogicalPlanBuilderBridge bridge;

  public LogicalPlanBuilder(LogicalPlanBuilderBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Creates a builder for an empty relation.
   *
   * @param produceOneRow if true, the empty relation produces one row with no columns
   * @param ctx the session context
   * @return a new LogicalPlanBuilder
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.empty">Rust
   *     DataFusion: LogicalPlanBuilder::empty</a>
   */
  public static LogicalPlanBuilder empty(boolean produceOneRow, SessionContext ctx) {
    return new LogicalPlanBuilder(
        LogicalPlanBuilderBridge.createEmpty(ctx.bridge().dfContext(), produceOneRow));
  }

  /**
   * Creates a builder from an existing {@link LogicalPlan}.
   *
   * @param plan the logical plan to start from
   * @param ctx the session context
   * @return a new LogicalPlanBuilder
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.from">Rust
   *     DataFusion: LogicalPlanBuilder::from</a>
   */
  public static LogicalPlanBuilder from(LogicalPlan plan, SessionContext ctx) {
    return new LogicalPlanBuilder(
        LogicalPlanBuilderBridge.createFromPlan(ctx.bridge().dfContext(), plan.bridge()));
  }

  /**
   * Projects the specified expressions.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param exprs the expressions to project
   * @return a new LogicalPlanBuilder with the projection applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.project">Rust
   *     DataFusion: LogicalPlanBuilder::project</a>
   */
  public LogicalPlanBuilder project(List<Expr> exprs) {
    LogicalPlanBuilderBridge result = bridge.project(exprs);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Projects the specified expressions (varargs convenience).
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param exprs the expressions to project
   * @return a new LogicalPlanBuilder with the projection applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.project">Rust
   *     DataFusion: LogicalPlanBuilder::project</a>
   */
  public LogicalPlanBuilder project(Expr... exprs) {
    return project(List.of(exprs));
  }

  /**
   * Filters rows by the given predicate expression.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param predicate the filter predicate
   * @return a new LogicalPlanBuilder with the filter applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.filter">Rust
   *     DataFusion: LogicalPlanBuilder::filter</a>
   */
  public LogicalPlanBuilder filter(Expr predicate) {
    LogicalPlanBuilderBridge result = bridge.filter(predicate);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Sorts by the given sort expressions.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param sortExprs the sort expressions
   * @return a new LogicalPlanBuilder with the sort applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.sort">Rust
   *     DataFusion: LogicalPlanBuilder::sort</a>
   */
  public LogicalPlanBuilder sort(List<SortExpr> sortExprs) {
    LogicalPlanBuilderBridge result = bridge.sort(sortExprs);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Sorts by the given sort expressions (varargs convenience).
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param sortExprs the sort expressions
   * @return a new LogicalPlanBuilder with the sort applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.sort">Rust
   *     DataFusion: LogicalPlanBuilder::sort</a>
   */
  public LogicalPlanBuilder sort(SortExpr... sortExprs) {
    return sort(List.of(sortExprs));
  }

  /**
   * Limits the number of rows returned.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param skip the number of rows to skip
   * @param fetch the maximum number of rows to return, or -1 for no limit
   * @return a new LogicalPlanBuilder with the limit applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.limit">Rust
   *     DataFusion: LogicalPlanBuilder::limit</a>
   */
  public LogicalPlanBuilder limit(long skip, long fetch) {
    LogicalPlanBuilderBridge result = bridge.limit(skip, fetch);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Aggregates with the given group-by and aggregate expressions.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param groupExprs the group-by expressions
   * @param aggrExprs the aggregate expressions
   * @return a new LogicalPlanBuilder with the aggregation applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.aggregate">Rust
   *     DataFusion: LogicalPlanBuilder::aggregate</a>
   */
  public LogicalPlanBuilder aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    LogicalPlanBuilderBridge result = bridge.aggregate(groupExprs, aggrExprs);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Removes duplicate rows.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @return a new LogicalPlanBuilder with the distinct applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.distinct">Rust
   *     DataFusion: LogicalPlanBuilder::distinct</a>
   */
  public LogicalPlanBuilder distinct() {
    LogicalPlanBuilderBridge result = bridge.distinct();
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Applies a HAVING filter on an aggregation result.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param predicate the having predicate
   * @return a new LogicalPlanBuilder with the having applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.having">Rust
   *     DataFusion: LogicalPlanBuilder::having</a>
   */
  public LogicalPlanBuilder having(Expr predicate) {
    LogicalPlanBuilderBridge result = bridge.having(predicate);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Applies window expressions.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param windowExprs the window expressions
   * @return a new LogicalPlanBuilder with the window applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.window">Rust
   *     DataFusion: LogicalPlanBuilder::window</a>
   */
  public LogicalPlanBuilder window(List<Expr> windowExprs) {
    LogicalPlanBuilderBridge result = bridge.window(windowExprs);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Applies a subquery alias.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param alias the alias name
   * @return a new LogicalPlanBuilder with the alias applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.alias">Rust
   *     DataFusion: LogicalPlanBuilder::alias</a>
   */
  public LogicalPlanBuilder alias(String alias) {
    LogicalPlanBuilderBridge result = bridge.alias(alias);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Creates an explain plan.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param verbose if true, include more details
   * @param analyze if true, actually execute and show timing
   * @return a new LogicalPlanBuilder with the explain applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.explain">Rust
   *     DataFusion: LogicalPlanBuilder::explain</a>
   */
  public LogicalPlanBuilder explain(boolean verbose, boolean analyze) {
    LogicalPlanBuilderBridge result = bridge.explain(verbose, analyze);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Joins with another plan using column name pairs.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param right the right-side plan to join with
   * @param joinType the type of join
   * @param leftCols the left-side join column names
   * @param rightCols the right-side join column names
   * @return a new LogicalPlanBuilder with the join applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.join">Rust
   *     DataFusion: LogicalPlanBuilder::join</a>
   */
  public LogicalPlanBuilder join(
      LogicalPlan right, JoinType joinType, List<String> leftCols, List<String> rightCols) {
    LogicalPlanBuilderBridge result = bridge.join(right.bridge(), joinType, leftCols, rightCols);
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Cross joins with another plan.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param right the right-side plan to cross join with
   * @return a new LogicalPlanBuilder with the cross join applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.cross_join">Rust
   *     DataFusion: LogicalPlanBuilder::cross_join</a>
   */
  public LogicalPlanBuilder crossJoin(LogicalPlan right) {
    LogicalPlanBuilderBridge result = bridge.crossJoin(right.bridge());
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Unions with another plan.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param other the plan to union with
   * @return a new LogicalPlanBuilder with the union applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.union">Rust
   *     DataFusion: LogicalPlanBuilder::union</a>
   */
  public LogicalPlanBuilder union(LogicalPlan other) {
    LogicalPlanBuilderBridge result = bridge.union(other.bridge());
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Unions with another plan, removing duplicates.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @param other the plan to union distinct with
   * @return a new LogicalPlanBuilder with the union distinct applied
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.union_distinct">Rust
   *     DataFusion: LogicalPlanBuilder::union_distinct</a>
   */
  public LogicalPlanBuilder unionDistinct(LogicalPlan other) {
    LogicalPlanBuilderBridge result = bridge.unionDistinct(other.bridge());
    bridge.close();
    return new LogicalPlanBuilder(result);
  }

  /**
   * Intersects two plans.
   *
   * @param left the left-side plan
   * @param right the right-side plan
   * @param isAll if true, INTERSECT ALL (keeps duplicates); if false, INTERSECT
   * @return the intersected LogicalPlan
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.intersect">Rust
   *     DataFusion: LogicalPlanBuilder::intersect</a>
   */
  public static LogicalPlan intersect(LogicalPlan left, LogicalPlan right, boolean isAll) {
    return new LogicalPlan(
        LogicalPlanBuilderBridge.intersect(left.bridge(), right.bridge(), isAll));
  }

  /**
   * Computes the set difference of two plans.
   *
   * @param left the left-side plan
   * @param right the right-side plan
   * @param isAll if true, EXCEPT ALL (keeps duplicates); if false, EXCEPT
   * @return the excepted LogicalPlan
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.except">Rust
   *     DataFusion: LogicalPlanBuilder::except</a>
   */
  public static LogicalPlan except(LogicalPlan left, LogicalPlan right, boolean isAll) {
    return new LogicalPlan(LogicalPlanBuilderBridge.except(left.bridge(), right.bridge(), isAll));
  }

  /**
   * Builds and returns the final {@link LogicalPlan}.
   *
   * <p>This is a consuming method: the source builder is closed after the call.
   *
   * @return the built LogicalPlan
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.build">Rust
   *     DataFusion: LogicalPlanBuilder::build</a>
   */
  public LogicalPlan build() {
    LogicalPlanBridge result = bridge.build();
    bridge.close();
    return new LogicalPlan(result);
  }

  @Override
  public void close() {
    bridge.close();
  }
}
