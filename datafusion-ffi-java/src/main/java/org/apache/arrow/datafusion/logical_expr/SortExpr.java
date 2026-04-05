package org.apache.arrow.datafusion.logical_expr;

/**
 * A sort expression, corresponding to DataFusion's {@code SortExpr}.
 *
 * <p>Example:
 *
 * {@snippet :
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * SortExpr ascending = new SortExpr(col("name"), true, false);
 * SortExpr descNullsFirst = new SortExpr(col("score"), false, true);
 * LogicalPlan sorted = builder.sort(List.of(ascending, descNullsFirst)).build();
 * }
 *
 * @param expr the expression to sort by
 * @param asc true for ascending order, false for descending
 * @param nullsFirst true to sort nulls before non-null values
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.SortExpr.html">Rust
 *     DataFusion: SortExpr</a>
 */
public record SortExpr(Expr expr, boolean asc, boolean nullsFirst) {

  /**
   * Returns the expression to sort by.
   *
   * @return the sort expression
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.SortExpr.html#structfield.expr">Rust
   *     DataFusion: SortExpr::expr</a>
   */
  public Expr expr() {
    return expr;
  }

  /**
   * Returns whether the sort order is ascending.
   *
   * @return true for ascending order, false for descending
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.SortExpr.html#structfield.asc">Rust
   *     DataFusion: SortExpr::asc</a>
   */
  public boolean asc() {
    return asc;
  }

  /**
   * Returns whether nulls are sorted before non-null values.
   *
   * @return true to sort nulls first
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.SortExpr.html#structfield.nulls_first">Rust
   *     DataFusion: SortExpr::nulls_first</a>
   */
  public boolean nullsFirst() {
    return nullsFirst;
  }
}
