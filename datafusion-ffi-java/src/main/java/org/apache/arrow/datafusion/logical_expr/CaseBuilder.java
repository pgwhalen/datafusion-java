package org.apache.arrow.datafusion.logical_expr;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for CASE/WHEN/THEN/ELSE expressions.
 *
 * <p>Mirrors Rust DataFusion's {@code CaseBuilder}. Usage:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * when(col("status").eq(lit("active")), lit(1))
 *     .when(col("status").eq(lit("pending")), lit(0))
 *     .otherwise(lit(-1))
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html">Rust
 *     DataFusion: CaseBuilder</a>
 */
public final class CaseBuilder {
  private final List<WhenThen> branches = new ArrayList<>();

  public CaseBuilder() {}

  /**
   * Add a WHEN/THEN branch.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html#method.when">Rust
   *     DataFusion: CaseBuilder::when</a>
   */
  public CaseBuilder when(Expr condition, Expr then) {
    branches.add(new WhenThen(condition, then));
    return this;
  }

  /**
   * Terminal: build the CASE expression with an ELSE clause.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html#method.otherwise">Rust
   *     DataFusion: CaseBuilder::otherwise</a>
   */
  public Expr otherwise(Expr elseExpr) {
    return new Expr.CaseExpr(null, List.copyOf(branches), elseExpr);
  }

  /**
   * Terminal: build the CASE expression without an ELSE clause (result is NULL).
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/conditional_expressions/struct.CaseBuilder.html#method.end">Rust
   *     DataFusion: CaseBuilder::end</a>
   */
  public Expr end() {
    return new Expr.CaseExpr(null, List.copyOf(branches), null);
  }
}
