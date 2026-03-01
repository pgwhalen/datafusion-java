package org.apache.arrow.datafusion;

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
 */
public final class CaseBuilder {
  private final List<WhenThen> branches = new ArrayList<>();

  CaseBuilder() {}

  /** Add a WHEN/THEN branch. */
  public CaseBuilder when(Expr condition, Expr then) {
    branches.add(new WhenThen(condition, then));
    return this;
  }

  /** Terminal: build the CASE expression with an ELSE clause. */
  public Expr otherwise(Expr elseExpr) {
    return new Expr.CaseExpr(null, List.copyOf(branches), elseExpr);
  }

  /** Terminal: build the CASE expression without an ELSE clause (result is NULL). */
  public Expr end() {
    return new Expr.CaseExpr(null, List.copyOf(branches), null);
  }
}
