package org.apache.arrow.datafusion;

/**
 * A when-then pair in a {@link Expr.CaseExpr CASE} expression.
 *
 * @param when the condition expression
 * @param then the result expression when the condition is true
 */
public record WhenThen(Expr when, Expr then) {}
