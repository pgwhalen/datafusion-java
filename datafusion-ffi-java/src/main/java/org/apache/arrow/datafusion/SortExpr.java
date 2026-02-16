package org.apache.arrow.datafusion;

/**
 * A sort expression, corresponding to DataFusion's {@code SortExpr}.
 *
 * @param expr the expression to sort by
 * @param asc true for ascending order, false for descending
 * @param nullsFirst true to sort nulls before non-null values
 */
public record SortExpr(Expr expr, boolean asc, boolean nullsFirst) {}
