package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Static factory methods for building DataFusion expressions.
 *
 * <p>This class provides the primary entry points for programmatic expression construction. With a
 * static import, expression code reads like SQL:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * col("age").gt(lit(18)).and(col("name").eq(lit("Alice")))
 * }</pre>
 *
 * <p>This parallels Rust DataFusion's {@code prelude} module and Spark's {@code functions} class.
 */
public final class Functions {
  private Functions() {}

  // ── Column references ──

  /** Reference a column by name. Equivalent to Rust's {@code col()}. */
  public static Expr col(String name) {
    return new Expr.ColumnExpr(new Column(name, null, null));
  }

  /** Reference a column with table qualification. */
  public static Expr col(String relation, String name) {
    return new Expr.ColumnExpr(new Column(name, new TableReference.Bare(relation), null));
  }

  // ── Literals ──

  /** Wrap an int value as a literal expression. */
  public static Expr lit(int value) {
    return new Expr.LiteralExpr(new ScalarValue.Int32(value));
  }

  /** Wrap a long value as a literal expression. */
  public static Expr lit(long value) {
    return new Expr.LiteralExpr(new ScalarValue.Int64(value));
  }

  /** Wrap a float value as a literal expression. */
  public static Expr lit(float value) {
    return new Expr.LiteralExpr(new ScalarValue.Float32(value));
  }

  /** Wrap a double value as a literal expression. */
  public static Expr lit(double value) {
    return new Expr.LiteralExpr(new ScalarValue.Float64(value));
  }

  /** Wrap a String value as a literal expression. */
  public static Expr lit(String value) {
    return new Expr.LiteralExpr(new ScalarValue.Utf8(value));
  }

  /** Wrap a boolean value as a literal expression. */
  public static Expr lit(boolean value) {
    return new Expr.LiteralExpr(new ScalarValue.BooleanValue(value));
  }

  /** Null literal. */
  public static Expr litNull() {
    return new Expr.LiteralExpr(new ScalarValue.Null());
  }

  // ── Aggregate functions ──

  /** AVG aggregate function. */
  public static Expr avg(Expr expr) {
    return aggFn("avg", expr);
  }

  /** SUM aggregate function. */
  public static Expr sum(Expr expr) {
    return aggFn("sum", expr);
  }

  /** COUNT aggregate function. */
  public static Expr count(Expr expr) {
    return aggFn("count", expr);
  }

  /** MIN aggregate function. */
  public static Expr min(Expr expr) {
    return aggFn("min", expr);
  }

  /** MAX aggregate function. */
  public static Expr max(Expr expr) {
    return aggFn("max", expr);
  }

  /** MEDIAN aggregate function. */
  public static Expr median(Expr expr) {
    return aggFn("median", expr);
  }

  /** STDDEV aggregate function. */
  public static Expr stddev(Expr expr) {
    return aggFn("stddev", expr);
  }

  /** VARIANCE aggregate function. */
  public static Expr variance(Expr expr) {
    return aggFn("variance", expr);
  }

  /** COUNT(DISTINCT expr) aggregate function. */
  public static Expr countDistinct(Expr expr) {
    return new Expr.AggregateFunctionExpr("count", List.of(expr), true, null, List.of(), null);
  }

  /** COUNT(*) aggregate function. */
  public static Expr countAll() {
    return aggFn("count", new Expr.LiteralExpr(new ScalarValue.Int64(1)));
  }

  // ── Conditional expressions ──

  /** Start a CASE WHEN chain. Equivalent to Rust's {@code when()}. */
  public static CaseBuilder when(Expr condition, Expr then) {
    return new CaseBuilder().when(condition, then);
  }

  // ── Utility ──

  /** Cast an expression to a different Arrow data type. */
  public static Expr cast(Expr expr, ArrowType dataType) {
    return new Expr.CastExpr(expr, dataType);
  }

  /** Try-cast an expression to a different Arrow data type (returns null on failure). */
  public static Expr tryCast(Expr expr, ArrowType dataType) {
    return new Expr.TryCastExpr(expr, dataType);
  }

  /** Logical NOT. */
  public static Expr not(Expr expr) {
    return new Expr.NotExpr(expr);
  }

  // ── Private helpers ──

  private static Expr aggFn(String name, Expr expr) {
    return new Expr.AggregateFunctionExpr(name, List.of(expr), false, null, List.of(), null);
  }
}
