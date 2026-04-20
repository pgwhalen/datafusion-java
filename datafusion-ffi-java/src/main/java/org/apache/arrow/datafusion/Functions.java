package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.datafusion.common.Column;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.logical_expr.CaseBuilder;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.ScalarUDF;
import org.apache.arrow.datafusion.logical_expr.ScalarUDFImpl;
import org.apache.arrow.datafusion.logical_expr.Signature;
import org.apache.arrow.datafusion.logical_expr.SimpleScalarUDF;
import org.apache.arrow.datafusion.logical_expr.Volatility;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Static factory methods for building DataFusion expressions.
 *
 * <p>This class provides the primary entry points for programmatic expression construction. With a
 * static import, expression code reads like SQL:
 *
 * {@snippet :
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * col("age").gt(lit(18)).and(col("name").eq(lit("Alice")))
 * }
 *
 * <p>This parallels Rust DataFusion's {@code prelude} module and Spark's {@code functions} class.
 *
 * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/index.html">Rust DataFusion:
 *     datafusion::prelude</a>
 */
public final class Functions {
  private Functions() {}

  // ── Column references ──

  /**
   * Reference a column by name.
   *
   * {@snippet :
   * Expr ageCol = col("age");
   * }
   *
   * @param name the column name
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.col.html">Rust
   *     DataFusion: col</a>
   */
  public static Expr col(String name) {
    return new Expr.ColumnExpr(new Column(name, null, null));
  }

  /**
   * Reference a column with table qualification.
   *
   * {@snippet :
   * Expr col = col("orders", "id");
   * }
   *
   * @param relation the table name
   * @param name the column name
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.col.html">Rust
   *     DataFusion: col</a>
   */
  public static Expr col(String relation, String name) {
    return new Expr.ColumnExpr(new Column(name, new TableReference.Bare(relation), null));
  }

  // ── Literals ──

  /**
   * Wrap an int value as a literal expression.
   *
   * {@snippet :
   * Expr threshold = lit(42);
   * }
   *
   * @param value the int value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(int value) {
    return new Expr.LiteralExpr(new ScalarValue.Int32(value));
  }

  /**
   * Wrap a long value as a literal expression. See {@link #lit(int)} for an example.
   *
   * @param value the long value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(long value) {
    return new Expr.LiteralExpr(new ScalarValue.Int64(value));
  }

  /**
   * Wrap a float value as a literal expression. See {@link #lit(int)} for an example.
   *
   * @param value the float value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(float value) {
    return new Expr.LiteralExpr(new ScalarValue.Float32(value));
  }

  /**
   * Wrap a double value as a literal expression. See {@link #lit(int)} for an example.
   *
   * @param value the double value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(double value) {
    return new Expr.LiteralExpr(new ScalarValue.Float64(value));
  }

  /**
   * Wrap a String value as a literal expression. See {@link #lit(int)} for an example.
   *
   * @param value the string value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(String value) {
    return new Expr.LiteralExpr(new ScalarValue.Utf8(value));
  }

  /**
   * Wrap a boolean value as a literal expression. See {@link #lit(int)} for an example.
   *
   * @param value the boolean value
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr lit(boolean value) {
    return new Expr.LiteralExpr(new ScalarValue.BooleanValue(value));
  }

  /**
   * Null literal.
   *
   * {@snippet :
   * Expr nullExpr = litNull();
   * }
   *
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.lit.html">Rust
   *     DataFusion: lit</a>
   */
  public static Expr litNull() {
    return new Expr.LiteralExpr(new ScalarValue.Null());
  }

  // ── Aggregate functions ──

  /**
   * AVG aggregate function.
   *
   * {@snippet :
   * Expr avgSalary = avg(col("salary"));
   * }
   *
   * @param expr the expression to average
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.avg.html">Rust
   *     DataFusion: avg</a>
   */
  public static Expr avg(Expr expr) {
    return aggFn("avg", expr);
  }

  /**
   * SUM aggregate function.
   *
   * {@snippet :
   * Expr totalRevenue = sum(col("revenue"));
   * }
   *
   * @param expr the expression to sum
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.sum.html">Rust
   *     DataFusion: sum</a>
   */
  public static Expr sum(Expr expr) {
    return aggFn("sum", expr);
  }

  /**
   * COUNT aggregate function.
   *
   * {@snippet :
   * Expr numRows = count(col("id"));
   * }
   *
   * @param expr the expression to count
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.count.html">Rust
   *     DataFusion: count</a>
   */
  public static Expr count(Expr expr) {
    return aggFn("count", expr);
  }

  /**
   * MIN aggregate function.
   *
   * {@snippet :
   * Expr lowest = min(col("price"));
   * }
   *
   * @param expr the expression to find the minimum of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.min.html">Rust
   *     DataFusion: min</a>
   */
  public static Expr min(Expr expr) {
    return aggFn("min", expr);
  }

  /**
   * MAX aggregate function.
   *
   * {@snippet :
   * Expr highest = max(col("price"));
   * }
   *
   * @param expr the expression to find the maximum of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.max.html">Rust
   *     DataFusion: max</a>
   */
  public static Expr max(Expr expr) {
    return aggFn("max", expr);
  }

  /**
   * MEDIAN aggregate function.
   *
   * {@snippet :
   * Expr mid = median(col("score"));
   * }
   *
   * @param expr the expression to compute the median of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.median.html">Rust
   *     DataFusion: median</a>
   */
  public static Expr median(Expr expr) {
    return aggFn("median", expr);
  }

  /**
   * STDDEV aggregate function.
   *
   * {@snippet :
   * Expr sd = stddev(col("measurement"));
   * }
   *
   * @param expr the expression to compute standard deviation of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.stddev.html">Rust
   *     DataFusion: stddev</a>
   */
  public static Expr stddev(Expr expr) {
    return aggFn("stddev", expr);
  }

  /**
   * VARIANCE (sample variance) aggregate function.
   *
   * {@snippet :
   * Expr var = variance(col("measurement"));
   * }
   *
   * @param expr the expression to compute variance of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.var_sample.html">Rust
   *     DataFusion: var_sample</a>
   */
  public static Expr variance(Expr expr) {
    return aggFn("variance", expr);
  }

  /**
   * COUNT(DISTINCT expr) aggregate function.
   *
   * {@snippet :
   * Expr uniqueCount = countDistinct(col("category"));
   * }
   *
   * @param expr the expression to count distinct values of
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.count_distinct.html">Rust
   *     DataFusion: count_distinct</a>
   */
  public static Expr countDistinct(Expr expr) {
    return new Expr.AggregateFunctionExpr("count", List.of(expr), true, null, List.of(), null);
  }

  /**
   * COUNT(*) aggregate function.
   *
   * {@snippet :
   * Expr totalRows = countAll();
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/functions_aggregate/expr_fn/fn.count.html">Rust
   *     DataFusion: count</a>
   */
  public static Expr countAll() {
    return aggFn("count", new Expr.LiteralExpr(new ScalarValue.Int64(1)));
  }

  // ── Conditional expressions ──

  /**
   * Start a CASE WHEN chain.
   *
   * {@snippet :
   * Expr result = when(col("status").eq(lit("active")), lit(1))
   *     .when(col("status").eq(lit("inactive")), lit(0))
   *     .otherwise(lit(-1));
   * }
   *
   * @param condition the WHEN condition
   * @param then the THEN expression
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.when.html">Rust
   *     DataFusion: when</a>
   */
  public static CaseBuilder when(Expr condition, Expr then) {
    return new CaseBuilder().when(condition, then);
  }

  // ── Utility ──

  /**
   * Cast an expression to a different Arrow data type.
   *
   * {@snippet :
   * Expr asDouble = cast(col("value"), new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
   * }
   *
   * @param expr the expression to cast
   * @param dataType the target Arrow data type
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.cast.html">Rust
   *     DataFusion: cast</a>
   */
  public static Expr cast(Expr expr, ArrowType dataType) {
    return new Expr.CastExpr(expr, dataType);
  }

  /**
   * Try-cast an expression to a different Arrow data type (returns null on failure).
   *
   * {@snippet :
   * Expr safe = tryCast(col("value"), new ArrowType.Int(32, true));
   * }
   *
   * @param expr the expression to cast
   * @param dataType the target Arrow data type
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.try_cast.html">Rust
   *     DataFusion: try_cast</a>
   */
  public static Expr tryCast(Expr expr, ArrowType dataType) {
    return new Expr.TryCastExpr(expr, dataType);
  }

  /**
   * Logical NOT.
   *
   * {@snippet :
   * Expr notActive = not(col("is_active"));
   * }
   *
   * @param expr the expression to negate
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.not.html">Rust
   *     DataFusion: not</a>
   */
  public static Expr not(Expr expr) {
    return new Expr.NotExpr(expr);
  }

  // ── UDF creation ──

  /**
   * Creates a simple scalar UDF with fixed input and output types.
   *
   * {@snippet :
   * ScalarUDF doubleIt = createUdf("double_it", Volatility.IMMUTABLE,
   *     List.of(new ArrowType.Int(32, true)),
   *     new ArrowType.Int(32, true),
   *     (args, numRows, alloc) -> { return resultVector; });
   * ctx.registerUdf(doubleIt, allocator);
   * }
   *
   * @param name the function name
   * @param volatility the function volatility
   * @param inputTypes the expected input Arrow types
   * @param outputType the output Arrow type
   * @param fn the function implementation
   * @return a new ScalarUDF
   * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/fn.create_udf.html">Rust
   *     DataFusion: create_udf</a>
   */
  public static ScalarUDF createUdf(
      String name,
      Volatility volatility,
      List<ArrowType> inputTypes,
      ArrowType outputType,
      ScalarUDFImpl fn) {
    return new SimpleScalarUDF(name, new Signature(volatility), inputTypes, outputType, fn);
  }

  // ── Private helpers ──

  private static Expr aggFn(String name, Expr expr) {
    return new Expr.AggregateFunctionExpr(name, List.of(expr), false, null, List.of(), null);
  }
}
