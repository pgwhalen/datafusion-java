package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * A DataFusion logical expression, corresponding to {@code datafusion::logical_expr::Expr}.
 *
 * <p>This sealed interface mirrors DataFusion's {@code Expr} enum. Each variant is a record that
 * holds the expression data. Expressions are deserialized from protobuf bytes received from the
 * Rust FFI layer.
 *
 * <p>Use {@code instanceof} pattern matching to inspect expression structure:
 *
 * <pre>{@code
 * if (expr instanceof Expr.BinaryExpr bin) {
 *     System.out.println("Left: " + bin.left());
 *     System.out.println("Op: " + bin.op());
 *     System.out.println("Right: " + bin.right());
 * }
 * }</pre>
 *
 * <p>Variants that cannot be represented via protobuf (e.g., subquery expressions) are captured by
 * {@link UnresolvedExpr}, which stores the raw proto bytes for lossless round-trip serialization.
 *
 * @see <a href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.Expr.html">Rust
 *     DataFusion: Expr</a>
 */
public sealed interface Expr {

  // ── Comparison ──

  /** Equality: this = other */
  default Expr eq(Expr other) {
    return new BinaryExpr(this, Operator.Eq, other);
  }

  /** Not equal: this != other */
  default Expr notEq(Expr other) {
    return new BinaryExpr(this, Operator.NotEq, other);
  }

  /** Less than: this &lt; other */
  default Expr lt(Expr other) {
    return new BinaryExpr(this, Operator.Lt, other);
  }

  /** Less than or equal: this &lt;= other */
  default Expr ltEq(Expr other) {
    return new BinaryExpr(this, Operator.LtEq, other);
  }

  /** Greater than: this &gt; other */
  default Expr gt(Expr other) {
    return new BinaryExpr(this, Operator.Gt, other);
  }

  /** Greater than or equal: this &gt;= other */
  default Expr gtEq(Expr other) {
    return new BinaryExpr(this, Operator.GtEq, other);
  }

  // ── Logical ──

  /** Logical AND: this AND other */
  default Expr and(Expr other) {
    return new BinaryExpr(this, Operator.And, other);
  }

  /** Logical OR: this OR other */
  default Expr or(Expr other) {
    return new BinaryExpr(this, Operator.Or, other);
  }

  /** Logical NOT: NOT this */
  default Expr not() {
    return new NotExpr(this);
  }

  // ── Arithmetic ──

  /** Addition: this + other */
  default Expr plus(Expr other) {
    return new BinaryExpr(this, Operator.Plus, other);
  }

  /** Subtraction: this - other */
  default Expr minus(Expr other) {
    return new BinaryExpr(this, Operator.Minus, other);
  }

  /** Multiplication: this * other */
  default Expr multiply(Expr other) {
    return new BinaryExpr(this, Operator.Multiply, other);
  }

  /** Division: this / other */
  default Expr divide(Expr other) {
    return new BinaryExpr(this, Operator.Divide, other);
  }

  /** Modulo: this % other */
  default Expr modulo(Expr other) {
    return new BinaryExpr(this, Operator.Modulo, other);
  }

  /** Unary negation: -this */
  default Expr negate() {
    return new NegativeExpr(this);
  }

  // ── Pattern matching ──

  /** SQL LIKE pattern match */
  default Expr like(Expr pattern) {
    return new LikeExpr(false, this, pattern, null, false);
  }

  /** SQL NOT LIKE pattern match */
  default Expr notLike(Expr pattern) {
    return new LikeExpr(true, this, pattern, null, false);
  }

  /** Case-insensitive LIKE (ILIKE) */
  default Expr ilike(Expr pattern) {
    return new LikeExpr(false, this, pattern, null, true);
  }

  // ── NULL checks ──

  /** IS NULL */
  default Expr isNull() {
    return new IsNullExpr(this);
  }

  /** IS NOT NULL */
  default Expr isNotNull() {
    return new IsNotNullExpr(this);
  }

  /** IS TRUE */
  default Expr isTrue() {
    return new IsTrueExpr(this);
  }

  /** IS FALSE */
  default Expr isFalse() {
    return new IsFalseExpr(this);
  }

  // ── IN list ──

  /** IN (value1, value2, ...) */
  default Expr inList(List<Expr> values) {
    return new InListExpr(this, values, false);
  }

  /** NOT IN (value1, value2, ...) */
  default Expr notInList(List<Expr> values) {
    return new InListExpr(this, values, true);
  }

  // ── BETWEEN ──

  /** BETWEEN low AND high */
  default Expr between(Expr low, Expr high) {
    return new BetweenExpr(this, false, low, high);
  }

  /** NOT BETWEEN low AND high */
  default Expr notBetween(Expr low, Expr high) {
    return new BetweenExpr(this, true, low, high);
  }

  // ── Aliasing ──

  /** Alias this expression: expr AS name */
  default Expr alias(String name) {
    return new AliasExpr(this, name, List.of());
  }

  // ── Sorting ──

  /** Sort ascending, nulls last (default) */
  default SortExpr sortAsc() {
    return new SortExpr(this, true, false);
  }

  /** Sort descending, nulls first (default) */
  default SortExpr sortDesc() {
    return new SortExpr(this, false, true);
  }

  /** Sort with explicit control */
  default SortExpr sort(boolean asc, boolean nullsFirst) {
    return new SortExpr(this, asc, nullsFirst);
  }

  // ── Record types ──

  /**
   * An alias expression ({@code Expr::Alias}).
   *
   * @param expr the aliased expression
   * @param alias the alias name
   * @param relation the optional relation references
   */
  record AliasExpr(Expr expr, String alias, List<TableReference> relation) implements Expr {}

  /**
   * A column reference ({@code Expr::Column}).
   *
   * @param column the column reference
   */
  record ColumnExpr(Column column) implements Expr {}

  /**
   * A literal value ({@code Expr::Literal}).
   *
   * @param value the scalar value
   */
  record LiteralExpr(ScalarValue value) implements Expr {}

  /**
   * A binary expression ({@code Expr::BinaryExpr}).
   *
   * @param left the left operand
   * @param op the operator
   * @param right the right operand
   */
  record BinaryExpr(Expr left, Operator op, Expr right) implements Expr {}

  /**
   * A LIKE expression ({@code Expr::Like}).
   *
   * @param negated true for NOT LIKE
   * @param expr the expression to match
   * @param pattern the pattern expression
   * @param escapeChar the optional escape character
   * @param caseInsensitive true for ILIKE
   */
  record LikeExpr(
      boolean negated, Expr expr, Expr pattern, Character escapeChar, boolean caseInsensitive)
      implements Expr {}

  /**
   * A SIMILAR TO expression ({@code Expr::SimilarTo}).
   *
   * @param negated true for NOT SIMILAR TO
   * @param expr the expression to match
   * @param pattern the pattern expression
   * @param escapeChar the optional escape character
   */
  record SimilarToExpr(boolean negated, Expr expr, Expr pattern, Character escapeChar)
      implements Expr {}

  /**
   * A NOT expression ({@code Expr::Not}).
   *
   * @param expr the negated expression
   */
  record NotExpr(Expr expr) implements Expr {}

  /**
   * An IS NOT NULL expression ({@code Expr::IsNotNull}).
   *
   * @param expr the expression to check
   */
  record IsNotNullExpr(Expr expr) implements Expr {}

  /**
   * An IS NULL expression ({@code Expr::IsNull}).
   *
   * @param expr the expression to check
   */
  record IsNullExpr(Expr expr) implements Expr {}

  /**
   * An IS TRUE expression ({@code Expr::IsTrue}).
   *
   * @param expr the expression to check
   */
  record IsTrueExpr(Expr expr) implements Expr {}

  /**
   * An IS FALSE expression ({@code Expr::IsFalse}).
   *
   * @param expr the expression to check
   */
  record IsFalseExpr(Expr expr) implements Expr {}

  /**
   * An IS UNKNOWN expression ({@code Expr::IsUnknown}).
   *
   * @param expr the expression to check
   */
  record IsUnknownExpr(Expr expr) implements Expr {}

  /**
   * An IS NOT TRUE expression ({@code Expr::IsNotTrue}).
   *
   * @param expr the expression to check
   */
  record IsNotTrueExpr(Expr expr) implements Expr {}

  /**
   * An IS NOT FALSE expression ({@code Expr::IsNotFalse}).
   *
   * @param expr the expression to check
   */
  record IsNotFalseExpr(Expr expr) implements Expr {}

  /**
   * An IS NOT UNKNOWN expression ({@code Expr::IsNotUnknown}).
   *
   * @param expr the expression to check
   */
  record IsNotUnknownExpr(Expr expr) implements Expr {}

  /**
   * A negation expression ({@code Expr::Negative}).
   *
   * @param expr the negated expression
   */
  record NegativeExpr(Expr expr) implements Expr {}

  /**
   * A BETWEEN expression ({@code Expr::Between}).
   *
   * @param expr the expression to test
   * @param negated true for NOT BETWEEN
   * @param low the lower bound
   * @param high the upper bound
   */
  record BetweenExpr(Expr expr, boolean negated, Expr low, Expr high) implements Expr {}

  /**
   * A CASE expression ({@code Expr::Case}).
   *
   * @param expr the optional match expression (null for searched CASE)
   * @param whenThen the when-then clauses
   * @param elseExpr the optional else expression (null if absent)
   */
  record CaseExpr(Expr expr, List<WhenThen> whenThen, Expr elseExpr) implements Expr {}

  /**
   * A CAST expression ({@code Expr::Cast}).
   *
   * @param expr the expression to cast
   * @param dataType the target Arrow data type
   */
  record CastExpr(Expr expr, ArrowType dataType) implements Expr {}

  /**
   * A TRY_CAST expression ({@code Expr::TryCast}).
   *
   * @param expr the expression to cast
   * @param dataType the target Arrow data type
   */
  record TryCastExpr(Expr expr, ArrowType dataType) implements Expr {}

  /**
   * A scalar function call ({@code Expr::ScalarFunction}).
   *
   * @param funcName the function name
   * @param args the function arguments
   */
  record ScalarFunctionExpr(String funcName, List<Expr> args) implements Expr {}

  /**
   * An aggregate function call ({@code Expr::AggregateFunction}).
   *
   * @param funcName the function name
   * @param args the function arguments
   * @param distinct true for DISTINCT aggregation
   * @param filter the optional filter expression (null if absent)
   * @param orderBy the optional ORDER BY expressions
   * @param nullTreatment the optional null treatment
   */
  record AggregateFunctionExpr(
      String funcName,
      List<Expr> args,
      boolean distinct,
      Expr filter,
      List<SortExpr> orderBy,
      NullTreatment nullTreatment)
      implements Expr {}

  /**
   * A window function call ({@code Expr::WindowFunction}).
   *
   * @param funcName the function name
   * @param args the function arguments
   * @param partitionBy the PARTITION BY expressions
   * @param orderBy the ORDER BY expressions
   * @param windowFrame the window frame definition
   * @param nullTreatment the optional null treatment
   */
  record WindowFunctionExpr(
      String funcName,
      List<Expr> args,
      List<Expr> partitionBy,
      List<SortExpr> orderBy,
      WindowFrame windowFrame,
      NullTreatment nullTreatment)
      implements Expr {}

  /**
   * An IN list expression ({@code Expr::InList}).
   *
   * @param expr the expression to test
   * @param list the list of values
   * @param negated true for NOT IN
   */
  record InListExpr(Expr expr, List<Expr> list, boolean negated) implements Expr {}

  /**
   * A wildcard expression ({@code Expr::Wildcard}).
   *
   * @param qualifier the optional table qualifier
   */
  record WildcardExpr(TableReference qualifier) implements Expr {}

  /**
   * A grouping set expression ({@code Expr::GroupingSet}).
   *
   * @param kind the kind of grouping set
   * @param groups the grouping expressions
   */
  record GroupingSetExpr(GroupingSet kind, List<List<Expr>> groups) implements Expr {}

  /**
   * A placeholder expression ({@code Expr::Placeholder}).
   *
   * @param id the placeholder identifier (e.g., "$1")
   * @param dataType the optional data type (null if unresolved)
   */
  record PlaceholderExpr(String id, ArrowType dataType) implements Expr {}

  /**
   * An UNNEST expression ({@code Expr::Unnest}).
   *
   * @param exprs the expressions to unnest
   */
  record UnnestExpr(List<Expr> exprs) implements Expr {}

  /**
   * A catch-all for expression variants that cannot be fully decoded in Java.
   *
   * <p>This includes subquery variants and any future expression types. The raw protobuf bytes are
   * preserved for lossless round-trip serialization.
   *
   * @param protoBytes the raw protobuf bytes of the LogicalExprNode
   */
  record UnresolvedExpr(byte[] protoBytes) implements Expr {}
}
