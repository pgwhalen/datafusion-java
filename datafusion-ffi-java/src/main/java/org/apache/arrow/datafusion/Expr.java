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
 */
public sealed interface Expr {

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
  record GroupingSetExpr(GroupingSetKind kind, List<List<Expr>> groups) implements Expr {}

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
