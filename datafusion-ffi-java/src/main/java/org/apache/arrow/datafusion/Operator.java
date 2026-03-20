package org.apache.arrow.datafusion;

/**
 * Operators for binary expressions, corresponding to DataFusion's {@code Operator} enum.
 *
 * @see <a href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.Operator.html">Rust
 *     DataFusion: Operator</a>
 */
public enum Operator {
  Eq,
  NotEq,
  Lt,
  LtEq,
  Gt,
  GtEq,
  Plus,
  Minus,
  Multiply,
  Divide,
  Modulo,
  And,
  Or,
  IsDistinctFrom,
  IsNotDistinctFrom,
  RegexMatch,
  RegexIMatch,
  RegexNotMatch,
  RegexNotIMatch,
  LikeMatch,
  ILikeMatch,
  NotLikeMatch,
  NotILikeMatch,
  BitwiseAnd,
  BitwiseOr,
  BitwiseXor,
  BitwiseShiftRight,
  BitwiseShiftLeft,
  StringConcat,
  AtArrow,
  ArrowAt
}
