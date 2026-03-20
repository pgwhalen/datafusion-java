package org.apache.arrow.datafusion;

/**
 * The type of join operation.
 *
 * <p>Matches Rust DataFusion's {@code JoinType} enum variants.
 *
 * @see <a href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.JoinType.html">Rust
 *     DataFusion: JoinType</a>
 */
public enum JoinType {
  INNER,
  LEFT,
  RIGHT,
  FULL,
  LEFT_SEMI,
  LEFT_ANTI,
  RIGHT_SEMI,
  RIGHT_ANTI
}
