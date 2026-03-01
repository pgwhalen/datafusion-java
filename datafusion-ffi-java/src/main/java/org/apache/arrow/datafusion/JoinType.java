package org.apache.arrow.datafusion;

/**
 * The type of join operation.
 *
 * <p>Matches Rust DataFusion's {@code JoinType} enum variants.
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
