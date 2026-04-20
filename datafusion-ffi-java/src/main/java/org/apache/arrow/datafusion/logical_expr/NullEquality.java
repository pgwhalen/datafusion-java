package org.apache.arrow.datafusion.logical_expr;

/**
 * Null equality semantics for join operations.
 *
 * <p>{@code NULL_EQUALS_NOTHING} means {@code null != null} (standard SQL behavior). {@code
 * NULL_EQUALS_NULL} means {@code null == null} (used for IS NOT DISTINCT FROM).
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Join join) {
 *     NullEquality nullEq = join.nullEquality();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/53.1.0/datafusion_common/null_equality/enum.NullEquality.html">Rust
 *     DataFusion: NullEquality</a>
 */
public enum NullEquality {
  NULL_EQUALS_NOTHING,
  NULL_EQUALS_NULL
}
