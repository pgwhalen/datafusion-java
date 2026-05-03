package org.apache.arrow.datafusion.logical_expr;

/**
 * The constraint type for join operations (ON vs USING).
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Join join) {
 *     JoinConstraint constraint = join.joinConstraint();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/53.1.0/datafusion_common/join_type/enum.JoinConstraint.html">Rust
 *     DataFusion: JoinConstraint</a>
 */
public enum JoinConstraint {
  ON,
  USING
}
