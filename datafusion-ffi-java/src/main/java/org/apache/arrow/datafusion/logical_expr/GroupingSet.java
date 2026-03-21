package org.apache.arrow.datafusion.logical_expr;

/**
 * The kind of grouping set, corresponding to DataFusion's {@code GroupingSet} variants.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/enum.GroupingSet.html">Rust
 *     DataFusion: GroupingSet</a>
 */
public enum GroupingSet {
  ROLLUP,
  CUBE,
  GROUP_BY
}
