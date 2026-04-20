package org.apache.arrow.datafusion.logical_expr;

/**
 * The kind of grouping set, corresponding to DataFusion's {@code GroupingSet} variants.
 *
 * <p>Example:
 *
 * {@snippet :
 * GroupingSet rollup = GroupingSet.ROLLUP;
 * GroupingSet cube = GroupingSet.CUBE;
 * // Used in GROUP BY expressions
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/expr/enum.GroupingSet.html">Rust
 *     DataFusion: GroupingSet</a>
 */
public enum GroupingSet {
  ROLLUP,
  CUBE,
  GROUP_BY
}
