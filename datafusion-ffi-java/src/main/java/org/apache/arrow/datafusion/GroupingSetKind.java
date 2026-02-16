package org.apache.arrow.datafusion;

/** The kind of grouping set, corresponding to DataFusion's {@code GroupingSet} variants. */
public enum GroupingSetKind {
  ROLLUP,
  CUBE,
  GROUP_BY
}
