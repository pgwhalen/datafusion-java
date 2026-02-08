package org.apache.arrow.datafusion;

/** Output format for EXPLAIN statements. */
public enum ExplainFormat {
  INDENT,
  TREE,
  POSTGRES_JSON,
  GRAPHVIZ;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
