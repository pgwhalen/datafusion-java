package org.apache.arrow.datafusion.config;

/** Detail level for EXPLAIN ANALYZE output. */
public enum ExplainAnalyzeLevel {
  SUMMARY,
  DEV;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
