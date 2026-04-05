package org.apache.arrow.datafusion.config;

/**
 * Detail level for EXPLAIN ANALYZE output.
 *
 * <p>Example:
 *
 * <p>{@snippet : ExplainOptions explain = ExplainOptions.builder()
 * .analyzeLevel(ExplainAnalyzeLevel.DEV) .showStatistics(true) .build(); ConfigOptions config =
 * ConfigOptions.builder() .explain(explain) .build(); }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/common/format/enum.ExplainAnalyzeLevel.html">Rust
 *     DataFusion: ExplainAnalyzeLevel</a>
 */
public enum ExplainAnalyzeLevel {
  SUMMARY,
  DEV;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
