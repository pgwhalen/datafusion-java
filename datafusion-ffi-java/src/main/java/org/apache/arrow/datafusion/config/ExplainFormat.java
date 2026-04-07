package org.apache.arrow.datafusion.config;

/**
 * Output format for EXPLAIN statements.
 *
 * <p>Example:
 *
 * {@snippet :
 * ExplainOptions explain = ExplainOptions.builder()
 *     .format(ExplainFormat.TREE)
 *     .showStatistics(true)
 *     .build();
 * ConfigOptions config = ConfigOptions.builder()
 *     .explain(explain)
 *     .build();
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.ExplainFormat.html">Rust
 *     DataFusion: ExplainFormat</a>
 */
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
