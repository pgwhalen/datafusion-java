package org.apache.arrow.datafusion.config;

/**
 * SQL dialect for the parser.
 *
 * <p>Example:
 *
 * {@snippet :
 * SqlParserOptions parser = SqlParserOptions.builder()
 *     .dialect(Dialect.POSTGRESQL)
 *     .enableIdentNormalization(false)
 *     .build();
 * ConfigOptions config = ConfigOptions.builder()
 *     .sqlParser(parser)
 *     .build();
 * }
 *
 * @see <a href="https://docs.rs/datafusion/53.1.0/datafusion/config/enum.Dialect.html">Rust
 *     DataFusion: Dialect</a>
 */
public enum Dialect {
  GENERIC,
  MYSQL,
  POSTGRESQL,
  HIVE,
  SQLITE,
  SNOWFLAKE,
  REDSHIFT,
  MSSQL,
  CLICKHOUSE,
  BIGQUERY,
  ANSI,
  DUCKDB,
  DATABRICKS;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
