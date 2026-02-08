package org.apache.arrow.datafusion;

/** SQL dialect for the parser. Maps to DataFusion's {@code Dialect} enum. */
public enum SqlDialect {
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
