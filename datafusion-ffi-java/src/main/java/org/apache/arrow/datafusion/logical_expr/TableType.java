package org.apache.arrow.datafusion.logical_expr;

/**
 * The type of a table in a DataFusion catalog.
 *
 * @see <a href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/enum.TableType.html">Rust
 *     DataFusion: TableType</a>
 */
public enum TableType {
  /** A base table (physical storage). */
  BASE,
  /** A view (virtual table defined by a query). */
  VIEW,
  /** A temporary table that exists only for the session. */
  TEMPORARY;
}
