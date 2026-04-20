package org.apache.arrow.datafusion.logical_expr;

/**
 * The type of a table in a DataFusion catalog.
 *
 * <p>Example:
 *
 * {@snippet :
 * TableType type = TableType.BASE;
 * if (type == TableType.VIEW) {
 *     System.out.println("This is a view");
 * }
 * }
 *
 * @see <a href="https://docs.rs/datafusion-expr/53.1.0/datafusion_expr/enum.TableType.html">Rust
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
