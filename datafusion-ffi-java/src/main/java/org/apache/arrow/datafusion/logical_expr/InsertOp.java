package org.apache.arrow.datafusion.logical_expr;

/**
 * Insert operation mode for DataFrame write operations.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/dml/enum.InsertOp.html">Rust
 *     DataFusion: InsertOp</a>
 */
public enum InsertOp {
  APPEND,
  OVERWRITE,
  REPLACE
}
