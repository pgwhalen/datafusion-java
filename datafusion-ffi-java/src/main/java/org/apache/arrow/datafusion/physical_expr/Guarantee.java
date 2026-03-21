package org.apache.arrow.datafusion.physical_expr;

/**
 * The type of guarantee from a literal guarantee analysis.
 *
 * @see <a
 *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/enum.Guarantee.html">Rust
 *     DataFusion: Guarantee</a>
 */
public enum Guarantee {
  /** The column's values must be one of the specified literals. */
  IN,
  /** The column's values must not be any of the specified literals. */
  NOT_IN;
}
