package org.apache.arrow.datafusion;

/**
 * How nulls are treated in aggregate and window functions, corresponding to DataFusion's {@code
 * NullTreatment}.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/enum.NullTreatment.html">Rust
 *     DataFusion: NullTreatment</a>
 */
public enum NullTreatment {
  RESPECT_NULLS,
  IGNORE_NULLS
}
