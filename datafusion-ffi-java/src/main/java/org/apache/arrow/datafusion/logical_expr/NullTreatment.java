package org.apache.arrow.datafusion.logical_expr;

/**
 * How nulls are treated in aggregate and window functions, corresponding to DataFusion's {@code
 * NullTreatment}.
 *
 * <p>Example:
 *
 * {@snippet :
 * // RESPECT_NULLS includes nulls in computation
 * NullTreatment treatment = NullTreatment.RESPECT_NULLS;
 * // IGNORE_NULLS skips null values
 * NullTreatment ignore = NullTreatment.IGNORE_NULLS;
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/enum.NullTreatment.html">Rust
 *     DataFusion: NullTreatment</a>
 */
public enum NullTreatment {
  RESPECT_NULLS,
  IGNORE_NULLS
}
