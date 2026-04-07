package org.apache.arrow.datafusion.logical_expr;

/**
 * The volatility of a scalar UDF, indicating how its result depends on its inputs.
 *
 * <p>Example:
 *
 * {@snippet :
 * // Register a UDF with IMMUTABLE volatility
 * Volatility vol = Volatility.IMMUTABLE;
 * // IMMUTABLE means same input always gives same output
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.Volatility.html">Rust
 *     DataFusion: Volatility</a>
 */
public enum Volatility {
  /** The function always returns the same result for the same input. */
  IMMUTABLE,
  /** The function returns the same result within a single query but may differ across queries. */
  STABLE,
  /** The function may return different results each time it is called. */
  VOLATILE
}
