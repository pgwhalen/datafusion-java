package org.apache.arrow.datafusion.logical_expr;

/**
 * Defines the type signature and volatility for a function.
 *
 * <p>Currently only exposes {@link #volatility()}. {@code TypeSignature} is not yet supported.
 *
 * <p>Example:
 *
 * {@snippet :
 * Signature sig = new Signature(Volatility.IMMUTABLE);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.Signature.html">Rust
 *     DataFusion: Signature</a>
 */
public record Signature(Volatility volatility) {

  /**
   * Returns the volatility of this signature.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.Signature.html#structfield.volatility">Rust
   *     DataFusion: Signature::volatility</a>
   */
  @Override
  public Volatility volatility() {
    return volatility;
  }
}
