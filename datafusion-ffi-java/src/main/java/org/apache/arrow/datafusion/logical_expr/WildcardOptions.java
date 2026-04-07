package org.apache.arrow.datafusion.logical_expr;

/**
 * Options for a wildcard expression, corresponding to DataFusion's {@code WildcardOptions}.
 *
 * <p>The fields use {@code Object} as a placeholder type because the underlying sqlparser AST types
 * ({@code IlikeSelectItem}, {@code ExcludeSelectItem}, etc.) are not yet modeled in Java. They will
 * be replaced with concrete record types when needed.
 *
 * <p>Example:
 *
 * {@snippet :
 * WildcardOptions opts = WildcardOptions.EMPTY;
 * // All fields are null in the EMPTY instance
 * Object ilike = opts.ilike(); // null
 * Object exclude = opts.exclude(); // null
 * }
 *
 * @param ilike optional ILIKE pattern filter (stub, always null)
 * @param exclude optional EXCLUDE column list (stub, always null)
 * @param except optional EXCEPT column list (stub, always null)
 * @param replace optional REPLACE expressions (stub, always null)
 * @param rename optional RENAME column aliases (stub, always null)
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html">Rust
 *     DataFusion: WildcardOptions</a>
 */
public record WildcardOptions(
    Object ilike, Object exclude, Object except, Object replace, Object rename) {

  /** Empty options with all fields null. */
  public static final WildcardOptions EMPTY = new WildcardOptions(null, null, null, null, null);

  /**
   * {@inheritDoc}
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html#structfield.ilike">Rust
   *     DataFusion: WildcardOptions::ilike</a>
   */
  @Override
  public Object ilike() {
    return ilike;
  }

  /**
   * {@inheritDoc}
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html#structfield.exclude">Rust
   *     DataFusion: WildcardOptions::exclude</a>
   */
  @Override
  public Object exclude() {
    return exclude;
  }

  /**
   * {@inheritDoc}
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html#structfield.except">Rust
   *     DataFusion: WildcardOptions::except</a>
   */
  @Override
  public Object except() {
    return except;
  }

  /**
   * {@inheritDoc}
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html#structfield.replace">Rust
   *     DataFusion: WildcardOptions::replace</a>
   */
  @Override
  public Object replace() {
    return replace;
  }

  /**
   * {@inheritDoc}
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html#structfield.rename">Rust
   *     DataFusion: WildcardOptions::rename</a>
   */
  @Override
  public Object rename() {
    return rename;
  }
}
