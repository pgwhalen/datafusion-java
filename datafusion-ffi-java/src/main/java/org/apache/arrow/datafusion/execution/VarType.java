package org.apache.arrow.datafusion.execution;

/**
 * The type of variable being registered with a {@link VarProvider}.
 *
 * <p>DataFusion supports two kinds of variables in SQL:
 *
 * <ul>
 *   <li>{@link #SYSTEM} variables, referenced with {@code @@} prefix (e.g., {@code @@version})
 *   <li>{@link #USER_DEFINED} variables, referenced with {@code @} prefix (e.g., {@code @my_var})
 * </ul>
 *
 * {@snippet :
 * ctx.registerVariable(VarType.USER_DEFINED, myProvider, allocator);
 * ctx.registerVariable(VarType.SYSTEM, systemProvider, allocator);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/enum.VarType.html">Rust
 *     DataFusion: VarType</a>
 */
public enum VarType {
  /**
   * System variable, referenced with {@code @@} prefix in SQL (e.g., {@code SELECT @@version}).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/enum.VarType.html#variant.System">Rust
   *     DataFusion: VarType::System</a>
   */
  SYSTEM,

  /**
   * User-defined variable, referenced with {@code @} prefix in SQL (e.g., {@code SELECT @name}).
   *
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/enum.VarType.html#variant.UserDefined">Rust
   *     DataFusion: VarType::UserDefined</a>
   */
  USER_DEFINED
}
