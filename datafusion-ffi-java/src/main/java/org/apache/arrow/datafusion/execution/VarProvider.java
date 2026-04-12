package org.apache.arrow.datafusion.execution;

import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * A provider of variable values for SQL queries.
 *
 * <p>Implementations resolve variable references in SQL at execution time. User-defined variables
 * use the {@code @} prefix (e.g., {@code @my_var}), while system variables use {@code @@} (e.g.,
 * {@code @@version}).
 *
 * <p>The variable name is passed as a list of strings. For simple variables like {@code @name}, the
 * list is {@code ["@name"]}. For dotted variables like {@code @app.setting}, the list is
 * {@code ["@app", "setting"]}.
 *
 * <p>Example implementation:
 *
 * {@snippet :
 * VarProvider provider = new VarProvider() {
 *     @Override
 *     public ScalarValue getValue(List<String> varNames) {
 *         return switch (varNames.getFirst()) {
 *             case "@name" -> new ScalarValue.Utf8("Alice");
 *             case "@count" -> new ScalarValue.Int32(42);
 *             default -> throw new IllegalArgumentException("Unknown variable: " + varNames);
 *         };
 *     }
 *
 *     @Override
 *     public Optional<ArrowType> getType(List<String> varNames) {
 *         return switch (varNames.getFirst()) {
 *             case "@name" -> Optional.of(ArrowType.Utf8.INSTANCE);
 *             case "@count" -> Optional.of(new ArrowType.Int(32, true));
 *             default -> Optional.empty();
 *         };
 *     }
 * };
 * ctx.registerVariable(VarType.USER_DEFINED, provider, allocator);
 * DataFrame df = ctx.sql("SELECT @name, @count");
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/trait.VarProvider.html">Rust
 *     DataFusion: VarProvider</a>
 */
public interface VarProvider {

  /**
   * Returns the value of the variable identified by the given names.
   *
   * <p>Called during query execution to resolve variable references. The variable name includes the
   * {@code @} or {@code @@} prefix.
   *
   * {@snippet :
   * @Override
   * public ScalarValue getValue(List<String> varNames) {
   *     return new ScalarValue.Utf8("hello");
   * }
   * }
   *
   * @param varNames the variable name components (e.g., {@code ["@my_var"]} or {@code ["@app",
   *     "setting"]})
   * @return the scalar value for this variable
   * @throws RuntimeException if the variable is unknown or cannot be resolved
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/trait.VarProvider.html#tymethod.get_value">Rust
   *     DataFusion: VarProvider::get_value</a>
   */
  ScalarValue getValue(List<String> varNames);

  /**
   * Returns the data type of the variable identified by the given names.
   *
   * <p>Called during query planning to determine the type of a variable reference. Return {@link
   * Optional#empty()} if the variable is unknown.
   *
   * {@snippet :
   * @Override
   * public Optional<ArrowType> getType(List<String> varNames) {
   *     return Optional.of(ArrowType.Utf8.INSTANCE);
   * }
   * }
   *
   * @param varNames the variable name components
   * @return the Arrow type of the variable, or empty if unknown
   * @see <a
   *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/var_provider/trait.VarProvider.html#tymethod.get_type">Rust
   *     DataFusion: VarProvider::get_type</a>
   */
  Optional<ArrowType> getType(List<String> varNames);
}
