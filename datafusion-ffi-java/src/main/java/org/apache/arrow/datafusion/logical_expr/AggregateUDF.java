package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A user-defined aggregate function that can be registered with a {@link SessionContext}.
 *
 * <p>Implement this interface to create custom aggregate functions usable in SQL queries. Unlike
 * scalar UDFs (which process row-by-row), aggregate UDFs accumulate state across multiple rows to
 * produce a single result per group.
 *
 * <p>Example usage:
 *
 * {@snippet :
 * AggregateUDF mySum = new AggregateUDF() {
 *     public String name() { return "my_sum"; }
 *     public Signature signature() { return new Signature(Volatility.IMMUTABLE); }
 *     public Field returnField(List<Field> argFields) {
 *         return Field.nullable("my_sum", new ArrowType.Int(64, true));
 *     }
 *     public Accumulator createAccumulator() { return new MySumAccumulator(); }
 * };
 * ctx.registerUdaf(mySum, allocator);
 * DataFrame result = ctx.sql("SELECT my_sum(value) FROM t");
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.AggregateUDF.html">Rust
 *     DataFusion: AggregateUDF</a>
 */
public interface AggregateUDF {

  /**
   * Returns the name of this aggregate function as it will appear in SQL.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public String name() {
   *     return "my_sum";
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.AggregateUDF.html#method.name">Rust
   *     DataFusion: AggregateUDF::name</a>
   */
  String name();

  /**
   * Returns the signature of this aggregate function, including its volatility.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Signature signature() {
   *     return new Signature(Volatility.IMMUTABLE);
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.AggregateUDF.html#method.signature">Rust
   *     DataFusion: AggregateUDF::signature</a>
   */
  Signature signature();

  /**
   * Determines the return field (name + type) given the argument fields.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Field returnField(List<Field> argFields) {
   *     return Field.nullable("my_sum", new ArrowType.Int(64, true));
   * }
   * }
   *
   * @param argFields the fields of the input arguments
   * @return the field describing the return type
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.AggregateUDF.html#method.return_field">Rust
   *     DataFusion: AggregateUDF::return_field</a>
   */
  Field returnField(List<Field> argFields);

  /**
   * Creates a new accumulator instance for aggregating values.
   *
   * <p>DataFusion calls this method to create one accumulator per group. The accumulator tracks
   * state across multiple calls to {@link Accumulator#updateBatch} and produces a final result via
   * {@link Accumulator#evaluate}.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Accumulator createAccumulator() {
   *     return new MySumAccumulator();
   * }
   * }
   *
   * @return a new accumulator instance
   * @see <a
   *     href="https://docs.rs/datafusion-expr/53.1.0/datafusion_expr/trait.AggregateUDFImpl.html#tymethod.accumulator">Rust
   *     DataFusion: AggregateUDFImpl::accumulator</a>
   */
  Accumulator createAccumulator();

  /**
   * Returns the fields used to store intermediate aggregation state.
   *
   * <p>Override this method when your accumulator tracks multiple state values (e.g., AVG tracks
   * both sum and count). The default returns a single field with the return type.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public List<Field> stateFields(Field returnField) {
   *     return List.of(
   *         Field.nullable("sum", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
   *         Field.nullable("count", new ArrowType.Int(64, true))
   *     );
   * }
   * }
   *
   * @param returnField the return field of the aggregate function
   * @return the fields describing the intermediate state
   * @see <a
   *     href="https://docs.rs/datafusion-expr/53.1.0/datafusion_expr/trait.AggregateUDFImpl.html#method.state_fields">Rust
   *     DataFusion: AggregateUDFImpl::state_fields</a>
   */
  default List<Field> stateFields(Field returnField) {
    return List.of(Field.nullable(name() + "_value", returnField.getType()));
  }

  /**
   * Performs type coercion on the input argument fields. The default implementation returns the
   * argument fields unchanged.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public List<Field> coerceTypes(List<Field> argFields) {
   *     return List.of(Field.nullable("arg0", new ArrowType.Int(64, true)));
   * }
   * }
   *
   * @param argFields the fields of the input arguments
   * @return the coerced fields
   * @see <a
   *     href="https://docs.rs/datafusion-expr/53.1.0/datafusion_expr/trait.AggregateUDFImpl.html#method.coerce_types">Rust
   *     DataFusion: AggregateUDFImpl::coerce_types</a>
   */
  default List<Field> coerceTypes(List<Field> argFields) {
    return argFields;
  }
}
