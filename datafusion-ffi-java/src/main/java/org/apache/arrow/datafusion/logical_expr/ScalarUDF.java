package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import org.apache.arrow.datafusion.Functions;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A user-defined scalar function that can be registered with a {@link SessionContext}.
 *
 * <p>Implement this interface to create custom scalar functions usable in SQL queries. For simple
 * fixed-type UDFs, use {@link Functions#createUdf} instead.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * ScalarUDF pow = createUdf("pow", Volatility.IMMUTABLE,
 *     List.of(ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
 *             ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
 *     ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
 *     (args, numRows, allocator) -> {
 *         Float8Vector base = (Float8Vector) args.get(0);
 *         Float8Vector exp = (Float8Vector) args.get(1);
 *         Float8Vector result = new Float8Vector("result", allocator);
 *         result.allocateNew(numRows);
 *         for (int i = 0; i < numRows; i++) {
 *             result.set(i, Math.pow(base.get(i), exp.get(i)));
 *         }
 *         result.setValueCount(numRows);
 *         return result;
 *     });
 * ctx.registerUdf(pow, allocator);
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html">Rust
 *     DataFusion: ScalarUDF</a>
 */
public interface ScalarUDF {

  /**
   * Returns the name of this function as it will appear in SQL.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html#method.name">Rust
   *     DataFusion: ScalarUDF::name</a>
   */
  String name();

  /**
   * Returns the signature of this function, including its volatility.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html#method.signature">Rust
   *     DataFusion: ScalarUDF::signature</a>
   */
  Signature signature();

  /**
   * Determines the return field (name + type) given the argument fields.
   *
   * @param argFields the fields of the input arguments
   * @return the field describing the return type
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html#method.return_field_from_args">Rust
   *     DataFusion: ScalarUDF::return_field_from_args</a>
   */
  Field returnField(List<Field> argFields);

  /**
   * Invokes the function on the given input vectors.
   *
   * @param args the input vectors
   * @param argFields the fields describing each input argument
   * @param numRows the number of rows to process
   * @param returnField the expected return field
   * @param allocator the buffer allocator for creating the result vector
   * @return the result vector
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html#method.invoke_with_args">Rust
   *     DataFusion: ScalarUDF::invoke_with_args</a>
   */
  FieldVector invoke(
      List<FieldVector> args,
      List<Field> argFields,
      int numRows,
      Field returnField,
      BufferAllocator allocator);

  /**
   * Performs type coercion on the input argument fields. The default implementation returns the
   * argument fields unchanged.
   *
   * @param argFields the fields of the input arguments
   * @return the coerced fields
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/struct.ScalarUDF.html#method.coerce_types">Rust
   *     DataFusion: ScalarUDF::coerce_types</a>
   */
  default List<Field> coerceTypes(List<Field> argFields) {
    return argFields;
  }
}
