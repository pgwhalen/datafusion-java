package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A user-defined scalar function that can be registered with a {@link SessionContext}.
 *
 * <p>Implement this interface to create custom scalar functions usable in SQL queries. For simple
 * fixed-type UDFs, use the {@link #simple} factory method instead.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ScalarUdf pow = ScalarUdf.simple("pow", Volatility.IMMUTABLE,
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
 */
public interface ScalarUdf {

  /** Returns the name of this function as it will appear in SQL. */
  String name();

  /** Returns the volatility of this function. */
  Volatility volatility();

  /**
   * Determines the return field (name + type) given the argument fields.
   *
   * @param argFields the fields of the input arguments
   * @return the field describing the return type
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
   */
  default List<Field> coerceTypes(List<Field> argFields) {
    return argFields;
  }

  /**
   * Creates a simple scalar UDF with fixed input and output types.
   *
   * @param name the function name
   * @param volatility the function volatility
   * @param inputTypes the expected input Arrow types
   * @param outputType the output Arrow type
   * @param fn the function implementation
   * @return a new ScalarUdf
   */
  static ScalarUdf simple(
      String name,
      Volatility volatility,
      List<ArrowType> inputTypes,
      ArrowType outputType,
      SimpleScalarFunction fn) {
    return new SimpleScalarUdf(name, volatility, inputTypes, outputType, fn);
  }
}
