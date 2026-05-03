package org.apache.arrow.datafusion.logical_expr;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A scalar UDF with a single fixed signature and return type.
 *
 * <p>Created via {@link org.apache.arrow.datafusion.Functions#createUdf}.
 *
 * <p>Example:
 *
 * {@snippet :
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * ScalarUDF doubleIt = createUdf("double_it", Volatility.IMMUTABLE,
 *     List.of(new ArrowType.Int(64, true)),
 *     new ArrowType.Int(64, true),
 *     (args, numRows, allocator) -> {
 *         BigIntVector input = (BigIntVector) args.get(0);
 *         BigIntVector result = new BigIntVector("result", allocator);
 *         result.allocateNew(numRows);
 *         for (int i = 0; i < numRows; i++) {
 *             result.set(i, input.get(i) * 2);
 *         }
 *         result.setValueCount(numRows);
 *         return result;
 *     });
 * ctx.registerUdf(doubleIt, allocator);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.SimpleScalarUDF.html">Rust
 *     DataFusion: SimpleScalarUDF</a>
 */
public final class SimpleScalarUDF implements ScalarUDF {
  private final String name;
  private final Signature signature;
  private final List<ArrowType> inputTypes;
  private final ArrowType outputType;
  private final ScalarUDFImpl fn;

  /**
   * Creates a new simple scalar UDF.
   *
   * @param name the function name as it will appear in SQL
   * @param signature the function signature (including volatility)
   * @param inputTypes the expected input Arrow types
   * @param outputType the output Arrow type
   * @param fn the function implementation
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/struct.SimpleScalarUDF.html#method.new">Rust
   *     DataFusion: SimpleScalarUDF::new</a>
   */
  public SimpleScalarUDF(
      String name,
      Signature signature,
      List<ArrowType> inputTypes,
      ArrowType outputType,
      ScalarUDFImpl fn) {
    this.name = name;
    this.signature = signature;
    this.inputTypes = List.copyOf(inputTypes);
    this.outputType = outputType;
    this.fn = fn;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }

  /** {@inheritDoc} */
  @Override
  public Signature signature() {
    return signature;
  }

  /** {@inheritDoc} */
  @Override
  public Field returnField(List<Field> argFields) {
    return new Field(name, Field.nullable(name, outputType).getFieldType(), null);
  }

  /** {@inheritDoc} */
  @Override
  public FieldVector invoke(
      List<FieldVector> args,
      List<Field> argFields,
      int numRows,
      Field returnField,
      BufferAllocator allocator) {
    return fn.invoke(args, numRows, allocator);
  }

  /** {@inheritDoc} */
  @Override
  public List<Field> coerceTypes(List<Field> argFields) {
    List<Field> coerced = new ArrayList<>(inputTypes.size());
    for (int i = 0; i < inputTypes.size(); i++) {
      String fieldName = i < argFields.size() ? argFields.get(i).getName() : "arg" + i;
      coerced.add(Field.nullable(fieldName, inputTypes.get(i)));
    }
    return coerced;
  }
}
