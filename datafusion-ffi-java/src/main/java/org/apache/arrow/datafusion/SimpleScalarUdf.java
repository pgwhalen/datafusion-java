package org.apache.arrow.datafusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Package-private implementation of {@link ScalarUdf} for simple fixed-type UDFs.
 *
 * <p>Created via {@link ScalarUdf#simple}.
 */
final class SimpleScalarUdf implements ScalarUdf {
  private final String name;
  private final Volatility volatility;
  private final List<ArrowType> inputTypes;
  private final ArrowType outputType;
  private final SimpleScalarFunction fn;

  SimpleScalarUdf(
      String name,
      Volatility volatility,
      List<ArrowType> inputTypes,
      ArrowType outputType,
      SimpleScalarFunction fn) {
    this.name = name;
    this.volatility = volatility;
    this.inputTypes = List.copyOf(inputTypes);
    this.outputType = outputType;
    this.fn = fn;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Volatility volatility() {
    return volatility;
  }

  @Override
  public Field returnField(List<Field> argFields) {
    return new Field(name, Field.nullable(name, outputType).getFieldType(), null);
  }

  @Override
  public FieldVector invoke(
      List<FieldVector> args,
      List<Field> argFields,
      int numRows,
      Field returnField,
      BufferAllocator allocator) {
    return fn.invoke(args, numRows, allocator);
  }

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
