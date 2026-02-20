package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

/**
 * A simplified function interface for scalar UDFs with fixed input/output types.
 *
 * <p>Use this with {@link ScalarUdf#simple} to create UDFs without implementing the full {@link
 * ScalarUdf} interface.
 */
@FunctionalInterface
public interface SimpleScalarFunction {
  /**
   * Invokes the scalar function on the given input vectors.
   *
   * @param args the input vectors
   * @param numRows the number of rows to process
   * @param allocator the buffer allocator for creating the result vector
   * @return the result vector
   */
  FieldVector invoke(List<FieldVector> args, int numRows, BufferAllocator allocator);
}
