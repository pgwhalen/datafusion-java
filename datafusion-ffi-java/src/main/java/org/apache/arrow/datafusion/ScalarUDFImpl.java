package org.apache.arrow.datafusion;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

/**
 * A simplified function interface for scalar UDFs with fixed input/output types.
 *
 * <p>Use this with {@link ScalarUDF#simple} to create UDFs without implementing the full {@link
 * ScalarUDF} interface.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/trait.ScalarUDFImpl.html">Rust
 *     DataFusion: ScalarUDFImpl</a>
 */
@FunctionalInterface
public interface ScalarUDFImpl {
  /**
   * Invokes the scalar function on the given input vectors.
   *
   * @param args the input vectors
   * @param numRows the number of rows to process
   * @param allocator the buffer allocator for creating the result vector
   * @return the result vector
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/trait.ScalarUDFImpl.html#method.invoke_with_args">Rust
   *     DataFusion: ScalarUDFImpl::invoke_with_args</a>
   */
  FieldVector invoke(List<FieldVector> args, int numRows, BufferAllocator allocator);
}
