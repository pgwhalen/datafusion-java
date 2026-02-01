package org.apache.arrow.datafusion.ffi;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An execution plan that produces record batches.
 *
 * <p>This interface allows Java code to implement custom execution plans that DataFusion can
 * execute. When DataFusion calls {@link #execute(int, BufferAllocator)}, the implementation should
 * return a {@link RecordBatchReader} that produces the data for the given partition.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * public class MyExecutionPlan implements ExecutionPlan {
 *     private final Schema schema;
 *     private final List<VectorSchemaRoot> batches;
 *
 *     @Override
 *     public Schema schema() {
 *         return schema;
 *     }
 *
 *     @Override
 *     public RecordBatchReader execute(int partition, BufferAllocator allocator) {
 *         return new MyBatchReader(batches);
 *     }
 * }
 * }</pre>
 */
public interface ExecutionPlan {
  /**
   * Returns the schema of the data produced by this plan.
   *
   * @return The Arrow schema
   */
  Schema schema();

  /**
   * Returns the number of output partitions.
   *
   * <p>Default implementation returns 1 (single partition).
   *
   * @return The number of partitions
   */
  default int outputPartitioning() {
    return 1;
  }

  /**
   * Executes the plan for the given partition.
   *
   * <p>The returned {@link RecordBatchReader} will be iterated by the caller to consume the data.
   * The reader should produce batches that conform to the schema returned by {@link #schema()}.
   *
   * @param partition The partition index (0-based)
   * @param allocator The buffer allocator to use for Arrow memory
   * @return A reader that produces record batches for this partition
   * @throws DataFusionException if execution fails
   */
  RecordBatchReader execute(int partition, BufferAllocator allocator);
}
