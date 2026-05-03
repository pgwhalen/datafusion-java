package org.apache.arrow.datafusion.physical_plan;

import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.execution.TaskContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * An execution plan that produces record batches.
 *
 * <p>This interface allows Java code to implement custom execution plans that DataFusion can
 * execute. When DataFusion calls {@link #execute(int, TaskContext, BufferAllocator)}, the
 * implementation should return a {@link RecordBatchReader} that produces the data for the given
 * partition.
 *
 * <p>Example implementation:
 *
 * {@snippet :
 * public class MyExecutionPlan implements ExecutionPlan {
 *     private final Schema schema;
 *     private final List<VectorSchemaRoot> batches;
 *
 *     @Override
 *     public Schema schema() { return schema; }
 *
 *     @Override
 *     public RecordBatchReader execute(int partition, TaskContext taskContext, BufferAllocator allocator) {
 *         return new MyBatchReader(batches);
 *     }
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/physical_plan/trait.ExecutionPlan.html">Rust
 *     DataFusion: ExecutionPlan</a>
 */
public interface ExecutionPlan {
  /**
   * Returns the schema of the data produced by this plan.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Schema schema() {
   *     return new Schema(List.of(Field.nullable("value", new ArrowType.Utf8())));
   * }
   * }
   *
   * @return The Arrow schema
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/physical_plan/trait.ExecutionPlan.html#method.schema">Rust
   *     DataFusion: ExecutionPlan::schema</a>
   */
  Schema schema();

  /**
   * Returns the number of output partitions.
   *
   * <p>Default implementation returns 1 (single partition).
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public int outputPartitioning() {
   *     return dataPartitions.size();
   * }
   * }
   *
   * @return The number of partitions
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/physical_plan/trait.ExecutionPlan.html#method.output_partitioning">Rust
   *     DataFusion: ExecutionPlan::output_partitioning</a>
   */
  default int outputPartitioning() {
    return 1;
  }

  /**
   * Returns the plan properties describing output characteristics.
   *
   * <p>Default implementation uses {@link #outputPartitioning()} with incremental emission and
   * bounded input. Override this method to customize emission type and boundedness.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public PlanProperties properties() {
   *     return new PlanProperties(2, EmissionType.FINAL, Boundedness.BOUNDED);
   * }
   * }
   *
   * @return The plan properties
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/physical_plan/trait.ExecutionPlan.html#method.properties">Rust
   *     DataFusion: ExecutionPlan::properties</a>
   */
  default PlanProperties properties() {
    return new PlanProperties(outputPartitioning(), EmissionType.INCREMENTAL, Boundedness.BOUNDED);
  }

  /**
   * Executes the plan for the given partition.
   *
   * <p>The returned {@link RecordBatchReader} will be iterated by the caller to consume the data.
   * The reader should produce batches that conform to the schema returned by {@link #schema()}.
   *
   * <p>{@code taskContext} is a borrowed handle owned by the FFI adapter and closed after this
   * call returns; implementations must not retain it. Call {@link TaskContext#withSessionConfig}
   * or {@link TaskContext#withRuntime} to derive owned copies.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public RecordBatchReader execute(int partition, TaskContext taskContext, BufferAllocator allocator) {
   *     VectorSchemaRoot root = VectorSchemaRoot.create(schema(), allocator);
   *     // populate root with data for this partition
   *     return new SingleBatchReader(root);
   * }
   * }
   *
   * @param partition The partition index (0-based)
   * @param taskContext The task context (session id, config, runtime, UDF registries)
   * @param allocator The buffer allocator to use for Arrow memory
   * @return A reader that produces record batches for this partition
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/physical_plan/trait.ExecutionPlan.html#method.execute">Rust
   *     DataFusion: ExecutionPlan::execute</a>
   */
  RecordBatchReader execute(int partition, TaskContext taskContext, BufferAllocator allocator);
}
