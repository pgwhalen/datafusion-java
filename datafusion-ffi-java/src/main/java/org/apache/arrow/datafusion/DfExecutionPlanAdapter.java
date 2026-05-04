package org.apache.arrow.datafusion;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.execution.TaskContext;
import org.apache.arrow.datafusion.generated.DfBoundedness;
import org.apache.arrow.datafusion.generated.DfEmissionType;
import org.apache.arrow.datafusion.generated.DfExecutionPlanTrait;
import org.apache.arrow.datafusion.generated.DfRecordBatchReader;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link ExecutionPlan} to the Diplomat-generated {@link
 * DfExecutionPlanTrait} interface for FFI callbacks.
 */
final class DfExecutionPlanAdapter implements DfExecutionPlanTrait {
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  // Cached FFI schema
  private final ArrowSchema ffiSchema;
  private final long schemaAddr;

  DfExecutionPlanAdapter(ExecutionPlan plan, BufferAllocator allocator, boolean fullStackTrace) {
    this.plan = plan;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
    this.ffiSchema = ArrowSchema.allocateNew(allocator);
    try {
      Data.exportSchema(allocator, plan.schema(), null, ffiSchema);
    } catch (Exception e) {
      ffiSchema.close();
      throw e;
    }
    this.schemaAddr = ffiSchema.memoryAddress();
  }

  /** Close the cached FFI schema after Rust has imported it (call after createRaw). */
  void closeFfiSchema() {
    ffiSchema.release();
    ffiSchema.close();
  }

  @Override
  public long schemaAddress() {
    return schemaAddr;
  }

  @Override
  public int outputPartitioning() {
    return plan.properties().outputPartitioning();
  }

  @Override
  public DfEmissionType emissionType() {
    return switch (plan.properties().emissionType()) {
      case INCREMENTAL -> DfEmissionType.INCREMENTAL;
      case FINAL -> DfEmissionType.FINAL;
      case BOTH -> DfEmissionType.BOTH;
    };
  }

  @Override
  public DfBoundedness boundedness() {
    return switch (plan.properties().boundedness()) {
      case BOUNDED -> DfBoundedness.BOUNDED;
      case UNBOUNDED -> DfBoundedness.UNBOUNDED;
    };
  }

  @Override
  public long execute(int partition, long taskCtxAddr, long errorAddr, long errorCap) {
    try (TaskContext taskCtx = TaskContext.fromNativeAddress(taskCtxAddr)) {
      RecordBatchReader reader = plan.execute(partition, taskCtx, allocator);
      DfRecordBatchReaderAdapter adapter =
          new DfRecordBatchReaderAdapter(reader, allocator, fullStackTrace);
      long ptr = DfRecordBatchReader.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
