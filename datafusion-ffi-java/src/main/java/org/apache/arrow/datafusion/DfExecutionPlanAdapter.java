package org.apache.arrow.datafusion;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
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
  public int emissionType() {
    return switch (plan.properties().emissionType()) {
      case INCREMENTAL -> 0;
      case FINAL -> 1;
      case BOTH -> 2;
    };
  }

  @Override
  public int boundedness() {
    return switch (plan.properties().boundedness()) {
      case BOUNDED -> 0;
      case UNBOUNDED -> 1;
    };
  }

  @Override
  public long execute(int partition, long errorAddr, long errorCap) {
    try {
      RecordBatchReader reader = plan.execute(partition, allocator);
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
