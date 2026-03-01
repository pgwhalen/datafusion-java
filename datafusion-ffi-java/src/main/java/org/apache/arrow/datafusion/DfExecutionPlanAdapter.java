package org.apache.arrow.datafusion;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link ExecutionPlan} to the Diplomat-generated {@link
 * DfExecutionPlanTrait} interface for FFI callbacks.
 */
final class DfExecutionPlanAdapter implements DfExecutionPlanTrait {
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;

  // Cached FFI schema
  private final ArrowSchema ffiSchema;
  private final long schemaAddr;

  DfExecutionPlanAdapter(ExecutionPlan plan, BufferAllocator allocator) {
    this.plan = plan;
    this.allocator = allocator;
    this.ffiSchema = ArrowSchema.allocateNew(allocator);
    Data.exportSchema(allocator, plan.schema(), null, ffiSchema);
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
      DfRecordBatchReaderAdapter adapter = new DfRecordBatchReaderAdapter(reader, allocator);
      long ptr = DfRecordBatchReader.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      DfCatalogAdapter.writeError(errorAddr, errorCap, e.getMessage());
      return 0;
    }
  }
}
