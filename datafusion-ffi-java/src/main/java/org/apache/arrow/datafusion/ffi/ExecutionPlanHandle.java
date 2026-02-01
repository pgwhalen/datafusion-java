package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for ExecutionPlan.
 *
 * <p>This class creates upcall stubs that Rust can invoke to execute a Java {@link ExecutionPlan}.
 * It manages the lifecycle of the callback struct and upcall stubs.
 */
final class ExecutionPlanHandle implements AutoCloseable {
  private static final long ARROW_SCHEMA_SIZE = 72;

  // Callback struct field offsets
  // struct JavaExecutionPlanCallbacks {
  //   java_object: *mut c_void,             // offset 0
  //   schema_fn: fn,                        // offset 8
  //   output_partitioning_fn: fn,           // offset 16
  //   execute_fn: fn,                       // offset 24
  //   release_fn: fn,                       // offset 32
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_SCHEMA_FN = 8;
  private static final long OFFSET_OUTPUT_PARTITIONING_FN = 16;
  private static final long OFFSET_EXECUTE_FN = 24;
  private static final long OFFSET_RELEASE_FN = 32;

  private final Arena arena;
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final MemorySegment schemaStub;
  private final MemorySegment outputPartitioningStub;
  private final MemorySegment executeStub;
  private final MemorySegment releaseStub;

  ExecutionPlanHandle(ExecutionPlan plan, BufferAllocator allocator, Arena arena) {
    this.arena = arena;
    this.plan = plan;
    this.allocator = allocator;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_EXECUTION_PLAN_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate ExecutionPlan callbacks");
      }

      // Create upcall stubs for the callback functions
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Linker linker = DataFusionBindings.getLinker();

      // schema_fn: (java_object, schema_out, error_out) -> i32
      MethodHandle schemaHandle =
          lookup.bind(
              this,
              "getSchema",
              MethodType.methodType(
                  int.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
      FunctionDescriptor schemaDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
      this.schemaStub = linker.upcallStub(schemaHandle, schemaDesc, arena);

      // output_partitioning_fn: (java_object) -> i32
      MethodHandle outputPartitioningHandle =
          lookup.bind(
              this, "getOutputPartitioning", MethodType.methodType(int.class, MemorySegment.class));
      FunctionDescriptor outputPartitioningDesc =
          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
      this.outputPartitioningStub =
          linker.upcallStub(outputPartitioningHandle, outputPartitioningDesc, arena);

      // execute_fn: (java_object, partition, reader_out, error_out) -> i32
      MethodHandle executeHandle =
          lookup.bind(
              this,
              "execute",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  long.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor executeDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.JAVA_LONG,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.executeStub = linker.upcallStub(executeHandle, executeDesc, arena);

      // release_fn: (java_object) -> void
      MethodHandle releaseHandle =
          lookup.bind(this, "release", MethodType.methodType(void.class, MemorySegment.class));
      FunctionDescriptor releaseDesc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
      this.releaseStub = linker.upcallStub(releaseHandle, releaseDesc, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(40); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_FN, schemaStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_OUTPUT_PARTITIONING_FN, outputPartitioningStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_EXECUTE_FN, executeStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub);

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create ExecutionPlanHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Get the schema. */
  @SuppressWarnings("unused")
  int getSchema(MemorySegment javaObject, MemorySegment schemaOut, MemorySegment errorOut) {
    try {
      Schema schema = plan.schema();

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);

        // Copy the FFI schema to the output pointer
        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment destSchema = schemaOut.reinterpret(ARROW_SCHEMA_SIZE);
        destSchema.copyFrom(srcSchema);

        // Clear release in source - dest (Rust copy) has it
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      }

      return 0;
    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
    }
  }

  /** Callback: Get output partitioning count. */
  @SuppressWarnings("unused")
  int getOutputPartitioning(MemorySegment javaObject) {
    return plan.outputPartitioning();
  }

  /** Callback: Execute the plan for a partition. */
  @SuppressWarnings("unused")
  int execute(
      MemorySegment javaObject, long partition, MemorySegment readerOut, MemorySegment errorOut) {
    try {
      RecordBatchReader reader = plan.execute((int) partition, allocator);

      // Create a handle for the reader
      RecordBatchReaderHandle readerHandle = new RecordBatchReaderHandle(reader, allocator, arena);

      // Return the callback struct pointer
      MemorySegment readerCallbacks = readerHandle.getCallbackStruct();
      readerOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, readerCallbacks);

      return 0;
    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
    }
  }

  /** Callback: Release the plan. */
  @SuppressWarnings("unused")
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  private void setError(MemorySegment errorOut, String message) {
    if (errorOut.equals(MemorySegment.NULL)) {
      return;
    }
    try {
      MemorySegment msgSegment = arena.allocateUtf8String(message);
      errorOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, msgSegment);
    } catch (Exception ignored) {
      // Best effort error reporting
    }
  }

  @Override
  public void close() {
    // Callback struct freed by Rust
  }
}
