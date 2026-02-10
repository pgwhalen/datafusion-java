package org.apache.arrow.datafusion;

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
final class ExecutionPlanHandle implements TraitHandle {
  private static final long ARROW_SCHEMA_SIZE = 72;

  // Callback struct field offsets
  // struct JavaExecutionPlanCallbacks {
  //   java_object: *mut c_void,             // offset 0
  //   schema_fn: fn,                        // offset 8
  //   properties_fn: fn,                    // offset 16
  //   execute_fn: fn,                       // offset 24
  //   release_fn: fn,                       // offset 32
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_SCHEMA_FN = 8;
  private static final long OFFSET_PROPERTIES_FN = 16;
  private static final long OFFSET_EXECUTE_FN = 24;
  private static final long OFFSET_RELEASE_FN = 32;

  // FFI_PlanProperties struct field offsets (3 x i32 = 12 bytes)
  // struct FFI_PlanProperties {
  //   output_partitioning: i32,             // offset 0
  //   emission_type: i32,                   // offset 4
  //   boundedness: i32,                     // offset 8
  // }
  private static final long FFI_PLAN_PROPERTIES_SIZE = 12;
  private static final long PROPS_OFFSET_OUTPUT_PARTITIONING = 0;
  private static final long PROPS_OFFSET_EMISSION_TYPE = 4;
  private static final long PROPS_OFFSET_BOUNDEDNESS = 8;

  // Static FunctionDescriptors - define the FFI signatures once at class load
  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  private static final FunctionDescriptor PROPERTIES_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  private static final FunctionDescriptor EXECUTE_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();
  private static final MethodHandle PROPERTIES_MH = initPropertiesMethodHandle();
  private static final MethodHandle EXECUTE_MH = initExecuteMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getSchema",
              MethodType.methodType(
                  int.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropertiesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getProperties",
              MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initExecuteMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "execute",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  long.class,
                  MemorySegment.class,
                  MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub schemaStub;
  private final UpcallStub propertiesStub;
  private final UpcallStub executeStub;
  private final UpcallStub releaseStub;

  ExecutionPlanHandle(
      ExecutionPlan plan, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.plan = plan;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_EXECUTION_PLAN_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate ExecutionPlan callbacks");
      }

      // Create upcall stubs - only the stubs are per-instance
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.propertiesStub = UpcallStub.create(PROPERTIES_MH.bindTo(this), PROPERTIES_DESC, arena);
      this.executeStub = UpcallStub.create(EXECUTE_MH.bindTo(this), EXECUTE_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(40); // 5 pointers * 8 bytes
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_FN, schemaStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_PROPERTIES_FN, propertiesStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_EXECUTE_FN, executeStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create ExecutionPlanHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return callbackStruct;
  }

  /** Callback: Get the schema. */
  @SuppressWarnings("unused") // Called via upcall stub
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

      return Errors.SUCCESS;
    } catch (Exception e) {
      return Errors.fromException(errorOut, e, arena, fullStackTrace);
    }
  }

  /** Callback: Get plan properties (partitioning, emission type, boundedness). */
  @SuppressWarnings("unused") // Called via upcall stub
  void getProperties(MemorySegment javaObject, MemorySegment propertiesOut) {
    PlanProperties props = plan.properties();
    MemorySegment out = propertiesOut.reinterpret(FFI_PLAN_PROPERTIES_SIZE);
    out.set(ValueLayout.JAVA_INT, PROPS_OFFSET_OUTPUT_PARTITIONING, props.outputPartitioning());
    out.set(
        ValueLayout.JAVA_INT,
        PROPS_OFFSET_EMISSION_TYPE,
        switch (props.emissionType()) {
          case INCREMENTAL -> 0;
          case FINAL -> 1;
          case BOTH -> 2;
        });
    out.set(
        ValueLayout.JAVA_INT,
        PROPS_OFFSET_BOUNDEDNESS,
        switch (props.boundedness()) {
          case BOUNDED -> 0;
          case UNBOUNDED -> 1;
        });
  }

  /** Callback: Execute the plan for a partition. */
  @SuppressWarnings("unused") // Called via upcall stub
  int execute(
      MemorySegment javaObject, long partition, MemorySegment readerOut, MemorySegment errorOut) {
    try {
      RecordBatchReader reader = plan.execute((int) partition, allocator);

      // Create a handle for the reader
      RecordBatchReaderHandle readerHandle =
          new RecordBatchReaderHandle(reader, allocator, arena, fullStackTrace);

      // Return the callback struct pointer
      readerHandle.setToPointer(readerOut);

      return Errors.SUCCESS;
    } catch (Exception e) {
      return Errors.fromException(errorOut, e, arena, fullStackTrace);
    }
  }

  /** Callback: Release the plan. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // Callback struct freed by Rust
  }
}
