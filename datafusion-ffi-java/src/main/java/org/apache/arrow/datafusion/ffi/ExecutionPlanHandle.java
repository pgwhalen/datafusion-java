package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.ExecutionPlan;
import org.apache.arrow.datafusion.PlanProperties;
import org.apache.arrow.datafusion.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for ExecutionPlan.
 *
 * <p>This class constructs an {@code FFI_ExecutionPlan} struct directly in Java memory. The struct
 * contains function pointers ({@code properties}, {@code children}, {@code name}, {@code execute},
 * {@code clone}, {@code release}) that Rust invokes via {@code ForeignExecutionPlan}.
 *
 * <p>Layout of FFI_ExecutionPlan (64 bytes, align 8):
 *
 * <pre>
 * offset  0: properties fn ptr         (ADDRESS)
 * offset  8: children fn ptr           (ADDRESS)
 * offset 16: name fn ptr               (ADDRESS)
 * offset 24: execute fn ptr            (ADDRESS)
 * offset 32: clone fn ptr              (ADDRESS)
 * offset 40: release fn ptr            (ADDRESS)
 * offset 48: private_data ptr          (ADDRESS)
 * offset 56: library_marker_id fn ptr  (ADDRESS)
 * </pre>
 */
final class ExecutionPlanHandle implements TraitHandle {
  private static final long ARROW_SCHEMA_SIZE = 72;

  // FFI_ExecutionPlan struct layout (8 pointers = 64 bytes)
  private static final long FFI_EXECUTION_PLAN_SIZE = 64;
  private static final long OFFSET_PROPERTIES = 0;
  private static final long OFFSET_CHILDREN = 8;
  private static final long OFFSET_NAME = 16;
  private static final long OFFSET_EXECUTE = 24;
  private static final long OFFSET_CLONE = 32;
  private static final long OFFSET_RELEASE = 40;
  private static final long OFFSET_PRIVATE_DATA = 48;
  private static final long OFFSET_LIBRARY_MARKER_ID = 56;

  // Return type sizes (validated against Rust at runtime)
  private static final long FFI_PLAN_PROPERTIES_SIZE = 64;
  private static final long RVEC_PLAN_SIZE = 32;
  private static final long FFI_RESULT_STREAM_SIZE = 40;
  private static final long FFI_TASK_CONTEXT_SIZE = 72;

  // FFIResult discriminant offsets (same pattern as poll_next)
  private static final long RESULT_DISC_OFFSET = 0; // RResult discriminant (0=ROk, 1=RErr)
  private static final long RESULT_PAYLOAD_OFFSET = 8; // Start of payload
  private static final long FFI_RECORD_BATCH_STREAM_SIZE = 32;

  // Struct layouts for return types — use JAVA_LONG for natural 8-byte alignment
  private static final StructLayout PLAN_PROPERTIES_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(FFI_PLAN_PROPERTIES_SIZE / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout RVEC_PLAN_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(RVEC_PLAN_SIZE / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRStringSize() / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout FFI_RESULT_STREAM_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(FFI_RESULT_STREAM_SIZE / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout TASK_CONTEXT_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(FFI_TASK_CONTEXT_SIZE / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout EXECUTION_PLAN_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(FFI_EXECUTION_PLAN_SIZE / 8, ValueLayout.JAVA_LONG));

  // properties: (ADDRESS plan) -> STRUCT (FFI_PlanProperties, 64 bytes)
  private static final FunctionDescriptor PROPERTIES_DESC =
      FunctionDescriptor.of(PLAN_PROPERTIES_LAYOUT, ValueLayout.ADDRESS);

  // children: (ADDRESS plan) -> STRUCT (RVec<FFI_ExecutionPlan>, 32 bytes)
  private static final FunctionDescriptor CHILDREN_DESC =
      FunctionDescriptor.of(RVEC_PLAN_LAYOUT, ValueLayout.ADDRESS);

  // name: (ADDRESS plan) -> STRUCT (RString)
  private static final FunctionDescriptor NAME_DESC =
      FunctionDescriptor.of(RSTRING_LAYOUT, ValueLayout.ADDRESS);

  // execute: (ADDRESS plan, LONG partition, STRUCT context) -> STRUCT (FFIResult<stream>)
  private static final FunctionDescriptor EXECUTE_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_STREAM_LAYOUT,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          TASK_CONTEXT_LAYOUT);

  // clone: (ADDRESS plan) -> STRUCT (FFI_ExecutionPlan, 64 bytes)
  private static final FunctionDescriptor CLONE_DESC =
      FunctionDescriptor.of(EXECUTION_PLAN_LAYOUT, ValueLayout.ADDRESS);

  // release: (ADDRESS plan) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle PROPERTIES_MH = initPropertiesMethodHandle();
  private static final MethodHandle CHILDREN_MH = initChildrenMethodHandle();
  private static final MethodHandle NAME_MH = initNameMethodHandle();
  private static final MethodHandle EXECUTE_MH = initExecuteMethodHandle();
  private static final MethodHandle CLONE_MH = initCloneMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  // library_marker_id function pointer — looked up once at class load
  private static final MemorySegment JAVA_MARKER_ID_FN = initMarkerIdFnPtr();

  private static MethodHandle initPropertiesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getProperties",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initChildrenMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getChildren",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initNameMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getName",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
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
                  MemorySegment.class, MemorySegment.class, long.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initCloneMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "clonePlan",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
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

  private static MemorySegment initMarkerIdFnPtr() {
    return NativeLoader.get()
        .find("datafusion_java_marker_id")
        .orElseThrow(
            () -> new ExceptionInInitializerError("Symbol not found: datafusion_java_marker_id"));
  }

  // Runtime-validated sizes (set once at first construction)
  private static volatile boolean sizesValidated = false;

  private final Arena arena;
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiPlan;

  // Pre-allocated buffers for return values to avoid per-call allocation
  private final MemorySegment propertiesBuffer;
  private final MemorySegment childrenBuffer;
  private final MemorySegment nameBuffer;
  private final MemorySegment executeResultBuffer;
  private final MemorySegment cloneBuffer;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub propertiesStub;
  private final UpcallStub childrenStub;
  private final UpcallStub nameStub;
  private final UpcallStub executeStub;
  private final UpcallStub cloneStub;
  private final UpcallStub releaseStub;

  // Keep references to reader handles created during execute() to prevent GC
  // while Rust holds pointers to the FFI_RecordBatchStream structs
  private final List<RecordBatchReaderHandle> readerHandles = new ArrayList<>();

  ExecutionPlanHandle(
      ExecutionPlan plan, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.plan = plan;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Validate sizes against Rust at first use
      if (!sizesValidated) {
        validateSizes();
        sizesValidated = true;
      }

      // Pre-allocate return buffers in arena
      this.propertiesBuffer = arena.allocate(FFI_PLAN_PROPERTIES_SIZE, 8);
      this.childrenBuffer = arena.allocate(RVEC_PLAN_SIZE, 8);
      this.nameBuffer = arena.allocate(NativeUtil.getRStringSize(), 8);
      this.executeResultBuffer = arena.allocate(FFI_RESULT_STREAM_SIZE, 8);
      this.cloneBuffer = arena.allocate(FFI_EXECUTION_PLAN_SIZE, 8);

      // Build FFI_PlanProperties eagerly so errors propagate immediately
      // (returning zeroed bytes would create null function pointers → SIGSEGV)
      buildProperties();

      // Create upcall stubs
      this.propertiesStub = UpcallStub.create(PROPERTIES_MH.bindTo(this), PROPERTIES_DESC, arena);
      this.childrenStub = UpcallStub.create(CHILDREN_MH.bindTo(this), CHILDREN_DESC, arena);
      this.nameStub = UpcallStub.create(NAME_MH.bindTo(this), NAME_DESC, arena);
      this.executeStub = UpcallStub.create(EXECUTE_MH.bindTo(this), EXECUTE_DESC, arena);
      this.cloneStub = UpcallStub.create(CLONE_MH.bindTo(this), CLONE_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Allocate FFI_ExecutionPlan struct in Java arena memory
      this.ffiPlan = arena.allocate(FFI_EXECUTION_PLAN_SIZE, 8);
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_PROPERTIES, propertiesStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_CHILDREN, childrenStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_NAME, nameStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_EXECUTE, executeStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_CLONE, cloneStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_RELEASE, releaseStub.segment());
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_PRIVATE_DATA, MemorySegment.NULL);
      ffiPlan.set(ValueLayout.ADDRESS, OFFSET_LIBRARY_MARKER_ID, JAVA_MARKER_ID_FN);

    } catch (RuntimeException e) {
      throw e; // Preserve original exception (e.g., from plan.schema())
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create ExecutionPlanHandle", e);
    }
  }

  private static void validateSizes() {
    NativeUtil.validateSize(
        FFI_EXECUTION_PLAN_SIZE, DataFusionBindings.FFI_EXECUTION_PLAN_SIZE, "FFI_ExecutionPlan");
    NativeUtil.validateSize(
        FFI_PLAN_PROPERTIES_SIZE,
        DataFusionBindings.FFI_PLAN_PROPERTIES_SIZE,
        "FFI_PlanProperties");
    NativeUtil.validateSize(
        RVEC_PLAN_SIZE, DataFusionBindings.RVEC_PLAN_SIZE, "RVec<FFI_ExecutionPlan>");
    NativeUtil.validateSize(
        FFI_RESULT_STREAM_SIZE,
        DataFusionBindings.FFI_RESULT_STREAM_SIZE,
        "FFIResult<FFI_RecordBatchStream>");
    NativeUtil.validateSize(
        FFI_TASK_CONTEXT_SIZE, DataFusionBindings.FFI_TASK_CONTEXT_SIZE, "FFI_TaskContext");
  }

  /** Get the FFI_ExecutionPlan struct to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return ffiPlan;
  }

  /**
   * Copy the FFI_ExecutionPlan bytes into a destination buffer. Used by callers that need to write
   * the struct into Rust stack memory (e.g., scan callback's plan_out parameter).
   */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_EXECUTION_PLAN_SIZE).copyFrom(ffiPlan);
  }

  /**
   * Build FFI_PlanProperties eagerly during construction. This ensures errors from {@code
   * plan.schema()} or {@code plan.properties()} propagate immediately rather than producing null
   * function pointers that would cause a SIGSEGV when DataFusion tries to call them.
   */
  /**
   * Build FFI_PlanProperties from the plan's properties and schema. Exceptions from {@code
   * plan.schema()} and {@code plan.properties()} propagate directly so the original error message
   * is preserved through the FFI error chain.
   */
  private void buildProperties() {
    PlanProperties props = plan.properties();
    Schema schema = plan.schema();

    // Export schema to a temporary FFI_ArrowSchema
    MemorySegment schemaSegment = arena.allocate(ARROW_SCHEMA_SIZE, 8);
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, schema, null, ffiSchema);

      MemorySegment srcSchema =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      schemaSegment.copyFrom(srcSchema);

      // Clear release in source — schemaSegment now owns it
      srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }

    int emissionType =
        switch (props.emissionType()) {
          case INCREMENTAL -> 0;
          case FINAL -> 1;
          case BOTH -> 2;
        };
    int boundedness =
        switch (props.boundedness()) {
          case BOUNDED -> 0;
          case UNBOUNDED -> 1;
        };

    try {
      int result =
          (int)
              DataFusionBindings.CREATE_FFI_PLAN_PROPERTIES.invokeExact(
                  props.outputPartitioning(),
                  emissionType,
                  boundedness,
                  schemaSegment,
                  propertiesBuffer);
      if (result != 0) {
        throw new DataFusionException("Failed to create FFI_PlanProperties from Rust helper");
      }
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create FFI_PlanProperties", e);
    }
  }

  /**
   * Callback: properties. Returns the pre-built 64-byte FFI_PlanProperties struct.
   *
   * <p>The struct was built eagerly in the constructor via {@link #buildProperties()}.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getProperties(MemorySegment planRef) {
    return propertiesBuffer;
  }

  /**
   * Callback: children. Returns a 32-byte RVec&lt;FFI_ExecutionPlan&gt; (always empty — Java-backed
   * plans are leaf nodes).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getChildren(MemorySegment planRef) {
    childrenBuffer.fill((byte) 0);
    try {
      DataFusionBindings.CREATE_EMPTY_RVEC_PLAN.invokeExact(childrenBuffer);
    } catch (Throwable e) {
      // Best effort — return zeroed buffer
    }
    return childrenBuffer;
  }

  /** Callback: name. Returns an RString with the plan name. */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getName(MemorySegment planRef) {
    nameBuffer.fill((byte) 0);
    NativeUtil.writeRString("JavaBackedExecutionPlan", nameBuffer, 0, arena);
    return nameBuffer;
  }

  /**
   * Callback: execute. Returns a 40-byte FFIResult&lt;FFI_RecordBatchStream&gt;.
   *
   * <p>Layout: [RResult disc (1B), 7B pad, payload (32B)]
   *
   * <ul>
   *   <li>Success: [0, pad, FFI_RecordBatchStream (32B)]
   *   <li>Error: [1, pad, RString]
   * </ul>
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment execute(MemorySegment planRef, long partition, MemorySegment context) {
    executeResultBuffer.fill((byte) 0);
    try {
      RecordBatchReader reader = plan.execute((int) partition, allocator);

      // Create a handle that constructs FFI_RecordBatchStream
      RecordBatchReaderHandle readerHandle =
          new RecordBatchReaderHandle(reader, allocator, arena, fullStackTrace);

      // FFIResult::ROk(stream)
      executeResultBuffer.set(ValueLayout.JAVA_BYTE, RESULT_DISC_OFFSET, (byte) 0); // ROk

      // Copy the 32-byte FFI_RecordBatchStream into the payload area
      MemorySegment payload =
          executeResultBuffer.asSlice(RESULT_PAYLOAD_OFFSET, FFI_RECORD_BATCH_STREAM_SIZE);
      readerHandle.copyStructTo(payload);

      // Prevent GC while Rust holds pointers to the stream's upcall stubs
      readerHandles.add(readerHandle);

      return executeResultBuffer;
    } catch (Exception e) {
      // FFIResult::RErr(rstring)
      executeResultBuffer.fill((byte) 0);
      executeResultBuffer.set(ValueLayout.JAVA_BYTE, RESULT_DISC_OFFSET, (byte) 1); // RErr

      String errorMsg = fullStackTrace ? getStackTrace(e) : e.getMessage();
      if (errorMsg == null) {
        errorMsg = e.getClass().getName();
      }
      NativeUtil.writeRString(errorMsg, executeResultBuffer, RESULT_PAYLOAD_OFFSET, arena);

      return executeResultBuffer;
    }
  }

  /**
   * Callback: clone. Returns a 64-byte FFI_ExecutionPlan that is a byte-for-byte copy of the
   * original. All clones share the same Java upcall stubs and the same ExecutionPlanHandle
   * instance.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment clonePlan(MemorySegment planRef) {
    cloneBuffer.copyFrom(ffiPlan);
    return cloneBuffer;
  }

  /** Callback: release. Called by Rust when done with the plan (or a clone). */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment planRef) {
    // Cleanup happens when arena is closed.
    // This may be called multiple times (once per clone) — all no-ops.
  }

  @Override
  public void close() {
    // The FFI_ExecutionPlan struct lives in the arena.
    // The arena will clean up the upcall stubs.
  }

  private static String getStackTrace(Exception e) {
    java.io.StringWriter sw = new java.io.StringWriter();
    e.printStackTrace(new java.io.PrintWriter(sw));
    return sw.toString();
  }
}
