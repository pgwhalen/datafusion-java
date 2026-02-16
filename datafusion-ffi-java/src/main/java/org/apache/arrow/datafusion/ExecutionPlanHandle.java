package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
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
 *
 * <p>The {@code properties} callback returns a 64-byte {@code FFI_PlanProperties} struct, which is
 * also constructed directly in Java memory with its own 6 upcall stubs for the sub-callbacks
 * (output_partitioning, emission_type, boundedness, output_ordering, schema, release).
 */
final class ExecutionPlanHandle implements TraitHandle {
  // ======== Downcall handles for size validation ========

  private static final MethodHandle FFI_EXECUTION_PLAN_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_execution_plan_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_PLAN_PROPERTIES_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_plan_properties_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle RVEC_PLAN_SIZE =
      NativeUtil.downcall(
          "datafusion_rvec_plan_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_STREAM_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_result_stream_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_TASK_CONTEXT_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_task_context_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_PARTITIONING_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_partitioning_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_EMISSION_TYPE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_emission_type_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_BOUNDEDNESS_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_boundedness_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle ROPTION_RVEC_SORT_EXPR_SIZE =
      NativeUtil.downcall(
          "datafusion_roption_rvec_sort_expr_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle CREATE_EMPTY_RVEC_PLAN =
      NativeUtil.downcall(
          "datafusion_create_empty_rvec_plan",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS // out (RVec<FFI_ExecutionPlan> buffer)
              ));

  // Size of Arrow FFI structures (external constants, not defined by our layouts)
  private static final long ARROW_SCHEMA_SIZE = 72;

  // Scalar return type (not a struct)
  private static final long FFI_EMISSION_TYPE_SIZE = 4;

  // ======== Category A: Rich named struct layouts ========

  // FFI_ExecutionPlan struct layout (8 pointers = 64 bytes)
  private static final StructLayout FFI_EXECUTION_PLAN_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("properties"),
          ValueLayout.ADDRESS.withName("children"),
          ValueLayout.ADDRESS.withName("name"),
          ValueLayout.ADDRESS.withName("execute"),
          ValueLayout.ADDRESS.withName("clone_plan"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_PROPERTIES =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("properties"));
  private static final VarHandle VH_CHILDREN =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("children"));
  private static final VarHandle VH_NAME =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("name"));
  private static final VarHandle VH_EXECUTE =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("execute"));
  private static final VarHandle VH_CLONE =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("clone_plan"));
  private static final VarHandle VH_RELEASE =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_EXECUTION_PLAN_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // FFI_PlanProperties struct layout (8 pointers = 64 bytes)
  private static final StructLayout FFI_PLAN_PROPERTIES_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("output_partitioning"),
          ValueLayout.ADDRESS.withName("emission_type"),
          ValueLayout.ADDRESS.withName("boundedness"),
          ValueLayout.ADDRESS.withName("output_ordering"),
          ValueLayout.ADDRESS.withName("schema"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_PROPS_OUTPUT_PARTITIONING =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("output_partitioning"));
  private static final VarHandle VH_PROPS_EMISSION_TYPE =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("emission_type"));
  private static final VarHandle VH_PROPS_BOUNDEDNESS =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("boundedness"));
  private static final VarHandle VH_PROPS_OUTPUT_ORDERING =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("output_ordering"));
  private static final VarHandle VH_PROPS_SCHEMA =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("schema"));
  private static final VarHandle VH_PROPS_RELEASE =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_PROPS_PRIVATE_DATA =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_PROPS_LIBRARY_MARKER_ID =
      FFI_PLAN_PROPERTIES_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // FFI_Partitioning repr(C) enum (48 bytes): disc(i32) + pad(4) + partition_count(i64) + remaining
  private static final StructLayout FFI_PARTITIONING_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_INT.withName("discriminant"),
          MemoryLayout.paddingLayout(4),
          ValueLayout.JAVA_LONG.withName("partition_count"),
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("_remaining"));

  private static final VarHandle VH_PART_DISC =
      FFI_PARTITIONING_LAYOUT.varHandle(PathElement.groupElement("discriminant"));
  private static final VarHandle VH_PART_COUNT =
      FFI_PARTITIONING_LAYOUT.varHandle(PathElement.groupElement("partition_count"));

  // FFI_Boundedness repr(C) enum (8 bytes): disc(i32) + requires_infinite_memory(bool) + pad(3)
  private static final StructLayout FFI_BOUNDEDNESS_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_INT.withName("discriminant"),
          ValueLayout.JAVA_BYTE.withName("requires_infinite_memory"),
          MemoryLayout.paddingLayout(3));

  private static final VarHandle VH_BOUND_DISC =
      FFI_BOUNDEDNESS_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  // FFIResult<FFI_RecordBatchStream> (40 bytes): disc(u8) padded to 8B by abi_stable + payload(32B)
  // Discriminant modeled as JAVA_LONG (FFM linker rejects non-natural padding between members)
  private static final StructLayout FFI_RESULT_STREAM_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_RESULT_DISC =
      FFI_RESULT_STREAM_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  // Kept as numeric offset — used with asSlice() and writeRString(), not set/get
  private static final long RESULT_PAYLOAD_OFFSET =
      FFI_RESULT_STREAM_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // ROption<RVec<FFI_PhysicalSortExpr>> (40 bytes): disc(u8) padded to 8B by abi_stable + payload
  // Discriminant modeled as JAVA_LONG (FFM linker rejects non-natural padding between members)
  private static final StructLayout ROPTION_SORT_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_SORT_DISC =
      ROPTION_SORT_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  // ======== Category B: Opaque layouts (abi_stable internals or passthrough) ========

  // RVec<FFI_ExecutionPlan> (opaque, abi_stable internal)
  private static final StructLayout RVEC_PLAN_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG)); // 32 bytes

  // RString (opaque, abi_stable internal)
  private static final StructLayout RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRStringSize() / 8, ValueLayout.JAVA_LONG));

  // FFI_TaskContext (opaque, received as param, never field-accessed)
  private static final StructLayout TASK_CONTEXT_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(9, ValueLayout.JAVA_LONG)); // 72 bytes

  // WrappedSchema = FFI_ArrowSchema (opaque, bulk-copied)
  private static final StructLayout WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(ARROW_SCHEMA_SIZE / 8, ValueLayout.JAVA_LONG));

  // ======== FFI_ExecutionPlan callback descriptors ========

  // properties: (ADDRESS plan) -> STRUCT (FFI_PlanProperties, 64 bytes)
  private static final FunctionDescriptor PROPERTIES_DESC =
      FunctionDescriptor.of(FFI_PLAN_PROPERTIES_LAYOUT, ValueLayout.ADDRESS);

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
      FunctionDescriptor.of(FFI_EXECUTION_PLAN_LAYOUT, ValueLayout.ADDRESS);

  // release: (ADDRESS plan) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== FFI_PlanProperties callback descriptors ========

  // output_partitioning: (ADDRESS props) -> STRUCT (FFI_Partitioning, 48 bytes)
  private static final FunctionDescriptor PROPS_PARTITIONING_DESC =
      FunctionDescriptor.of(FFI_PARTITIONING_LAYOUT, ValueLayout.ADDRESS);

  // emission_type: (ADDRESS props) -> JAVA_INT (FFI_EmissionType, 4 bytes C enum)
  private static final FunctionDescriptor PROPS_EMISSION_TYPE_DESC =
      FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);

  // boundedness: (ADDRESS props) -> STRUCT (FFI_Boundedness, 8 bytes)
  private static final FunctionDescriptor PROPS_BOUNDEDNESS_DESC =
      FunctionDescriptor.of(FFI_BOUNDEDNESS_LAYOUT, ValueLayout.ADDRESS);

  // output_ordering: (ADDRESS props) -> STRUCT (ROption<RVec<FFI_PhysicalSortExpr>>, 40 bytes)
  private static final FunctionDescriptor PROPS_OUTPUT_ORDERING_DESC =
      FunctionDescriptor.of(ROPTION_SORT_LAYOUT, ValueLayout.ADDRESS);

  // schema: (ADDRESS props) -> STRUCT (WrappedSchema, 72 bytes)
  private static final FunctionDescriptor PROPS_SCHEMA_DESC =
      FunctionDescriptor.of(WRAPPED_SCHEMA_LAYOUT, ValueLayout.ADDRESS);

  // release: (ADDRESS props) -> void
  private static final FunctionDescriptor PROPS_RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== Static MethodHandles for FFI_ExecutionPlan callbacks ========

  private static final MethodHandle PROPERTIES_MH = initPropertiesMethodHandle();
  private static final MethodHandle CHILDREN_MH = initChildrenMethodHandle();
  private static final MethodHandle NAME_MH = initNameMethodHandle();
  private static final MethodHandle EXECUTE_MH = initExecuteMethodHandle();
  private static final MethodHandle CLONE_MH = initCloneMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  // ======== Static MethodHandles for FFI_PlanProperties callbacks ========

  private static final MethodHandle PROPS_PARTITIONING_MH = initPropsPartitioningMethodHandle();
  private static final MethodHandle PROPS_EMISSION_TYPE_MH = initPropsEmissionTypeMethodHandle();
  private static final MethodHandle PROPS_BOUNDEDNESS_MH = initPropsBoundednessMethodHandle();
  private static final MethodHandle PROPS_OUTPUT_ORDERING_MH =
      initPropsOutputOrderingMethodHandle();
  private static final MethodHandle PROPS_SCHEMA_MH = initPropsSchemaMethodHandle();
  private static final MethodHandle PROPS_RELEASE_MH = initPropsReleaseMethodHandle();

  // library_marker_id function pointer — looked up once at class load
  private static final MemorySegment JAVA_MARKER_ID_FN = initMarkerIdFnPtr();

  // ======== FFI_ExecutionPlan MethodHandle init ========

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

  // ======== FFI_PlanProperties MethodHandle init ========

  private static MethodHandle initPropsPartitioningMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getPartitioning",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropsEmissionTypeMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getEmissionType",
              MethodType.methodType(int.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropsBoundednessMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getBoundedness",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropsOutputOrderingMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getOutputOrdering",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropsSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "getPropsSchema",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initPropsReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ExecutionPlanHandle.class,
              "releaseProps",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MemorySegment initMarkerIdFnPtr() {
    return NativeUtil.JAVA_MARKER_ID_FN;
  }

  private final Arena arena;
  private final ExecutionPlan plan;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiPlan;

  // Schema cached eagerly in constructor: the properties schema callback has no error channel,
  // so exceptions from plan.schema() would crash the VM if called lazily during the upcall.
  // Caching here ensures errors propagate cleanly through the scan() callback's error path.
  private final Schema cachedSchema;

  // FFI_PlanProperties struct: built once with upcall stub pointers, only read in getProperties().
  // Concurrent reads are safe, so this is kept as a pre-allocated field.
  private final MemorySegment propertiesBuffer;

  // Keep references to upcall stubs to prevent GC (FFI_ExecutionPlan)
  private final UpcallStub propertiesStub;
  private final UpcallStub childrenStub;
  private final UpcallStub nameStub;
  private final UpcallStub executeStub;
  private final UpcallStub cloneStub;
  private final UpcallStub releaseStub;

  // Keep references to upcall stubs to prevent GC (FFI_PlanProperties)
  private final UpcallStub partitioningStub;
  private final UpcallStub emissionTypeStub;
  private final UpcallStub boundednessStub;
  private final UpcallStub outputOrderingStub;
  private final UpcallStub propsSchemaStub;
  private final UpcallStub propsReleaseStub;

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
      // Cache schema eagerly — see field comment for rationale
      this.cachedSchema = plan.schema();

      // Create upcall stubs for FFI_PlanProperties callbacks
      this.partitioningStub =
          UpcallStub.create(PROPS_PARTITIONING_MH.bindTo(this), PROPS_PARTITIONING_DESC, arena);
      this.emissionTypeStub =
          UpcallStub.create(PROPS_EMISSION_TYPE_MH.bindTo(this), PROPS_EMISSION_TYPE_DESC, arena);
      this.boundednessStub =
          UpcallStub.create(PROPS_BOUNDEDNESS_MH.bindTo(this), PROPS_BOUNDEDNESS_DESC, arena);
      this.outputOrderingStub =
          UpcallStub.create(
              PROPS_OUTPUT_ORDERING_MH.bindTo(this), PROPS_OUTPUT_ORDERING_DESC, arena);
      this.propsSchemaStub =
          UpcallStub.create(PROPS_SCHEMA_MH.bindTo(this), PROPS_SCHEMA_DESC, arena);
      this.propsReleaseStub =
          UpcallStub.create(PROPS_RELEASE_MH.bindTo(this), PROPS_RELEASE_DESC, arena);

      // Build FFI_PlanProperties struct in Java memory
      this.propertiesBuffer = arena.allocate(FFI_PLAN_PROPERTIES_LAYOUT);
      VH_PROPS_OUTPUT_PARTITIONING.set(propertiesBuffer, 0L, partitioningStub.segment());
      VH_PROPS_EMISSION_TYPE.set(propertiesBuffer, 0L, emissionTypeStub.segment());
      VH_PROPS_BOUNDEDNESS.set(propertiesBuffer, 0L, boundednessStub.segment());
      VH_PROPS_OUTPUT_ORDERING.set(propertiesBuffer, 0L, outputOrderingStub.segment());
      VH_PROPS_SCHEMA.set(propertiesBuffer, 0L, propsSchemaStub.segment());
      VH_PROPS_RELEASE.set(propertiesBuffer, 0L, propsReleaseStub.segment());
      VH_PROPS_PRIVATE_DATA.set(propertiesBuffer, 0L, MemorySegment.NULL);
      VH_PROPS_LIBRARY_MARKER_ID.set(propertiesBuffer, 0L, JAVA_MARKER_ID_FN);

      // Create upcall stubs for FFI_ExecutionPlan callbacks
      this.propertiesStub = UpcallStub.create(PROPERTIES_MH.bindTo(this), PROPERTIES_DESC, arena);
      this.childrenStub = UpcallStub.create(CHILDREN_MH.bindTo(this), CHILDREN_DESC, arena);
      this.nameStub = UpcallStub.create(NAME_MH.bindTo(this), NAME_DESC, arena);
      this.executeStub = UpcallStub.create(EXECUTE_MH.bindTo(this), EXECUTE_DESC, arena);
      this.cloneStub = UpcallStub.create(CLONE_MH.bindTo(this), CLONE_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Allocate FFI_ExecutionPlan struct in Java arena memory
      this.ffiPlan = arena.allocate(FFI_EXECUTION_PLAN_LAYOUT);
      VH_PROPERTIES.set(ffiPlan, 0L, propertiesStub.segment());
      VH_CHILDREN.set(ffiPlan, 0L, childrenStub.segment());
      VH_NAME.set(ffiPlan, 0L, nameStub.segment());
      VH_EXECUTE.set(ffiPlan, 0L, executeStub.segment());
      VH_CLONE.set(ffiPlan, 0L, cloneStub.segment());
      VH_RELEASE.set(ffiPlan, 0L, releaseStub.segment());
      VH_PRIVATE_DATA.set(ffiPlan, 0L, MemorySegment.NULL);
      VH_LIBRARY_MARKER_ID.set(ffiPlan, 0L, JAVA_MARKER_ID_FN);

    } catch (RuntimeException e) {
      throw e; // Preserve original exception (e.g., from plan.schema())
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create ExecutionPlanHandle", e);
    }
  }

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_EXECUTION_PLAN_LAYOUT.byteSize(), FFI_EXECUTION_PLAN_SIZE_MH, "FFI_ExecutionPlan");
    NativeUtil.validateSize(
        FFI_PLAN_PROPERTIES_LAYOUT.byteSize(), FFI_PLAN_PROPERTIES_SIZE_MH, "FFI_PlanProperties");
    NativeUtil.validateSize(RVEC_PLAN_LAYOUT.byteSize(), RVEC_PLAN_SIZE, "RVec<FFI_ExecutionPlan>");
    NativeUtil.validateSize(
        FFI_RESULT_STREAM_LAYOUT.byteSize(),
        FFI_RESULT_STREAM_SIZE,
        "FFIResult<FFI_RecordBatchStream>");
    NativeUtil.validateSize(
        TASK_CONTEXT_LAYOUT.byteSize(), FFI_TASK_CONTEXT_SIZE, "FFI_TaskContext");
    NativeUtil.validateSize(
        FFI_PARTITIONING_LAYOUT.byteSize(), FFI_PARTITIONING_SIZE, "FFI_Partitioning");
    NativeUtil.validateSize(FFI_EMISSION_TYPE_SIZE, FFI_EMISSION_TYPE_SIZE_MH, "FFI_EmissionType");
    NativeUtil.validateSize(
        FFI_BOUNDEDNESS_LAYOUT.byteSize(), FFI_BOUNDEDNESS_SIZE, "FFI_Boundedness");
    NativeUtil.validateSize(
        ROPTION_SORT_LAYOUT.byteSize(),
        ROPTION_RVEC_SORT_EXPR_SIZE,
        "ROption<RVec<FFI_PhysicalSortExpr>>");
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
    out.reinterpret(FFI_EXECUTION_PLAN_LAYOUT.byteSize()).copyFrom(ffiPlan);
  }

  // ======== FFI_PlanProperties callbacks ========

  /**
   * Callback: output_partitioning. Returns a 48-byte FFI_Partitioning struct (UnknownPartitioning
   * with the current partition count).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getPartitioning(MemorySegment selfPtr) {
    MemorySegment buffer = arena.allocate(FFI_PARTITIONING_LAYOUT);
    int partitionCount;
    try {
      partitionCount = plan.properties().outputPartitioning();
    } catch (Exception e) {
      partitionCount = 1; // safe default — no error channel in this callback
    }
    VH_PART_DISC.set(buffer, 0L, 2); // UnknownPartitioning
    VH_PART_COUNT.set(buffer, 0L, (long) partitionCount);
    return buffer;
  }

  /**
   * Callback: emission_type. Returns a 4-byte FFI_EmissionType C enum as JAVA_INT. 0=Incremental,
   * 1=Final, 2=Both.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  int getEmissionType(MemorySegment selfPtr) {
    EmissionType emissionType;
    try {
      emissionType = plan.properties().emissionType();
    } catch (Exception e) {
      emissionType = EmissionType.INCREMENTAL; // safe default — no error channel in this callback
    }
    return switch (emissionType) {
      case INCREMENTAL -> 0;
      case FINAL -> 1;
      case BOTH -> 2;
    };
  }

  /**
   * Callback: boundedness. Returns an 8-byte FFI_Boundedness struct. 0=Bounded, 1=Unbounded (with
   * requires_infinite_memory=false).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getBoundedness(MemorySegment selfPtr) {
    MemorySegment buffer = arena.allocate(FFI_BOUNDEDNESS_LAYOUT);
    Boundedness b;
    try {
      b = plan.properties().boundedness();
    } catch (Exception e) {
      b = Boundedness.BOUNDED; // safe default — no error channel in this callback
    }
    int boundedness =
        switch (b) {
          case BOUNDED -> 0;
          case UNBOUNDED -> 1;
        };
    VH_BOUND_DISC.set(buffer, 0L, boundedness);
    return buffer;
  }

  /**
   * Callback: output_ordering. Returns a 40-byte ROption (always RNone — no sort ordering support).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getOutputOrdering(MemorySegment selfPtr) {
    MemorySegment buffer = arena.allocate(ROPTION_SORT_LAYOUT);
    VH_SORT_DISC.set(buffer, 0L, 1L); // RNone
    return buffer;
  }

  /** Callback: schema. Returns a 72-byte WrappedSchema (= FFI_ArrowSchema) with fresh export. */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getPropsSchema(MemorySegment selfPtr) {
    MemorySegment buffer = arena.allocate(WRAPPED_SCHEMA_LAYOUT);
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, cachedSchema, null, ffiSchema);

      MemorySegment srcSchema =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      buffer.copyFrom(srcSchema);

      // Clear release in source — dest (Rust copy) has it
      srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }
    return buffer;
  }

  /** Callback: release (FFI_PlanProperties). No-op — arena owns the memory. */
  @SuppressWarnings("unused") // Called via upcall stub
  void releaseProps(MemorySegment selfPtr) {
    // No-op — cleanup happens when arena is closed
  }

  // ======== FFI_ExecutionPlan callbacks ========

  /**
   * Callback: properties. Returns the pre-built 64-byte FFI_PlanProperties struct, which itself
   * contains function pointers to the property callbacks above.
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
    MemorySegment buffer = arena.allocate(RVEC_PLAN_LAYOUT);
    try {
      CREATE_EMPTY_RVEC_PLAN.invokeExact(buffer);
    } catch (Throwable e) {
      // Best effort — return zeroed buffer
    }
    return buffer;
  }

  /** Callback: name. Returns an RString with the plan name. */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getName(MemorySegment planRef) {
    MemorySegment buffer = arena.allocate(RSTRING_LAYOUT);
    NativeUtil.writeRString("JavaBackedExecutionPlan", buffer, 0, arena);
    return buffer;
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
    MemorySegment buffer = arena.allocate(FFI_RESULT_STREAM_LAYOUT);
    try {
      RecordBatchReader reader = plan.execute((int) partition, allocator);

      // Create a handle that constructs FFI_RecordBatchStream
      RecordBatchReaderHandle readerHandle =
          new RecordBatchReaderHandle(reader, allocator, arena, fullStackTrace);

      // FFIResult::ROk(stream)
      VH_RESULT_DISC.set(buffer, 0L, 0L); // ROk

      // Copy the FFI_RecordBatchStream into the payload area
      MemorySegment payload =
          buffer.asSlice(RESULT_PAYLOAD_OFFSET, RecordBatchReaderHandle.streamStructSize());
      readerHandle.copyStructTo(payload);

      // Prevent GC while Rust holds pointers to the stream's upcall stubs
      readerHandles.add(readerHandle);

      return buffer;
    } catch (Exception e) {
      // FFIResult::RErr(rstring)
      buffer.fill((byte) 0);
      VH_RESULT_DISC.set(buffer, 0L, 1L); // RErr

      String errorMsg = Errors.getErrorMessage(e, fullStackTrace);
      NativeUtil.writeRString(errorMsg, buffer, RESULT_PAYLOAD_OFFSET, arena);

      return buffer;
    }
  }

  /**
   * Callback: clone. Returns a 64-byte FFI_ExecutionPlan that is a byte-for-byte copy of the
   * original. All clones share the same Java upcall stubs and the same ExecutionPlanHandle
   * instance.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment clonePlan(MemorySegment planRef) {
    MemorySegment buffer = arena.allocate(FFI_EXECUTION_PLAN_LAYOUT);
    buffer.copyFrom(ffiPlan);
    return buffer;
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
}
