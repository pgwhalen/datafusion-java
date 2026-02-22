package org.apache.arrow.datafusion.graaltest;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BOOLEAN;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeForeignAccess;

/**
 * GraalVM native-image Feature that registers all FFM FunctionDescriptor signatures used by
 * datafusion-ffi-java. Without this, downcall/upcall stubs cannot be created at runtime.
 *
 * <p>Each unique FunctionDescriptor signature (ignoring {@code .withName()} annotations) is
 * registered exactly once. The signatures were extracted from all {@code *Ffi} and {@code *Handle}
 * classes in the datafusion-ffi-java module.
 *
 * <p>Struct layout sizes are hardcoded from known ABI-stable types. These sizes are validated at
 * test time by {@code FfiSizeValidationTest} in the main module. If upstream Rust struct sizes
 * change, the native image test will fail with a descriptor mismatch error.
 */
class ForeignRegistrationFeature implements Feature {

  // ======== Struct layout sizes (bytes) — hardcoded from known ABI-stable types ========
  // These match the values returned by Rust size helpers at runtime.

  /** RString: ptr + len + cap + vtable = 4 × 8 = 32 bytes. */
  private static final int RSTRING_BYTES = 32;

  /** RVec&lt;T&gt;: ptr + len + cap + vtable = 4 × 8 = 32 bytes. */
  private static final int RVEC_BYTES = 32;

  /** FFI_ArrowSchema (WrappedSchema): 9 fields × 8 = 72 bytes. */
  private static final int ARROW_SCHEMA_BYTES = 72;

  /**
   * FFI_SchemaProvider: owner_name(40) + 5 fn ptrs(40) + logical_codec(144) + 5 fields(40) = 264
   * bytes.
   */
  private static final int FFI_SCHEMA_PROVIDER_BYTES = 264;

  /**
   * FFI_SessionRef: 10 fn ptrs(80) + FFI_LogicalExtensionCodec(144) + 5 fields(40) = 264 bytes.
   */
  private static final int FFI_SESSION_REF_BYTES = 264;

  /**
   * FFIResult&lt;RVec&lt;FFI_TableProviderFilterPushDown&gt;&gt;: disc(8) + max(RVec=32, RString=32)
   * = 40 bytes.
   */
  private static final int FILTER_PUSHDOWN_RESULT_BYTES = 40;

  @Override
  public void duringSetup(DuringSetupAccess access) {
    registerDowncalls();
    registerUpcalls();
  }

  /** Register all unique downcall FunctionDescriptor signatures. */
  private static void registerDowncalls() {
    // () -> long — size helpers (many)
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_LONG));

    // (address) -> long — STRING_LEN
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_LONG, ADDRESS));

    // (address) -> void — destroys, free, create_noop, create_empty_rvec, etc.
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.ofVoid(ADDRESS));

    // () -> address — RUNTIME_CREATE, CONTEXT_CREATE, CREATE_SESSION_HANDLE
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS));

    // (address, long, address) -> void — CREATE_RSTRING, *_ERROR helpers,
    // CREATE_FILTER_PUSHDOWN_OK, CREATE_RVEC_WRAPPED_SCHEMA
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG, ADDRESS));

    // (address, address, long, address) -> void — CREATE_RVEC_RSTRING
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.ofVoid(ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));

    // (address, address, address, long, address) -> address — CONTEXT_CREATE_WITH_CONFIG
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));

    // (address x5) -> int — REGISTER_RECORD_BATCH, STREAM_NEXT
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));

    // (address x4) -> address — CONTEXT_SQL
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));

    // (address x2) -> address — CONTEXT_STATE
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS));

    // (address x4) -> int — REGISTER_CATALOG
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS));

    // (address x3) -> address — EXECUTE_STREAM, GUARANTEE_ANALYZE,
    // SESSION_STATE_CREATE_LOGICAL_PLAN
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(ADDRESS, ADDRESS, ADDRESS, ADDRESS));

    // (address x3) -> int — SESSION_START_TIME, REGISTER_UDF, STREAM_SCHEMA
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));

    // CONTEXT_REGISTER_LISTING_TABLE — the largest downcall
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(
            JAVA_INT,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            JAVA_LONG,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            JAVA_INT,
            JAVA_LONG,
            ADDRESS));

    // (address, address) -> void — CREATE_TABLE_FUTURE_SOME, CREATE_SCAN_FUTURE_OK
    RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.ofVoid(ADDRESS, ADDRESS));

    // (address, long, address, address) -> address — SESSION_CREATE_PHYSICAL_EXPR_FROM_PROTO
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));

    // GUARANTEE_GET_INFO — 10 params
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(
            JAVA_INT,
            ADDRESS,
            JAVA_LONG,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS));

    // GUARANTEE_GET_SPAN — 8 params
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(
            JAVA_INT,
            ADDRESS,
            JAVA_LONG,
            JAVA_LONG,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS,
            ADDRESS));

    // GUARANTEE_GET_LITERAL — 6 params
    RuntimeForeignAccess.registerForDowncall(
        FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG, ADDRESS, ADDRESS, ADDRESS));
  }

  /**
   * Register all upcall FunctionDescriptor signatures for the full callback chain:
   * CatalogProvider → SchemaProvider → TableProvider → ExecutionPlan → RecordBatchReader.
   *
   * <p>Struct layouts are constructed to be structurally equivalent to those in the Handle classes.
   * Sizes are hardcoded because the native library is not loaded at image build time.
   */
  private static void registerUpcalls() {
    // ── Reusable struct layouts ──────────────────────────────────────────────

    // RString / RVec<T> — 32 bytes, opaque
    StructLayout struct32 = opaqueStruct(RVEC_BYTES);

    // FfiFuture<T> — 24 bytes
    StructLayout struct24 = opaqueStruct(24);

    // WrappedSchema (FFI_ArrowSchema) / FFI_TaskContext — 72 bytes
    StructLayout struct72 = opaqueStruct(ARROW_SCHEMA_BYTES);

    // ROption<usize> — 16 bytes
    StructLayout struct16 = opaqueStruct(16);

    // FFI_SessionRef — 264 bytes
    StructLayout sessionRef = opaqueStruct(FFI_SESSION_REF_BYTES);

    // ROption<FFI_SchemaProvider> — disc(8) + FFI_SchemaProvider(264) = 272 bytes
    StructLayout roptionSchemaProvider =
        MemoryLayout.structLayout(
            JAVA_LONG,
            MemoryLayout.sequenceLayout(FFI_SCHEMA_PROVIDER_BYTES / 8, JAVA_LONG));

    // FFIResult<FFI_RecordBatchStream> / ROption<RVec<sort>> — disc(8) + payload(32) = 40 bytes
    StructLayout struct40disc =
        MemoryLayout.structLayout(JAVA_LONG, MemoryLayout.sequenceLayout(4, JAVA_LONG));

    // FFIResult<RVec<FFI_TableProviderFilterPushDown>> — 40 bytes (opaque)
    StructLayout filterPushdownResult = opaqueStruct(FILTER_PUSHDOWN_RESULT_BYTES);

    // FFI_ExecutionPlan / FFI_PlanProperties — 8 ADDRESS fields = 64 bytes
    StructLayout struct64addr =
        MemoryLayout.structLayout(ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS,
            ADDRESS);

    // FFI_Partitioning — disc(i32) + pad(4) + partition_count(i64) + remaining(32) = 48 bytes
    StructLayout partitioning =
        MemoryLayout.structLayout(
            JAVA_INT, MemoryLayout.paddingLayout(4), JAVA_LONG,
            MemoryLayout.sequenceLayout(4, JAVA_LONG));

    // FFI_Boundedness — disc(i32) + requires_infinite_memory(bool) + pad(3) = 8 bytes
    StructLayout boundedness =
        MemoryLayout.structLayout(JAVA_INT, JAVA_BYTE, MemoryLayout.paddingLayout(3));

    // PollResult: FfiPoll<ROption<RResult<WrappedArray, RString>>> — 176 bytes
    // 3 discriminants (each 8B) + payload (19 × 8B)
    StructLayout pollResult =
        MemoryLayout.structLayout(
            JAVA_LONG, JAVA_LONG, JAVA_LONG, MemoryLayout.sequenceLayout(19, JAVA_LONG));

    // ── Simple upcall descriptors (no struct layouts) ──────────────────────

    // (address) -> void — RELEASE_DESC (multiple Handle classes)
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.ofVoid(ADDRESS));

    // (address) -> int — TABLE_TYPE_DESC, PROPS_EMISSION_TYPE_DESC
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(JAVA_INT, ADDRESS));

    // ── CatalogProviderHandle descriptors ──────────────────────────────────

    // schema_names: (&self) -> RVec<RString> [32B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(struct32, ADDRESS));

    // schema: (&self, RString) -> ROption<FFI_SchemaProvider> [272B]
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(roptionSchemaProvider, ADDRESS, struct32));

    // ── SchemaProviderHandle descriptors ────────────────────────────────────

    // table_names: (&self) -> RVec<RString> [32B] — same as schema_names, already registered

    // table: (&self, RString) -> FfiFuture<FFIResult<ROption<FFI_TableProvider>>> [24B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(struct24, ADDRESS, struct32));

    // table_exist: (&self, RString) -> bool
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(JAVA_BOOLEAN, ADDRESS, struct32));

    // ── TableProviderHandle descriptors ─────────────────────────────────────

    // schema: (&self) -> WrappedSchema [72B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(struct72, ADDRESS));

    // table_type: (&self) -> int — already registered

    // supports_filters_pushdown: (&self, RVec<u8>) -> FFIResult<RVec<FilterPushDown>> [40B]
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(filterPushdownResult, ADDRESS, struct32));

    // scan: (&self, SessionRef, RVec<usize>, RVec<u8>, ROption<usize>)
    //       -> FfiFuture<FFIResult<FFI_ExecutionPlan>> [24B]
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(struct24, ADDRESS, sessionRef, struct32, struct32, struct16));

    // ── ExecutionPlanHandle descriptors (FFI_ExecutionPlan callbacks) ────────

    // properties: (&self) -> FFI_PlanProperties [64B, 8×ADDRESS]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(struct64addr, ADDRESS));

    // children: (&self) -> RVec<FFI_ExecutionPlan> [32B] — same shape as schema_names

    // name: (&self) -> RString [32B] — same shape as schema_names

    // execute: (&self, long partition, FFI_TaskContext) -> FFIResult<FFI_RecordBatchStream> [40B]
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(struct40disc, ADDRESS, JAVA_LONG, struct72));

    // clone: (&self) -> FFI_ExecutionPlan [64B, 8×ADDRESS] — same shape as properties

    // ── ExecutionPlanHandle descriptors (FFI_PlanProperties callbacks) ───────

    // output_partitioning: (&self) -> FFI_Partitioning [48B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(partitioning, ADDRESS));

    // emission_type: (&self) -> int — already registered

    // boundedness: (&self) -> FFI_Boundedness [8B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(boundedness, ADDRESS));

    // output_ordering: (&self) -> ROption<RVec<FFI_PhysicalSortExpr>> [40B]
    RuntimeForeignAccess.registerForUpcall(FunctionDescriptor.of(struct40disc, ADDRESS));

    // schema: (&self) -> WrappedSchema [72B] — same shape as TableProvider schema

    // ── RecordBatchReaderHandle descriptors ──────────────────────────────────

    // poll_next: (&self, &cx) -> FfiPoll<ROption<RResult<WrappedArray, RString>>> [176B]
    RuntimeForeignAccess.registerForUpcall(
        FunctionDescriptor.of(pollResult, ADDRESS, ADDRESS));

    // schema: (&self) -> WrappedSchema [72B] — same shape as above

    // release: (&self) -> void — already registered
  }

  /** Create an opaque struct layout of the given byte size as a sequence of JAVA_LONG. */
  private static StructLayout opaqueStruct(int bytes) {
    if (bytes % 8 != 0) {
      throw new IllegalArgumentException("Size must be a multiple of 8: " + bytes);
    }
    return MemoryLayout.structLayout(MemoryLayout.sequenceLayout(bytes / 8, JAVA_LONG));
  }
}
