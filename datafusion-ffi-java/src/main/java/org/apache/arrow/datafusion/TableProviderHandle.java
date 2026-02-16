package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for TableProvider.
 *
 * <p>This class constructs an {@code FFI_TableProvider} struct directly in Java arena memory. The
 * struct contains function pointers that Rust invokes via {@code ForeignTableProvider}.
 *
 * <p>Layout of FFI_TableProvider (224 bytes, align 8):
 *
 * <pre>
 * offset   0: schema fn ptr                 (ADDRESS)
 * offset   8: scan fn ptr                   (ADDRESS)
 * offset  16: table_type fn ptr             (ADDRESS)
 * offset  24: supports_filters_pushdown     (ADDRESS) — upcall stub
 * offset  32: insert_into fn ptr            (ADDRESS) — Rust stub
 * offset  40: logical_codec                 (144 bytes) — via Rust helper
 * offset 184: clone fn ptr                  (ADDRESS)
 * offset 192: release fn ptr                (ADDRESS)
 * offset 200: version fn ptr                (ADDRESS)
 * offset 208: private_data ptr              (ADDRESS) — NULL
 * offset 216: library_marker_id fn ptr      (ADDRESS)
 * </pre>
 */
final class TableProviderHandle implements TraitHandle {
  // ======== Downcall handles for size validation (table_provider.rs) ========

  private static final MethodHandle FFI_TABLE_PROVIDER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_table_provider_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle SCAN_FUTURE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_scan_future_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle RVEC_USIZE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_rvec_usize_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle ROPTION_USIZE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_roption_usize_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle SESSION_REF_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_session_ref_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle TABLE_TYPE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_table_type_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle WRAPPED_SCHEMA_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_wrapped_schema_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FILTER_PUSHDOWN_RESULT_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_filter_pushdown_result_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  // ======== Downcall handles for Rust helpers (table_provider.rs) ========

  private static final MethodHandle CREATE_SCAN_FUTURE_OK =
      NativeUtil.downcall(
          "datafusion_table_create_scan_future_ok",
          FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_SCAN_FUTURE_ERROR =
      NativeUtil.downcall(
          "datafusion_table_create_scan_future_error",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_SESSION_HANDLE =
      NativeUtil.downcall(
          "datafusion_table_create_session_handle", FunctionDescriptor.of(ValueLayout.ADDRESS));

  private static final MethodHandle FREE_SESSION_HANDLE =
      NativeUtil.downcall(
          "datafusion_table_free_session_handle", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_FILTER_PUSHDOWN_OK =
      NativeUtil.downcall(
          "datafusion_table_create_filter_pushdown_ok",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS.withName("discriminants"),
              ValueLayout.JAVA_LONG.withName("count"),
              ValueLayout.ADDRESS.withName("out")));

  private static final MethodHandle CREATE_FILTER_PUSHDOWN_ERROR =
      NativeUtil.downcall(
          "datafusion_table_create_filter_pushdown_error",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS.withName("ptr"),
              ValueLayout.JAVA_LONG.withName("len"),
              ValueLayout.ADDRESS.withName("out")));

  // ======== Rust symbol lookups (table_provider.rs only) ========

  private static final MemorySegment STUB_INSERT_INTO =
      NativeLoader.get().find("datafusion_table_provider_stub_insert_into").orElseThrow();

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_table_provider_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_table_provider_release").orElseThrow();

  // ======== Size constants ========

  private static final long LOGICAL_CODEC_SIZE = NativeUtil.getLogicalCodecSize();
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long SESSION_REF_SIZE = querySize(SESSION_REF_SIZE_MH);

  /**
   * A valid empty struct format string ("+s\0") for error fallback. If schema() throws, we return
   * this as the format so Arrow's TryFrom assertion doesn't panic. With release=NULL the Drop is a
   * no-op, and the upstream datafusion-ffi catch_df_schema_error converts it to Schema::empty().
   */
  private static final MemorySegment EMPTY_STRUCT_FORMAT =
      Arena.global().allocateFrom(ValueLayout.JAVA_BYTE, new byte[] {'+', 's', 0});

  /** Struct size exposed for parent Handle classes. */
  static final long FFI_SIZE = querySize(FFI_TABLE_PROVIDER_SIZE_MH);

  private static long querySize(MethodHandle mh) {
    try {
      return (long) mh.invokeExact();
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Struct layouts ========

  // FFI_TableProvider: 5 fn ptrs/Option(40) + codec(144) + 5 fields(40) = 224 bytes
  private static final StructLayout FFI_TABLE_PROVIDER_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("schema"),
          ValueLayout.ADDRESS.withName("scan"),
          ValueLayout.ADDRESS.withName("table_type"),
          ValueLayout.ADDRESS.withName("supports_filters_pushdown"),
          ValueLayout.ADDRESS.withName("insert_into"),
          MemoryLayout.sequenceLayout(LOGICAL_CODEC_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("logical_codec"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_SCHEMA =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("schema"));
  private static final VarHandle VH_SCAN =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("scan"));
  private static final VarHandle VH_TABLE_TYPE =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("table_type"));
  private static final VarHandle VH_SUPPORTS_FILTERS =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("supports_filters_pushdown"));
  private static final VarHandle VH_INSERT_INTO =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("insert_into"));
  private static final VarHandle VH_CLONE =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_TABLE_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  private static final long LOGICAL_CODEC_OFFSET =
      FFI_TABLE_PROVIDER_LAYOUT.byteOffset(PathElement.groupElement("logical_codec"));

  // WrappedSchema = FFI_ArrowSchema (72 bytes) — schema callback return
  private static final StructLayout WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(ARROW_SCHEMA_SIZE / 8, ValueLayout.JAVA_LONG));

  // FFI_TableType (4 bytes) — table_type callback return
  private static final long FFI_TABLE_TYPE_SIZE = 4;

  // FFI_SessionRef (opaque, received as param in scan callback)
  private static final StructLayout SESSION_REF_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(SESSION_REF_SIZE / 8, ValueLayout.JAVA_LONG));

  // RVec<usize> (32 bytes — projections param in scan callback)
  private static final StructLayout RVEC_USIZE_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG));

  // RVec<u8> (32 bytes — filters param in scan callback, same size as any RVec)
  private static final StructLayout RVEC_U8_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG));

  // ROption<usize> (16 bytes — limit param in scan callback)
  private static final StructLayout ROPTION_USIZE_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(2, ValueLayout.JAVA_LONG));

  // FfiFuture<FFIResult<FFI_ExecutionPlan>> (24 bytes — scan callback return)
  private static final StructLayout SCAN_FUTURE_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(3, ValueLayout.JAVA_LONG));

  // FFIResult<RVec<FFI_TableProviderFilterPushDown>> — supports_filters_pushdown callback return
  private static final long FILTER_PUSHDOWN_RESULT_SIZE = querySize(FILTER_PUSHDOWN_RESULT_SIZE_MH);

  private static final StructLayout FILTER_PUSHDOWN_RESULT_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(FILTER_PUSHDOWN_RESULT_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("ffi_result"));

  // ======== Callback descriptors ========

  // schema: (&Self) -> WrappedSchema (72 bytes)
  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(WRAPPED_SCHEMA_LAYOUT, ValueLayout.ADDRESS);

  // table_type: (&Self) -> FFI_TableType (i32)
  private static final FunctionDescriptor TABLE_TYPE_DESC =
      FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);

  // supports_filters_pushdown: (&Self, RVec<u8>) ->
  // FFIResult<RVec<FFI_TableProviderFilterPushDown>>
  private static final FunctionDescriptor SUPPORTS_FILTERS_DESC =
      FunctionDescriptor.of(FILTER_PUSHDOWN_RESULT_LAYOUT, ValueLayout.ADDRESS, RVEC_U8_LAYOUT);

  // scan: (&Self, FFI_SessionRef, RVec<usize>, RVec<u8>, ROption<usize>)
  //       -> FfiFuture<FFIResult<FFI_ExecutionPlan>>
  private static final FunctionDescriptor SCAN_DESC =
      FunctionDescriptor.of(
          SCAN_FUTURE_LAYOUT,
          ValueLayout.ADDRESS,
          SESSION_REF_LAYOUT,
          RVEC_USIZE_LAYOUT,
          RVEC_U8_LAYOUT,
          ROPTION_USIZE_LAYOUT);

  // ======== Static MethodHandles ========

  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();
  private static final MethodHandle TABLE_TYPE_MH = initTableTypeMethodHandle();
  private static final MethodHandle SCAN_MH = initScanMethodHandle();
  private static final MethodHandle SUPPORTS_FILTERS_MH = initSupportsFiltersMethodHandle();

  private static MethodHandle initSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              TableProviderHandle.class,
              "getSchema",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initTableTypeMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              TableProviderHandle.class,
              "getTableType",
              MethodType.methodType(int.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initScanMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              TableProviderHandle.class,
              "scan",
              MethodType.methodType(
                  MemorySegment.class,
                  MemorySegment.class, // selfRef
                  MemorySegment.class, // session
                  MemorySegment.class, // projections
                  MemorySegment.class, // filters
                  MemorySegment.class // limit
                  ));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initSupportsFiltersMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              TableProviderHandle.class,
              "supportsFiltersPushdown",
              MethodType.methodType(
                  MemorySegment.class,
                  MemorySegment.class, // selfRef
                  MemorySegment.class // filtersSerialized (RVec<u8>)
                  ));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Runtime size validation ========

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_TABLE_PROVIDER_LAYOUT.byteSize(), FFI_TABLE_PROVIDER_SIZE_MH, "FFI_TableProvider");
    NativeUtil.validateSize(
        SCAN_FUTURE_LAYOUT.byteSize(),
        SCAN_FUTURE_SIZE_MH,
        "FfiFuture<FFIResult<FFI_ExecutionPlan>>");
    NativeUtil.validateSize(RVEC_USIZE_LAYOUT.byteSize(), RVEC_USIZE_SIZE_MH, "RVec<usize>");
    NativeUtil.validateSize(
        ROPTION_USIZE_LAYOUT.byteSize(), ROPTION_USIZE_SIZE_MH, "ROption<usize>");
    NativeUtil.validateSize(SESSION_REF_SIZE, SESSION_REF_SIZE_MH, "FFI_SessionRef");
    NativeUtil.validateSize(FFI_TABLE_TYPE_SIZE, TABLE_TYPE_SIZE_MH, "FFI_TableType");
    NativeUtil.validateSize(ARROW_SCHEMA_SIZE, WRAPPED_SCHEMA_SIZE_MH, "WrappedSchema");
    NativeUtil.validateSize(
        FILTER_PUSHDOWN_RESULT_LAYOUT.byteSize(),
        FILTER_PUSHDOWN_RESULT_SIZE_MH,
        "FFIResult<RVec<FFI_TableProviderFilterPushDown>>");
  }

  // ======== Instance fields ========

  private final Arena arena;
  private final TableProvider provider;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiProvider;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub schemaStub;
  private final UpcallStub tableTypeStub;
  private final UpcallStub scanStub;
  private final UpcallStub supportsFiltersStub;

  // Keep references to child handles to prevent GC while Rust holds pointers
  private ExecutionPlanHandle lastPlanHandle;

  TableProviderHandle(
      TableProvider provider, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Create upcall stubs
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.tableTypeStub = UpcallStub.create(TABLE_TYPE_MH.bindTo(this), TABLE_TYPE_DESC, arena);
      this.scanStub = UpcallStub.create(SCAN_MH.bindTo(this), SCAN_DESC, arena);
      this.supportsFiltersStub =
          UpcallStub.create(SUPPORTS_FILTERS_MH.bindTo(this), SUPPORTS_FILTERS_DESC, arena);

      // Allocate FFI_TableProvider struct in Java arena memory
      this.ffiProvider = arena.allocate(FFI_TABLE_PROVIDER_LAYOUT);

      // Populate function pointer fields
      VH_SCHEMA.set(ffiProvider, 0L, schemaStub.segment());
      VH_SCAN.set(ffiProvider, 0L, scanStub.segment());
      VH_TABLE_TYPE.set(ffiProvider, 0L, tableTypeStub.segment());
      VH_SUPPORTS_FILTERS.set(ffiProvider, 0L, supportsFiltersStub.segment());
      VH_INSERT_INTO.set(ffiProvider, 0L, STUB_INSERT_INTO);
      VH_CLONE.set(ffiProvider, 0L, CLONE_FN);
      VH_RELEASE.set(ffiProvider, 0L, RELEASE_FN);
      VH_VERSION.set(ffiProvider, 0L, NativeUtil.VERSION_FN);
      VH_PRIVATE_DATA.set(ffiProvider, 0L, MemorySegment.NULL);
      VH_LIBRARY_MARKER_ID.set(ffiProvider, 0L, NativeUtil.JAVA_MARKER_ID_FN);

      // Initialize logical_codec via Rust helper
      MemorySegment codecSlice = ffiProvider.asSlice(LOGICAL_CODEC_OFFSET, LOGICAL_CODEC_SIZE);
      NativeUtil.CREATE_NOOP_LOGICAL_CODEC.invokeExact(codecSlice);

    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create TableProviderHandle", e);
    }
  }

  /** Get the FFI_TableProvider struct to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return ffiProvider;
  }

  /**
   * Copy the FFI_TableProvider bytes into a destination buffer. Used when embedding in larger
   * structs.
   */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_TABLE_PROVIDER_LAYOUT.byteSize()).copyFrom(ffiProvider);
  }

  // ======== Callbacks ========

  /** Callback: schema. Returns a 72-byte WrappedSchema (= FFI_ArrowSchema). */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getSchema(MemorySegment selfRef) {
    MemorySegment buffer = arena.allocate(WRAPPED_SCHEMA_LAYOUT);
    try {
      Schema schema = provider.schema();
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);

        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        buffer.copyFrom(srcSchema);

        // Clear release in source — dest (Rust copy) has it
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      }
      return buffer;
    } catch (Throwable e) {
      // Cannot propagate errors from schema() — upstream FFI_TableProvider has no error channel.
      // Return a valid empty struct schema (format="+s", release=NULL makes Drop a no-op).
      buffer.set(ValueLayout.ADDRESS, 0, EMPTY_STRUCT_FORMAT);
      return buffer;
    }
  }

  /** Callback: table_type. Returns FFI_TableType as i32. */
  @SuppressWarnings("unused") // Called via upcall stub
  int getTableType(MemorySegment selfRef) {
    return switch (provider.tableType()) {
      case BASE -> 0;
      case VIEW -> 1;
      case TEMPORARY -> 2;
    };
  }

  /**
   * Callback: scan. Takes session, projections, filters, limit. Returns
   * FfiFuture&lt;FFIResult&lt;FFI_ExecutionPlan&gt;&gt;.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment scan(
      MemorySegment selfRef,
      MemorySegment session,
      MemorySegment projections,
      MemorySegment filters,
      MemorySegment limit) {
    MemorySegment buffer = arena.allocate(SCAN_FUTURE_LAYOUT);
    try {
      // Read projections from RVec<usize>
      // RVec layout: { buf: *mut T, length: usize, capacity: usize, vtable: *const }
      MemorySegment projReinterpreted = projections.reinterpret(RVEC_USIZE_LAYOUT.byteSize());
      MemorySegment projBuf = projReinterpreted.get(ValueLayout.ADDRESS, 0);
      long projLen = projReinterpreted.get(ValueLayout.JAVA_LONG, 8);

      List<Integer> projectionList;
      if (projLen > 0 && !projBuf.equals(MemorySegment.NULL)) {
        projectionList = new ArrayList<>((int) projLen);
        MemorySegment projData = projBuf.reinterpret(projLen * 8);
        for (int i = 0; i < projLen; i++) {
          projectionList.add((int) projData.getAtIndex(ValueLayout.JAVA_LONG, i));
        }
      } else {
        projectionList = Collections.emptyList();
      }

      // Read limit from ROption<usize>
      // ROption layout: { disc: u8, pad(7), payload: usize }
      MemorySegment limitReinterpreted = limit.reinterpret(ROPTION_USIZE_LAYOUT.byteSize());
      byte limitDisc = limitReinterpreted.get(ValueLayout.JAVA_BYTE, 0);
      Long limitValue = null;
      if (limitDisc == 0) {
        // RSome
        limitValue = limitReinterpreted.get(ValueLayout.JAVA_LONG, 8);
      }

      // Decode filters from RVec<u8> (proto-encoded LogicalExprList)
      MemorySegment filterReinterpreted = filters.reinterpret(RVEC_U8_LAYOUT.byteSize());
      MemorySegment filterBuf = filterReinterpreted.get(ValueLayout.ADDRESS, 0);
      long filterLen = filterReinterpreted.get(ValueLayout.JAVA_LONG, 8);

      List<Expr> filterExprs;
      if (filterLen > 0 && !filterBuf.equals(MemorySegment.NULL)) {
        byte[] filterBytes = filterBuf.reinterpret(filterLen).toArray(ValueLayout.JAVA_BYTE);
        filterExprs = ExprProtoConverter.fromProtoBytes(filterBytes);
      } else {
        filterExprs = Collections.emptyList();
      }

      // Create Session handle (uses default SessionState since FFI_SessionRef is pub(crate))
      MemorySegment sessionHandle = (MemorySegment) CREATE_SESSION_HANDLE.invokeExact();
      Session scanSession = new Session(new SessionFfi(sessionHandle), allocator);

      try {
        // Call the Java provider's scan method
        ExecutionPlan plan = provider.scan(scanSession, filterExprs, projectionList, limitValue);

        // Create an ExecutionPlanHandle and use it to build the return value
        ExecutionPlanHandle planHandle =
            new ExecutionPlanHandle(plan, allocator, arena, fullStackTrace);
        this.lastPlanHandle = planHandle; // Prevent GC

        // Allocate temporary buffer for FFI_ExecutionPlan and copy into it
        MemorySegment planStruct = arena.allocate(64, 8); // FFI_ExecutionPlan is 64 bytes
        planHandle.copyStructTo(planStruct);

        // Create FfiFuture wrapping the plan
        CREATE_SCAN_FUTURE_OK.invokeExact(planStruct, buffer);
        return buffer;
      } finally {
        // Free native handles
        FREE_SESSION_HANDLE.invokeExact(sessionHandle);
      }
    } catch (Throwable e) {
      // Create FfiFuture wrapping error
      buffer.fill((byte) 0);
      String errorMsg = Errors.getErrorMessage(e, fullStackTrace);
      byte[] errorBytes = errorMsg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      MemorySegment errorSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, errorBytes);
      try {
        CREATE_SCAN_FUTURE_ERROR.invokeExact(errorSegment, (long) errorBytes.length, buffer);
      } catch (Throwable ignored) {
        // Best effort
      }
      return buffer;
    }
  }

  /**
   * Callback: supports_filters_pushdown. Takes serialized filters (RVec&lt;u8&gt;). Returns
   * FFIResult&lt;RVec&lt;FFI_TableProviderFilterPushDown&gt;&gt;.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment supportsFiltersPushdown(MemorySegment selfRef, MemorySegment filtersSerialized) {
    MemorySegment buffer = arena.allocate(FILTER_PUSHDOWN_RESULT_LAYOUT);
    try {
      // Read filter bytes from RVec<u8>
      MemorySegment filterReinterpreted = filtersSerialized.reinterpret(RVEC_U8_LAYOUT.byteSize());
      MemorySegment filterBuf = filterReinterpreted.get(ValueLayout.ADDRESS, 0);
      long filterLen = filterReinterpreted.get(ValueLayout.JAVA_LONG, 8);

      List<Expr> filterExprs;
      if (filterLen > 0 && !filterBuf.equals(MemorySegment.NULL)) {
        byte[] filterBytes = filterBuf.reinterpret(filterLen).toArray(ValueLayout.JAVA_BYTE);
        filterExprs = ExprProtoConverter.fromProtoBytes(filterBytes);
      } else {
        filterExprs = Collections.emptyList();
      }

      // Call Java provider
      List<FilterPushDown> pushdowns = provider.supportsFiltersPushdown(filterExprs);

      // Convert to int discriminants
      MemorySegment discriminants = arena.allocate(ValueLayout.JAVA_INT, pushdowns.size());
      int idx = 0;
      for (FilterPushDown pd : pushdowns) {
        int disc =
            switch (pd) {
              case UNSUPPORTED -> 0;
              case INEXACT -> 1;
              case EXACT -> 2;
            };
        discriminants.setAtIndex(ValueLayout.JAVA_INT, idx++, disc);
      }

      // Construct FFIResult via Rust helper
      CREATE_FILTER_PUSHDOWN_OK.invokeExact(discriminants, (long) pushdowns.size(), buffer);
      return buffer;
    } catch (Throwable e) {
      buffer.fill((byte) 0);
      String errorMsg = Errors.getErrorMessage(e, fullStackTrace);
      byte[] errorBytes = errorMsg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      MemorySegment errorSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, errorBytes);
      try {
        CREATE_FILTER_PUSHDOWN_ERROR.invokeExact(errorSegment, (long) errorBytes.length, buffer);
      } catch (Throwable ignored) {
        // Best effort
      }
      return buffer;
    }
  }

  @Override
  public void close() {
    // The FFI_TableProvider struct lives in the arena.
    // The arena will clean up the upcall stubs.
  }
}
