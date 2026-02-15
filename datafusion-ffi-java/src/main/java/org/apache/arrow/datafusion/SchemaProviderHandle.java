package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Internal FFI bridge for SchemaProvider.
 *
 * <p>This class constructs an {@code FFI_SchemaProvider} struct directly in Java arena memory. The
 * struct contains function pointers that Rust invokes via {@code ForeignSchemaProvider}.
 *
 * <p>Layout of FFI_SchemaProvider (264 bytes, align 8):
 *
 * <pre>
 * offset   0: owner_name                    (40 bytes) — ROption&lt;RString&gt;::RNone
 * offset  40: table_names fn ptr            (ADDRESS)
 * offset  48: table fn ptr                  (ADDRESS)
 * offset  56: register_table fn ptr         (ADDRESS) — Rust stub
 * offset  64: deregister_table fn ptr       (ADDRESS) — Rust stub
 * offset  72: table_exist fn ptr            (ADDRESS)
 * offset  80: logical_codec                 (144 bytes) — via Rust helper
 * offset 224: clone fn ptr                  (ADDRESS)
 * offset 232: release fn ptr                (ADDRESS)
 * offset 240: version fn ptr                (ADDRESS)
 * offset 248: private_data ptr              (ADDRESS) — NULL
 * offset 256: library_marker_id fn ptr      (ADDRESS)
 * </pre>
 */
final class SchemaProviderHandle implements TraitHandle {
  // ======== Downcall handles for size validation ========

  private static final MethodHandle FFI_SCHEMA_PROVIDER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_schema_provider_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle TABLE_FUTURE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_table_future_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle ROPTION_TABLE_PROVIDER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_roption_table_provider_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  // ======== Downcall handles for Rust helpers (schema_provider.rs) ========

  private static final MethodHandle CREATE_TABLE_FUTURE_SOME =
      NativeUtil.downcall(
          "datafusion_schema_create_table_future_some",
          FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_TABLE_FUTURE_NONE =
      NativeUtil.downcall(
          "datafusion_schema_create_table_future_none",
          FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_TABLE_FUTURE_ERROR =
      NativeUtil.downcall(
          "datafusion_schema_create_table_future_error",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS, // ptr (UTF-8 bytes)
              ValueLayout.JAVA_LONG, // len
              ValueLayout.ADDRESS // out
              ));

  // ======== Rust symbol lookups (schema_provider.rs only) ========

  private static final MemorySegment STUB_REGISTER_TABLE =
      NativeLoader.get().find("datafusion_schema_provider_stub_register_table").orElseThrow();

  private static final MemorySegment STUB_DEREGISTER_TABLE =
      NativeLoader.get().find("datafusion_schema_provider_stub_deregister_table").orElseThrow();

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_schema_provider_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_schema_provider_release").orElseThrow();

  // ======== Size constants ========

  private static final long LOGICAL_CODEC_SIZE = NativeUtil.getLogicalCodecSize();
  private static final long ROPTION_RSTRING_SIZE = NativeUtil.getRoptionRstringSize();
  private static final long FFI_TABLE_PROVIDER_SIZE = TableProviderHandle.FFI_SIZE;

  /** Struct size exposed for parent Handle classes. */
  static final long FFI_SIZE = querySize(FFI_SCHEMA_PROVIDER_SIZE_MH);

  private static long querySize(MethodHandle mh) {
    try {
      return (long) mh.invokeExact();
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Struct layouts ========

  // FFI_SchemaProvider: owner_name(40) + 5 fn ptrs(40) + codec(144) + 5 fields(40) = 264 bytes
  private static final StructLayout FFI_SCHEMA_PROVIDER_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(ROPTION_RSTRING_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("owner_name"),
          ValueLayout.ADDRESS.withName("table_names"),
          ValueLayout.ADDRESS.withName("table"),
          ValueLayout.ADDRESS.withName("register_table"),
          ValueLayout.ADDRESS.withName("deregister_table"),
          ValueLayout.ADDRESS.withName("table_exist"),
          MemoryLayout.sequenceLayout(LOGICAL_CODEC_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("logical_codec"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_TABLE_NAMES =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("table_names"));
  private static final VarHandle VH_TABLE =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("table"));
  private static final VarHandle VH_REGISTER_TABLE =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("register_table"));
  private static final VarHandle VH_DEREGISTER_TABLE =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("deregister_table"));
  private static final VarHandle VH_TABLE_EXIST =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("table_exist"));
  private static final VarHandle VH_CLONE =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_SCHEMA_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  private static final long OWNER_NAME_OFFSET =
      FFI_SCHEMA_PROVIDER_LAYOUT.byteOffset(PathElement.groupElement("owner_name"));
  private static final long LOGICAL_CODEC_OFFSET =
      FFI_SCHEMA_PROVIDER_LAYOUT.byteOffset(PathElement.groupElement("logical_codec"));

  // RVec<RString> (32 bytes, opaque — table_names return type)
  private static final StructLayout RVEC_RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRvecRstringSize() / 8, ValueLayout.JAVA_LONG));

  // RString (32 bytes, opaque — callback parameter)
  private static final StructLayout RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRStringSize() / 8, ValueLayout.JAVA_LONG));

  // FfiFuture<FFIResult<ROption<FFI_TableProvider>>> (24 bytes — table callback return)
  private static final StructLayout TABLE_FUTURE_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(3, ValueLayout.JAVA_LONG));

  // ======== Callback descriptors ========

  // table_names: (&Self) -> RVec<RString>
  private static final FunctionDescriptor TABLE_NAMES_DESC =
      FunctionDescriptor.of(RVEC_RSTRING_LAYOUT, ValueLayout.ADDRESS);

  // table: (&Self, RString) -> FfiFuture<FFIResult<ROption<FFI_TableProvider>>>
  private static final FunctionDescriptor TABLE_DESC =
      FunctionDescriptor.of(TABLE_FUTURE_LAYOUT, ValueLayout.ADDRESS, RSTRING_LAYOUT);

  // table_exist: (&Self, RString) -> bool
  private static final FunctionDescriptor TABLE_EXIST_DESC =
      FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, RSTRING_LAYOUT);

  // ======== Static MethodHandles ========

  private static final MethodHandle TABLE_NAMES_MH = initTableNamesMethodHandle();
  private static final MethodHandle TABLE_MH = initTableMethodHandle();
  private static final MethodHandle TABLE_EXIST_MH = initTableExistMethodHandle();

  private static MethodHandle initTableNamesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "getTableNames",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initTableMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "getTable",
              MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initTableExistMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "tableExists",
              MethodType.methodType(boolean.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Runtime size validation ========

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_SCHEMA_PROVIDER_LAYOUT.byteSize(), FFI_SCHEMA_PROVIDER_SIZE_MH, "FFI_SchemaProvider");
    NativeUtil.validateSize(
        TABLE_FUTURE_LAYOUT.byteSize(),
        TABLE_FUTURE_SIZE_MH,
        "FfiFuture<FFIResult<ROption<FFI_TableProvider>>>");
  }

  // ======== Instance fields ========

  private final Arena arena;
  private final SchemaProvider provider;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiProvider;

  // Pre-allocated return buffers
  private final MemorySegment tableNamesBuffer;
  private final MemorySegment tableBuffer;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub tableNamesStub;
  private final UpcallStub tableStub;
  private final UpcallStub tableExistStub;

  // Keep references to child handles to prevent GC while Rust holds pointers
  private TableProviderHandle lastTableHandle;

  SchemaProviderHandle(
      SchemaProvider provider, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Pre-allocate return buffers
      this.tableNamesBuffer = arena.allocate(RVEC_RSTRING_LAYOUT);
      this.tableBuffer = arena.allocate(TABLE_FUTURE_LAYOUT);

      // Create upcall stubs
      this.tableNamesStub = UpcallStub.create(TABLE_NAMES_MH.bindTo(this), TABLE_NAMES_DESC, arena);
      this.tableStub = UpcallStub.create(TABLE_MH.bindTo(this), TABLE_DESC, arena);
      this.tableExistStub = UpcallStub.create(TABLE_EXIST_MH.bindTo(this), TABLE_EXIST_DESC, arena);

      // Allocate FFI_SchemaProvider struct in Java arena memory
      this.ffiProvider = arena.allocate(FFI_SCHEMA_PROVIDER_LAYOUT);

      // Initialize owner_name as ROption<RString>::RNone via Rust helper
      MemorySegment ownerNameSlice = ffiProvider.asSlice(OWNER_NAME_OFFSET, ROPTION_RSTRING_SIZE);
      NativeUtil.CREATE_ROPTION_RSTRING_NONE.invokeExact(ownerNameSlice);

      // Populate function pointer fields
      VH_TABLE_NAMES.set(ffiProvider, 0L, tableNamesStub.segment());
      VH_TABLE.set(ffiProvider, 0L, tableStub.segment());
      VH_REGISTER_TABLE.set(ffiProvider, 0L, STUB_REGISTER_TABLE);
      VH_DEREGISTER_TABLE.set(ffiProvider, 0L, STUB_DEREGISTER_TABLE);
      VH_TABLE_EXIST.set(ffiProvider, 0L, tableExistStub.segment());
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
      throw new DataFusionException("Failed to create SchemaProviderHandle", e);
    }
  }

  /** Get the FFI_SchemaProvider struct to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return ffiProvider;
  }

  /**
   * Copy the FFI_SchemaProvider bytes into a destination buffer. Used when embedding in larger
   * structs (e.g., ROption payload).
   */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_SCHEMA_PROVIDER_LAYOUT.byteSize()).copyFrom(ffiProvider);
  }

  // ======== Helper: read string from RString ========

  private static String readRString(MemorySegment rstring) {
    MemorySegment reinterpreted = rstring.reinterpret(RSTRING_LAYOUT.byteSize());
    MemorySegment dataPtr = reinterpreted.get(ValueLayout.ADDRESS, 0);
    long len = reinterpreted.get(ValueLayout.JAVA_LONG, 8);
    byte[] bytes = dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  // ======== Callbacks ========

  /**
   * Callback: table_names. Returns an RVec<RString> containing all table names. Called via upcall
   * stub.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getTableNames(MemorySegment selfRef) {
    tableNamesBuffer.fill((byte) 0);
    try {
      List<String> names = provider.tableNames();

      if (names.isEmpty()) {
        NativeUtil.CREATE_RVEC_RSTRING.invokeExact(
            MemorySegment.NULL, MemorySegment.NULL, 0L, tableNamesBuffer);
        return tableNamesBuffer;
      }

      // Build parallel arrays of UTF-8 byte pointers and lengths
      MemorySegment ptrsArray = arena.allocate(ValueLayout.ADDRESS, names.size());
      MemorySegment lensArray = arena.allocate(ValueLayout.JAVA_LONG, names.size());

      for (int i = 0; i < names.size(); i++) {
        byte[] utf8 = names.get(i).getBytes(StandardCharsets.UTF_8);
        MemorySegment utf8Segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, utf8);
        ptrsArray.setAtIndex(ValueLayout.ADDRESS, i, utf8Segment);
        lensArray.setAtIndex(ValueLayout.JAVA_LONG, i, utf8.length);
      }

      NativeUtil.CREATE_RVEC_RSTRING.invokeExact(
          ptrsArray, lensArray, (long) names.size(), tableNamesBuffer);
      return tableNamesBuffer;
    } catch (Throwable e) {
      // Best effort — return zeroed buffer (empty RVec)
      return tableNamesBuffer;
    }
  }

  /**
   * Callback: table. Takes an RString name, returns FfiFuture&lt;FFIResult&lt;ROption&lt;
   * FFI_TableProvider&gt;&gt;&gt;. Called via upcall stub.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getTable(MemorySegment selfRef, MemorySegment nameRString) {
    tableBuffer.fill((byte) 0);
    try {
      String tableName = readRString(nameRString);
      Optional<TableProvider> table = provider.table(tableName);

      if (table.isEmpty()) {
        // Create FfiFuture wrapping ROption::RNone
        CREATE_TABLE_FUTURE_NONE.invokeExact(tableBuffer);
        return tableBuffer;
      }

      // Create a TableProviderHandle and embed its struct in the future
      TableProviderHandle tableHandle =
          new TableProviderHandle(table.get(), allocator, arena, fullStackTrace);
      this.lastTableHandle = tableHandle; // Prevent GC

      // Allocate a temporary buffer for the FFI_TableProvider struct
      MemorySegment tableStruct = arena.allocate(FFI_TABLE_PROVIDER_SIZE, 8);
      tableHandle.copyStructTo(tableStruct);

      // Create FfiFuture wrapping ROption::RSome(table_provider)
      CREATE_TABLE_FUTURE_SOME.invokeExact(tableStruct, tableBuffer);
      return tableBuffer;
    } catch (Throwable e) {
      // Create FfiFuture wrapping error
      tableBuffer.fill((byte) 0);
      String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
      byte[] errorBytes = errorMsg.getBytes(StandardCharsets.UTF_8);
      MemorySegment errorSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, errorBytes);
      try {
        CREATE_TABLE_FUTURE_ERROR.invokeExact(errorSegment, (long) errorBytes.length, tableBuffer);
      } catch (Throwable ignored) {
        // Best effort
      }
      return tableBuffer;
    }
  }

  /** Callback: table_exist. Takes an RString name, returns bool. Called via upcall stub. */
  @SuppressWarnings("unused") // Called via upcall stub
  boolean tableExists(MemorySegment selfRef, MemorySegment nameRString) {
    try {
      String tableName = readRString(nameRString);
      return provider.tableExists(tableName);
    } catch (Throwable e) {
      return false;
    }
  }

  @Override
  public void close() {
    // The FFI_SchemaProvider struct lives in the arena.
    // The arena will clean up the upcall stubs.
  }
}
