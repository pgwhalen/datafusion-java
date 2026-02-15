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
 * Internal FFI bridge for CatalogProvider.
 *
 * <p>This class constructs an {@code FFI_CatalogProvider} struct directly in Java arena memory. The
 * struct contains function pointers that Rust invokes via {@code ForeignCatalogProvider}.
 *
 * <p>Layout of FFI_CatalogProvider (216 bytes, align 8):
 *
 * <pre>
 * offset   0: schema_names fn ptr           (ADDRESS)
 * offset   8: schema fn ptr                 (ADDRESS)
 * offset  16: register_schema fn ptr        (ADDRESS) — Rust stub
 * offset  24: deregister_schema fn ptr      (ADDRESS) — Rust stub
 * offset  32: logical_codec                 (144 bytes) — via Rust helper
 * offset 176: clone fn ptr                  (ADDRESS) — Rust symbol
 * offset 184: release fn ptr                (ADDRESS) — Rust symbol
 * offset 192: version fn ptr                (ADDRESS) — Rust symbol
 * offset 200: private_data ptr              (ADDRESS) — NULL
 * offset 208: library_marker_id fn ptr      (ADDRESS) — Rust symbol
 * </pre>
 */
final class CatalogProviderHandle implements TraitHandle {
  // ======== Downcall handles for size validation ========

  private static final MethodHandle FFI_CATALOG_PROVIDER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_catalog_provider_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle ROPTION_SCHEMA_PROVIDER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_roption_schema_provider_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  // ======== Rust symbol lookups (catalog_provider.rs only) ========

  private static final MemorySegment STUB_REGISTER_SCHEMA =
      NativeLoader.get().find("datafusion_catalog_provider_stub_register_schema").orElseThrow();

  private static final MemorySegment STUB_DEREGISTER_SCHEMA =
      NativeLoader.get().find("datafusion_catalog_provider_stub_deregister_schema").orElseThrow();

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_catalog_provider_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_catalog_provider_release").orElseThrow();

  // ======== Size constants ========

  private static final long LOGICAL_CODEC_SIZE = NativeUtil.getLogicalCodecSize();
  private static final long FFI_SCHEMA_PROVIDER_SIZE = SchemaProviderHandle.FFI_SIZE;
  private static final long ROPTION_SCHEMA_PROVIDER_SIZE =
      querySize(ROPTION_SCHEMA_PROVIDER_SIZE_MH);

  /** Struct size exposed for parent Handle classes. */
  static final long FFI_SIZE = querySize(FFI_CATALOG_PROVIDER_SIZE_MH);

  private static long querySize(MethodHandle mh) {
    try {
      return (long) mh.invokeExact();
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Struct layouts ========

  // FFI_CatalogProvider (216 bytes)
  private static final StructLayout FFI_CATALOG_PROVIDER_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("schema_names"),
          ValueLayout.ADDRESS.withName("schema"),
          ValueLayout.ADDRESS.withName("register_schema"),
          ValueLayout.ADDRESS.withName("deregister_schema"),
          MemoryLayout.sequenceLayout(LOGICAL_CODEC_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("logical_codec"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_SCHEMA_NAMES =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("schema_names"));
  private static final VarHandle VH_SCHEMA =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("schema"));
  private static final VarHandle VH_REGISTER_SCHEMA =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("register_schema"));
  private static final VarHandle VH_DEREGISTER_SCHEMA =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("deregister_schema"));
  private static final VarHandle VH_CLONE =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_CATALOG_PROVIDER_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  private static final long LOGICAL_CODEC_OFFSET =
      FFI_CATALOG_PROVIDER_LAYOUT.byteOffset(PathElement.groupElement("logical_codec"));

  // RVec<RString> (32 bytes, opaque — schema_names return type)
  private static final StructLayout RVEC_RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRvecRstringSize() / 8, ValueLayout.JAVA_LONG));

  // RString (32 bytes, opaque — schema callback parameter)
  private static final StructLayout RSTRING_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(NativeUtil.getRStringSize() / 8, ValueLayout.JAVA_LONG));

  // ROption<FFI_SchemaProvider> (8 + FFI_SchemaProvider size)
  // Discriminant modeled as JAVA_LONG (FFM linker rejects non-natural padding)
  private static final StructLayout ROPTION_SCHEMA_PROVIDER_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(FFI_SCHEMA_PROVIDER_SIZE / 8, ValueLayout.JAVA_LONG)
              .withName("payload"));

  private static final long ROPTION_SCHEMA_PAYLOAD_OFFSET =
      ROPTION_SCHEMA_PROVIDER_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // ======== Callback descriptors ========

  // schema_names: (&Self) -> RVec<RString>
  private static final FunctionDescriptor SCHEMA_NAMES_DESC =
      FunctionDescriptor.of(RVEC_RSTRING_LAYOUT, ValueLayout.ADDRESS);

  // schema: (&Self, RString) -> ROption<FFI_SchemaProvider>
  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(ROPTION_SCHEMA_PROVIDER_LAYOUT, ValueLayout.ADDRESS, RSTRING_LAYOUT);

  // ======== Static MethodHandles ========

  private static final MethodHandle SCHEMA_NAMES_MH = initSchemaNamesMethodHandle();
  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();

  private static MethodHandle initSchemaNamesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              CatalogProviderHandle.class,
              "getSchemaNames",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              CatalogProviderHandle.class,
              "getSchema",
              MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Runtime size validation ========

  private static volatile boolean sizesValidated = false;

  private static void validateSizes() {
    NativeUtil.validateSize(
        FFI_CATALOG_PROVIDER_LAYOUT.byteSize(),
        FFI_CATALOG_PROVIDER_SIZE_MH,
        "FFI_CatalogProvider");
    NativeUtil.validateSize(
        ROPTION_SCHEMA_PROVIDER_LAYOUT.byteSize(),
        ROPTION_SCHEMA_PROVIDER_SIZE_MH,
        "ROption<FFI_SchemaProvider>");
  }

  // ======== Instance fields ========

  private final Arena arena;
  private final CatalogProvider provider;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiProvider;

  // Pre-allocated return buffers
  private final MemorySegment schemaNamesBuffer;
  private final MemorySegment schemaBuffer;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub schemaNamesStub;
  private final UpcallStub schemaStub;

  // Keep references to child handles to prevent GC while Rust holds pointers
  private SchemaProviderHandle lastSchemaHandle;

  CatalogProviderHandle(
      CatalogProvider provider, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Validate sizes against Rust at first use
      if (!sizesValidated) {
        validateSizes();
        sizesValidated = true;
      }

      // Pre-allocate return buffers
      this.schemaNamesBuffer = arena.allocate(RVEC_RSTRING_LAYOUT);
      this.schemaBuffer = arena.allocate(ROPTION_SCHEMA_PROVIDER_LAYOUT);

      // Create upcall stubs
      this.schemaNamesStub =
          UpcallStub.create(SCHEMA_NAMES_MH.bindTo(this), SCHEMA_NAMES_DESC, arena);
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);

      // Allocate FFI_CatalogProvider struct in Java arena memory
      this.ffiProvider = arena.allocate(FFI_CATALOG_PROVIDER_LAYOUT);

      // Populate function pointer fields
      VH_SCHEMA_NAMES.set(ffiProvider, 0L, schemaNamesStub.segment());
      VH_SCHEMA.set(ffiProvider, 0L, schemaStub.segment());
      VH_REGISTER_SCHEMA.set(ffiProvider, 0L, STUB_REGISTER_SCHEMA);
      VH_DEREGISTER_SCHEMA.set(ffiProvider, 0L, STUB_DEREGISTER_SCHEMA);
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
      throw new DataFusionException("Failed to create CatalogProviderHandle", e);
    }
  }

  /** Get the FFI_CatalogProvider struct to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return ffiProvider;
  }

  /**
   * Copy the FFI_CatalogProvider bytes into a destination buffer. Used when embedding in larger
   * structs (e.g., ROption payload).
   */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_CATALOG_PROVIDER_LAYOUT.byteSize()).copyFrom(ffiProvider);
  }

  // ======== Callbacks ========

  /**
   * Callback: schema_names. Returns an RVec<RString> containing all schema names. Called via upcall
   * stub.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getSchemaNames(MemorySegment selfRef) {
    schemaNamesBuffer.fill((byte) 0);
    try {
      List<String> names = provider.schemaNames();

      if (names.isEmpty()) {
        NativeUtil.CREATE_RVEC_RSTRING.invokeExact(
            MemorySegment.NULL, MemorySegment.NULL, 0L, schemaNamesBuffer);
        return schemaNamesBuffer;
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
          ptrsArray, lensArray, (long) names.size(), schemaNamesBuffer);
      return schemaNamesBuffer;
    } catch (Throwable e) {
      // Best effort — return zeroed buffer (empty RVec)
      return schemaNamesBuffer;
    }
  }

  /**
   * Callback: schema. Takes an RString name, returns ROption<FFI_SchemaProvider>. Called via upcall
   * stub.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getSchema(MemorySegment selfRef, MemorySegment nameRString) {
    schemaBuffer.fill((byte) 0);
    try {
      // Read the string from the RString parameter
      // RString = RVec<u8>: { buf: *mut u8, length: usize, capacity: usize, vtable: *const }
      MemorySegment nameRStringReinterpreted = nameRString.reinterpret(RSTRING_LAYOUT.byteSize());
      MemorySegment dataPtr = nameRStringReinterpreted.get(ValueLayout.ADDRESS, 0);
      long len = nameRStringReinterpreted.get(ValueLayout.JAVA_LONG, 8);
      byte[] bytes = dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
      String schemaName = new String(bytes, StandardCharsets.UTF_8);

      Optional<SchemaProvider> schema = provider.schema(schemaName);

      if (schema.isEmpty()) {
        // ROption::RNone — discriminant = 1
        schemaBuffer.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
        return schemaBuffer;
      }

      // ROption::RSome — discriminant = 0
      schemaBuffer.set(ValueLayout.JAVA_BYTE, 0, (byte) 0);

      // Create a SchemaProviderHandle and copy its struct into the payload
      SchemaProviderHandle schemaHandle =
          new SchemaProviderHandle(schema.get(), allocator, arena, fullStackTrace);
      this.lastSchemaHandle = schemaHandle; // Prevent GC

      MemorySegment payload =
          schemaBuffer.asSlice(ROPTION_SCHEMA_PAYLOAD_OFFSET, FFI_SCHEMA_PROVIDER_SIZE);
      schemaHandle.copyStructTo(payload);

      return schemaBuffer;
    } catch (Throwable e) {
      // Return RNone on error
      schemaBuffer.fill((byte) 0);
      schemaBuffer.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
      return schemaBuffer;
    }
  }

  @Override
  public void close() {
    // The FFI_CatalogProvider struct lives in the arena.
    // The arena will clean up the upcall stubs.
  }
}
