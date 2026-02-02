package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.datafusion.CatalogProvider;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.SchemaProvider;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Internal FFI bridge for CatalogProvider.
 *
 * <p>This class creates upcall stubs that Rust can invoke to access a Java {@link CatalogProvider}.
 * It manages the lifecycle of the callback struct and upcall stubs.
 */
final class CatalogProviderHandle implements AutoCloseable {
  // Callback struct field offsets
  // struct JavaCatalogProviderCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   schema_names_fn: fn,              // offset 8
  //   schema_fn: fn,                    // offset 16
  //   release_fn: fn,                   // offset 24
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_SCHEMA_NAMES_FN = 8;
  private static final long OFFSET_SCHEMA_FN = 16;
  private static final long OFFSET_RELEASE_FN = 24;

  // Static FunctionDescriptors - define the FFI signatures once at class load
  private static final FunctionDescriptor SCHEMA_NAMES_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle SCHEMA_NAMES_MH = initSchemaNamesMethodHandle();
  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initSchemaNamesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              CatalogProviderHandle.class,
              "getSchemaNames",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
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
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
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
              CatalogProviderHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final CatalogProvider provider;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub schemaNamesStub;
  private final UpcallStub schemaStub;
  private final UpcallStub releaseStub;

  CatalogProviderHandle(CatalogProvider provider, BufferAllocator allocator, Arena arena) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_CATALOG_PROVIDER_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate CatalogProvider callbacks");
      }

      // Create upcall stubs - only the stubs are per-instance
      this.schemaNamesStub =
          UpcallStub.create(SCHEMA_NAMES_MH.bindTo(this), SCHEMA_NAMES_DESC, arena);
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(32); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_NAMES_FN, schemaNamesStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_FN, schemaStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create CatalogProviderHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Get schema names. */
  @SuppressWarnings("unused")
  int getSchemaNames(
      MemorySegment javaObject,
      MemorySegment namesOut,
      MemorySegment namesLenOut,
      MemorySegment errorOut) {
    try {
      List<String> names = provider.schemaNames();
      PointerOut namesOutPtr = new PointerOut(namesOut);
      LongOut namesLenOutVal = new LongOut(namesLenOut);

      if (names.isEmpty()) {
        namesOutPtr.setNull();
        namesLenOutVal.set(0L);
        return 0;
      }

      // Allocate array of string pointers (address size * count)
      MemorySegment stringArray = arena.allocate(ValueLayout.ADDRESS.byteSize() * names.size());

      for (int i = 0; i < names.size(); i++) {
        MemorySegment nameSegment = arena.allocateFrom(names.get(i));
        stringArray.setAtIndex(ValueLayout.ADDRESS, i, nameSegment);
      }

      namesOutPtr.set(stringArray);
      namesLenOutVal.set(names.size());

      return 0;
    } catch (Exception e) {
      return ErrorOut.fromException(errorOut, e, arena);
    }
  }

  /** Callback: Get a schema by name. */
  @SuppressWarnings("unused")
  int getSchema(
      MemorySegment javaObject,
      MemorySegment name,
      MemorySegment schemaOut,
      MemorySegment errorOut) {
    try {
      String schemaName = new NativeString(name).value();
      Optional<SchemaProvider> schema = provider.schema(schemaName);
      PointerOut schemaOutPtr = new PointerOut(schemaOut);

      if (schema.isEmpty()) {
        schemaOutPtr.setNull();
        return 0;
      }

      // Create a handle for the schema
      SchemaProviderHandle schemaHandle = new SchemaProviderHandle(schema.get(), allocator, arena);

      // Return the callback struct pointer
      schemaOutPtr.set(schemaHandle.getCallbackStruct());

      return 0;
    } catch (Exception e) {
      return ErrorOut.fromException(errorOut, e, arena);
    }
  }

  /** Callback: Release the provider. */
  @SuppressWarnings("unused")
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // Callback struct freed by Rust
  }
}
