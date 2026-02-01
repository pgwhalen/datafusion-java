package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
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

  private final Arena arena;
  private final CatalogProvider provider;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final MemorySegment schemaNamesStub;
  private final MemorySegment schemaStub;
  private final MemorySegment releaseStub;

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

      // Create upcall stubs for the callback functions
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Linker linker = DataFusionBindings.getLinker();

      // schema_names_fn: (java_object, names_out, names_len_out, error_out) -> i32
      MethodHandle schemaNamesHandle =
          lookup.bind(
              this,
              "getSchemaNames",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor schemaNamesDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.schemaNamesStub = linker.upcallStub(schemaNamesHandle, schemaNamesDesc, arena);

      // schema_fn: (java_object, name, schema_out, error_out) -> i32
      MethodHandle schemaHandle =
          lookup.bind(
              this,
              "getSchema",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor schemaDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.schemaStub = linker.upcallStub(schemaHandle, schemaDesc, arena);

      // release_fn: (java_object) -> void
      MethodHandle releaseHandle =
          lookup.bind(this, "release", MethodType.methodType(void.class, MemorySegment.class));
      FunctionDescriptor releaseDesc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
      this.releaseStub = linker.upcallStub(releaseHandle, releaseDesc, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(32); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_NAMES_FN, schemaNamesStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_FN, schemaStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub);

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

      if (names.isEmpty()) {
        namesOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        namesLenOut.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, 0L);
        return 0;
      }

      // Allocate array of string pointers (address size * count)
      MemorySegment stringArray = arena.allocate(ValueLayout.ADDRESS.byteSize() * names.size());

      for (int i = 0; i < names.size(); i++) {
        MemorySegment nameSegment = arena.allocateUtf8String(names.get(i));
        stringArray.setAtIndex(ValueLayout.ADDRESS, i, nameSegment);
      }

      namesOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, stringArray);
      namesLenOut.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, (long) names.size());

      return 0;
    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
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
      String schemaName = name.reinterpret(1024).getUtf8String(0);
      SchemaProvider schema = provider.schema(schemaName);

      if (schema == null) {
        schemaOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        return 0;
      }

      // Create a handle for the schema
      SchemaProviderHandle schemaHandle = new SchemaProviderHandle(schema, allocator, arena);

      // Return the callback struct pointer
      MemorySegment schemaCallbacks = schemaHandle.getCallbackStruct();
      schemaOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, schemaCallbacks);

      return 0;
    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
    }
  }

  /** Callback: Release the provider. */
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
