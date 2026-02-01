package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Internal FFI bridge for SchemaProvider.
 *
 * <p>This class creates upcall stubs that Rust can invoke to access a Java {@link SchemaProvider}.
 * It manages the lifecycle of the callback struct and upcall stubs.
 */
final class SchemaProviderHandle implements AutoCloseable {
  // Callback struct field offsets
  // struct JavaSchemaProviderCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   table_names_fn: fn,               // offset 8
  //   table_fn: fn,                     // offset 16
  //   table_exists_fn: fn,              // offset 24
  //   release_fn: fn,                   // offset 32
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_TABLE_NAMES_FN = 8;
  private static final long OFFSET_TABLE_FN = 16;
  private static final long OFFSET_TABLE_EXISTS_FN = 24;
  private static final long OFFSET_RELEASE_FN = 32;

  private final Arena arena;
  private final SchemaProvider provider;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final MemorySegment tableNamesStub;
  private final MemorySegment tableStub;
  private final MemorySegment tableExistsStub;
  private final MemorySegment releaseStub;

  SchemaProviderHandle(SchemaProvider provider, BufferAllocator allocator, Arena arena) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_SCHEMA_PROVIDER_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate SchemaProvider callbacks");
      }

      // Create upcall stubs for the callback functions
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Linker linker = DataFusionBindings.getLinker();

      // table_names_fn: (java_object, names_out, names_len_out, error_out) -> i32
      MethodHandle tableNamesHandle =
          lookup.bind(
              this,
              "getTableNames",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor tableNamesDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.tableNamesStub = linker.upcallStub(tableNamesHandle, tableNamesDesc, arena);

      // table_fn: (java_object, name, table_out, error_out) -> i32
      MethodHandle tableHandle =
          lookup.bind(
              this,
              "getTable",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor tableDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.tableStub = linker.upcallStub(tableHandle, tableDesc, arena);

      // table_exists_fn: (java_object, name) -> i32
      MethodHandle tableExistsHandle =
          lookup.bind(
              this,
              "tableExists",
              MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class));
      FunctionDescriptor tableExistsDesc =
          FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
      this.tableExistsStub = linker.upcallStub(tableExistsHandle, tableExistsDesc, arena);

      // release_fn: (java_object) -> void
      MethodHandle releaseHandle =
          lookup.bind(this, "release", MethodType.methodType(void.class, MemorySegment.class));
      FunctionDescriptor releaseDesc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
      this.releaseStub = linker.upcallStub(releaseHandle, releaseDesc, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(40); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_NAMES_FN, tableNamesStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_FN, tableStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_EXISTS_FN, tableExistsStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub);

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create SchemaProviderHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Get table names. */
  @SuppressWarnings("unused")
  int getTableNames(
      MemorySegment javaObject,
      MemorySegment namesOut,
      MemorySegment namesLenOut,
      MemorySegment errorOut) {
    try {
      List<String> names = provider.tableNames();

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

  /** Callback: Get a table by name. */
  @SuppressWarnings("unused")
  int getTable(
      MemorySegment javaObject,
      MemorySegment name,
      MemorySegment tableOut,
      MemorySegment errorOut) {
    try {
      String tableName = name.reinterpret(1024).getUtf8String(0);
      TableProvider table = provider.table(tableName);

      if (table == null) {
        tableOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        return 0;
      }

      // Create a handle for the table
      TableProviderHandle tableHandle = new TableProviderHandle(table, allocator, arena);

      // Return the callback struct pointer
      MemorySegment tableCallbacks = tableHandle.getCallbackStruct();
      tableOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, tableCallbacks);

      return 0;
    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
    }
  }

  /** Callback: Check if a table exists. */
  @SuppressWarnings("unused")
  int tableExists(MemorySegment javaObject, MemorySegment name) {
    try {
      String tableName = name.reinterpret(1024).getUtf8String(0);
      return provider.tableExists(tableName) ? 1 : 0;
    } catch (Exception e) {
      return 0;
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
