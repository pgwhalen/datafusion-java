package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.SchemaProvider;
import org.apache.arrow.datafusion.TableProvider;
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

  // Static FunctionDescriptors - define the FFI signatures once at class load
  private static final FunctionDescriptor TABLE_NAMES_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor TABLE_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor TABLE_EXISTS_DESC =
      FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle TABLE_NAMES_MH = initTableNamesMethodHandle();
  private static final MethodHandle TABLE_MH = initTableMethodHandle();
  private static final MethodHandle TABLE_EXISTS_MH = initTableExistsMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initTableNamesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "getTableNames",
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

  private static MethodHandle initTableMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "getTable",
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

  private static MethodHandle initTableExistsMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "tableExists",
              MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              SchemaProviderHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final SchemaProvider provider;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub tableNamesStub;
  private final UpcallStub tableStub;
  private final UpcallStub tableExistsStub;
  private final UpcallStub releaseStub;

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

      // Create upcall stubs - only the stubs are per-instance
      this.tableNamesStub = UpcallStub.create(TABLE_NAMES_MH.bindTo(this), TABLE_NAMES_DESC, arena);
      this.tableStub = UpcallStub.create(TABLE_MH.bindTo(this), TABLE_DESC, arena);
      this.tableExistsStub =
          UpcallStub.create(TABLE_EXISTS_MH.bindTo(this), TABLE_EXISTS_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(40); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_NAMES_FN, tableNamesStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_FN, tableStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_EXISTS_FN, tableExistsStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

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
      new ErrorOut(errorOut).set(e.getMessage(), arena);
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
      String tableName = new NativeString(name).value();
      TableProvider table = provider.table(tableName);
      PointerOut tableOutPtr = new PointerOut(tableOut);

      if (table == null) {
        tableOutPtr.setNull();
        return 0;
      }

      // Create a handle for the table
      TableProviderHandle tableHandle = new TableProviderHandle(table, allocator, arena);

      // Return the callback struct pointer
      tableOutPtr.set(tableHandle.getCallbackStruct());

      return 0;
    } catch (Exception e) {
      new ErrorOut(errorOut).set(e.getMessage(), arena);
      return -1;
    }
  }

  /** Callback: Check if a table exists. */
  @SuppressWarnings("unused")
  int tableExists(MemorySegment javaObject, MemorySegment name) {
    try {
      String tableName = new NativeString(name).value();
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

  @Override
  public void close() {
    // Callback struct freed by Rust
  }
}
