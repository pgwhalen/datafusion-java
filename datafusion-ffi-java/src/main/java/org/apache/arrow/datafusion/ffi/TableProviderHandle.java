package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.ExecutionPlan;
import org.apache.arrow.datafusion.TableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for TableProvider.
 *
 * <p>This class creates upcall stubs that Rust can invoke to access a Java {@link TableProvider}.
 * It manages the lifecycle of the callback struct and upcall stubs.
 */
final class TableProviderHandle implements AutoCloseable {
  private static final long ARROW_SCHEMA_SIZE = 72;

  // Callback struct field offsets
  // struct JavaTableProviderCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   schema_fn: fn,                    // offset 8
  //   table_type_fn: fn,                // offset 16
  //   scan_fn: fn,                      // offset 24
  //   release_fn: fn,                   // offset 32
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_SCHEMA_FN = 8;
  private static final long OFFSET_TABLE_TYPE_FN = 16;
  private static final long OFFSET_SCAN_FN = 24;
  private static final long OFFSET_RELEASE_FN = 32;

  // Static FunctionDescriptors - define the FFI signatures once at class load
  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  private static final FunctionDescriptor TABLE_TYPE_DESC =
      FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS);

  private static final FunctionDescriptor SCAN_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_LONG,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();
  private static final MethodHandle TABLE_TYPE_MH = initTableTypeMethodHandle();
  private static final MethodHandle SCAN_MH = initScanMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              TableProviderHandle.class,
              "getSchema",
              MethodType.methodType(
                  int.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
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
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  long.class,
                  long.class,
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
              TableProviderHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final TableProvider provider;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub schemaStub;
  private final UpcallStub tableTypeStub;
  private final UpcallStub scanStub;
  private final UpcallStub releaseStub;

  TableProviderHandle(TableProvider provider, BufferAllocator allocator, Arena arena) {
    this.arena = arena;
    this.provider = provider;
    this.allocator = allocator;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_TABLE_PROVIDER_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate TableProvider callbacks");
      }

      // Create upcall stubs - only the stubs are per-instance
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.tableTypeStub = UpcallStub.create(TABLE_TYPE_MH.bindTo(this), TABLE_TYPE_DESC, arena);
      this.scanStub = UpcallStub.create(SCAN_MH.bindTo(this), SCAN_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(40); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_SCHEMA_FN, schemaStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_TABLE_TYPE_FN, tableTypeStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_SCAN_FN, scanStub.segment());
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create TableProviderHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Get the schema. */
  @SuppressWarnings("unused")
  int getSchema(MemorySegment javaObject, MemorySegment schemaOut, MemorySegment errorOut) {
    try {
      Schema schema = provider.schema();

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);

        // Copy the FFI schema to the output pointer
        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment destSchema = schemaOut.reinterpret(ARROW_SCHEMA_SIZE);
        destSchema.copyFrom(srcSchema);

        // Clear release in source - dest (Rust copy) has it
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      }

      return 0;
    } catch (Exception e) {
      return ErrorOut.fromException(errorOut, e, arena);
    }
  }

  /** Callback: Get the table type. */
  @SuppressWarnings("unused")
  int getTableType(MemorySegment javaObject) {
    return provider.tableType().getValue();
  }

  /** Callback: Create a scan (execution plan). */
  @SuppressWarnings("unused")
  int scan(
      MemorySegment javaObject,
      MemorySegment projection,
      long projectionLen,
      long limit,
      MemorySegment planOut,
      MemorySegment errorOut) {
    try {
      // Convert projection array
      int[] projectionArray = null;
      if (!projection.equals(MemorySegment.NULL) && projectionLen > 0) {
        projectionArray = new int[(int) projectionLen];
        MemorySegment projSegment = projection.reinterpret(projectionLen * 8);
        for (int i = 0; i < projectionLen; i++) {
          projectionArray[i] = (int) projSegment.getAtIndex(ValueLayout.JAVA_LONG, i);
        }
      }

      // Convert limit
      Long limitValue = (limit >= 0) ? limit : null;

      // Create the execution plan
      ExecutionPlan plan = provider.scan(projectionArray, limitValue);

      // Create a handle for the plan
      ExecutionPlanHandle planHandle = new ExecutionPlanHandle(plan, allocator, arena);

      // Return the callback struct pointer
      new PointerOut(planOut).set(planHandle.getCallbackStruct());

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
