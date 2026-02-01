package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Internal FFI bridge for RecordBatchReader.
 *
 * <p>This class creates upcall stubs that Rust can invoke to read batches from a Java {@link
 * RecordBatchReader}. It manages the lifecycle of the callback struct and upcall stubs.
 */
final class RecordBatchReaderHandle implements AutoCloseable {
  // Size of FFI_ArrowSchema and FFI_ArrowArray structures
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;

  // Callback struct field offsets
  // struct JavaRecordBatchReaderCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   load_next_batch_fn: fn,           // offset 8
  //   release_fn: fn,                   // offset 16
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_LOAD_NEXT_BATCH_FN = 8;
  private static final long OFFSET_RELEASE_FN = 16;

  private final Arena arena;
  private final RecordBatchReader reader;
  private final BufferAllocator allocator;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final MemorySegment loadNextBatchStub;
  private final MemorySegment releaseStub;

  RecordBatchReaderHandle(RecordBatchReader reader, BufferAllocator allocator, Arena arena) {
    this.arena = arena;
    this.reader = reader;
    this.allocator = allocator;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_RECORD_BATCH_READER_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate RecordBatchReader callbacks");
      }

      // Create upcall stubs for the callback functions
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      Linker linker = DataFusionBindings.getLinker();

      // load_next_batch_fn: (java_object, array_out, schema_out, error_out) -> i32
      MethodHandle loadNextBatchHandle =
          lookup.bind(
              this,
              "loadNextBatch",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class));
      FunctionDescriptor loadNextBatchDesc =
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS);
      this.loadNextBatchStub = linker.upcallStub(loadNextBatchHandle, loadNextBatchDesc, arena);

      // release_fn: (java_object) -> void
      MethodHandle releaseHandle =
          lookup.bind(this, "release", MethodType.methodType(void.class, MemorySegment.class));
      FunctionDescriptor releaseDesc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
      this.releaseStub = linker.upcallStub(releaseHandle, releaseDesc, arena);

      // Set up the callback struct
      MemorySegment struct = callbackStruct.reinterpret(24); // struct size
      struct.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct.set(ValueLayout.ADDRESS, OFFSET_LOAD_NEXT_BATCH_FN, loadNextBatchStub);
      struct.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub);

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create RecordBatchReaderHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Load the next batch into FFI structs. Returns 1 if batch, 0 if end, -1 if error. */
  @SuppressWarnings("unused") // Called via upcall stub
  int loadNextBatch(
      MemorySegment javaObject,
      MemorySegment arrayOut,
      MemorySegment schemaOut,
      MemorySegment errorOut) {
    try {
      boolean hasNext = reader.loadNextBatch();
      if (!hasNext) {
        return 0; // End of stream
      }

      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // Export the data via Arrow C Data Interface
      // We use try-with-resources but clear the release callbacks before closing
      // so that Rust's copy of the FFI struct has the valid release callback
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

        // Copy the FFI structs to the output pointers
        // The output pointers are pre-allocated by Rust
        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment srcArray =
            MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);

        MemorySegment destSchema = schemaOut.reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment destArray = arrayOut.reinterpret(ARROW_ARRAY_SIZE);

        destSchema.copyFrom(srcSchema);
        destArray.copyFrom(srcArray);

        // Clear the release callback in the SOURCE structs to prevent double-free
        // The DEST (Rust copy) still has the release callback and will call it
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
        srcArray.set(ValueLayout.ADDRESS, 72, MemorySegment.NULL);
      }

      return 1; // Batch available

    } catch (Exception e) {
      setError(errorOut, e.getMessage());
      return -1;
    }
  }

  /** Callback: Release the reader. Called by Rust when done with the reader. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment javaObject) {
    // Close the reader to release its resources
    try {
      reader.close();
    } catch (Exception e) {
      // Best effort - log but don't throw from callback
    }
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
    // The callback struct is freed by Rust when it drops the JavaBackedRecordBatchStream
    // The arena will clean up the upcall stubs
  }
}
