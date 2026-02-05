package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.FileOpener;
import org.apache.arrow.datafusion.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Internal FFI bridge for FileOpener.
 *
 * <p>This class creates upcall stubs that Rust can invoke to open file content using a Java {@link
 * FileOpener}. It manages the lifecycle of the callback struct and upcall stubs.
 */
final class FileOpenerHandle implements AutoCloseable {

  // Callback struct field offsets
  // struct JavaFileOpenerCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   open_fn: fn,                      // offset 8
  //   release_fn: fn,                   // offset 16
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_OPEN_FN = 8;
  private static final long OFFSET_RELEASE_FN = 16;
  private static final long STRUCT_SIZE = 24;

  // open_fn: (ADDRESS, ADDRESS, LONG, ADDRESS, ADDRESS) -> INT
  //   java_object, file_content, file_content_len, reader_out, error_out -> result
  private static final FunctionDescriptor OPEN_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  // release_fn: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle OPEN_MH = initOpenMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initOpenMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileOpenerHandle.class,
              "open",
              MethodType.methodType(
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
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
              FileOpenerHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final FileOpener opener;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub openStub;
  private final UpcallStub releaseStub;

  FileOpenerHandle(
      FileOpener opener, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.opener = opener;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_FILE_OPENER_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate FileOpener callbacks");
      }

      // Create upcall stubs
      this.openStub = UpcallStub.create(OPEN_MH.bindTo(this), OPEN_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct_ = callbackStruct.reinterpret(STRUCT_SIZE);
      struct_.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct_.set(ValueLayout.ADDRESS, OFFSET_OPEN_FN, openStub.segment());
      struct_.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create FileOpenerHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Open file content and return a RecordBatchReader. */
  @SuppressWarnings("unused") // Called via upcall stub
  int open(
      MemorySegment javaObject,
      MemorySegment fileContent,
      long fileContentLen,
      MemorySegment readerOut,
      MemorySegment errorOut) {
    try {
      // Copy native bytes to Java byte array
      byte[] bytes = fileContent.reinterpret(fileContentLen).toArray(ValueLayout.JAVA_BYTE);

      // Call Java FileOpener implementation
      RecordBatchReader reader = opener.open(bytes);

      // Wrap the reader using RecordBatchReaderHandle (reuse existing FFI bridge)
      RecordBatchReaderHandle readerHandle =
          new RecordBatchReaderHandle(reader, allocator, arena, fullStackTrace);

      // Return the callback struct pointer
      new PointerOut(readerOut).set(readerHandle.getCallbackStruct());

      return Errors.SUCCESS;
    } catch (Exception e) {
      return Errors.fromException(errorOut, e, arena, fullStackTrace);
    }
  }

  /** Callback: Release the opener. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // Callback struct freed by Rust when it drops the opener
  }
}
