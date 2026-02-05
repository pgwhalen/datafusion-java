package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.FileOpener;
import org.apache.arrow.datafusion.FileSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for FileSource.
 *
 * <p>This class creates upcall stubs that Rust can invoke to create a FileOpener from a Java {@link
 * FileSource}. It manages the lifecycle of the callback struct and upcall stubs.
 */
final class FileSourceHandle implements AutoCloseable {

  // Callback struct field offsets
  // struct JavaFileSourceCallbacks {
  //   java_object: *mut c_void,              // offset 0
  //   create_file_opener_fn: fn,             // offset 8
  //   release_fn: fn,                        // offset 16
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_CREATE_FILE_OPENER_FN = 8;
  private static final long OFFSET_RELEASE_FN = 16;
  private static final long STRUCT_SIZE = 24;

  // create_file_opener_fn: (ADDRESS, ADDRESS, ADDRESS) -> INT
  //   java_object, opener_out, error_out -> result
  private static final FunctionDescriptor CREATE_FILE_OPENER_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  // release_fn: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle CREATE_FILE_OPENER_MH = initCreateFileOpenerMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initCreateFileOpenerMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileSourceHandle.class,
              "createFileOpener",
              MethodType.methodType(
                  int.class, MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileSourceHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final FileSource source;
  private final Schema schema;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub createFileOpenerStub;
  private final UpcallStub releaseStub;

  FileSourceHandle(
      FileSource source,
      Schema schema,
      BufferAllocator allocator,
      Arena arena,
      boolean fullStackTrace) {
    this.arena = arena;
    this.source = source;
    this.schema = schema;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_FILE_SOURCE_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate FileSource callbacks");
      }

      // Create upcall stubs
      this.createFileOpenerStub =
          UpcallStub.create(CREATE_FILE_OPENER_MH.bindTo(this), CREATE_FILE_OPENER_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct_ = callbackStruct.reinterpret(STRUCT_SIZE);
      struct_.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct_.set(
          ValueLayout.ADDRESS, OFFSET_CREATE_FILE_OPENER_FN, createFileOpenerStub.segment());
      struct_.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create FileSourceHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  MemorySegment getCallbackStruct() {
    return callbackStruct;
  }

  /** Callback: Create a FileOpener from the FileSource. */
  @SuppressWarnings("unused") // Called via upcall stub
  int createFileOpener(MemorySegment javaObject, MemorySegment openerOut, MemorySegment errorOut) {
    try {
      // Call Java FileSource implementation
      FileOpener opener = source.createFileOpener(schema, allocator);

      // Wrap the opener using FileOpenerHandle
      FileOpenerHandle openerHandle =
          new FileOpenerHandle(opener, allocator, arena, fullStackTrace);

      // Return the callback struct pointer
      new PointerOut(openerOut).set(openerHandle.getCallbackStruct());

      return Errors.SUCCESS;
    } catch (Exception e) {
      return Errors.fromException(errorOut, e, arena, fullStackTrace);
    }
  }

  /** Callback: Release the source. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // Callback struct freed by Rust when it drops the source
  }
}
