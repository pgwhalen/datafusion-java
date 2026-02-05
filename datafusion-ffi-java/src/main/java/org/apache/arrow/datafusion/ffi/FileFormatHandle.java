package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.FileFormat;
import org.apache.arrow.datafusion.FileSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for FileFormat.
 *
 * <p>This class creates upcall stubs that Rust can invoke to get a FileSource from a Java {@link
 * FileFormat}. It manages the lifecycle of the callback struct and upcall stubs.
 */
final class FileFormatHandle implements TraitHandle {

  // Callback struct field offsets
  // struct JavaFileFormatCallbacks {
  //   java_object: *mut c_void,         // offset 0
  //   file_source_fn: fn,               // offset 8
  //   release_fn: fn,                   // offset 16
  // }
  private static final long OFFSET_JAVA_OBJECT = 0;
  private static final long OFFSET_FILE_SOURCE_FN = 8;
  private static final long OFFSET_RELEASE_FN = 16;
  private static final long STRUCT_SIZE = 24;

  // Static FunctionDescriptors
  // file_source_fn: (ADDRESS, ADDRESS, ADDRESS) -> INT
  //   java_object, source_out, error_out -> result
  private static final FunctionDescriptor FILE_SOURCE_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  // release_fn: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle FILE_SOURCE_MH = initFileSourceMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initFileSourceMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileFormatHandle.class,
              "fileSource",
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
              FileFormatHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final FileFormat format;
  private final Schema schema;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment callbackStruct;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub fileSourceStub;
  private final UpcallStub releaseStub;

  FileFormatHandle(
      FileFormat format,
      Schema schema,
      BufferAllocator allocator,
      Arena arena,
      boolean fullStackTrace) {
    this.arena = arena;
    this.format = format;
    this.schema = schema;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Allocate the callback struct from Rust
      this.callbackStruct =
          (MemorySegment) DataFusionBindings.ALLOC_FILE_FORMAT_CALLBACKS.invokeExact();

      if (callbackStruct.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to allocate FileFormat callbacks");
      }

      // Create upcall stubs
      this.fileSourceStub = UpcallStub.create(FILE_SOURCE_MH.bindTo(this), FILE_SOURCE_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Set up the callback struct
      MemorySegment struct_ = callbackStruct.reinterpret(STRUCT_SIZE);
      struct_.set(ValueLayout.ADDRESS, OFFSET_JAVA_OBJECT, MemorySegment.NULL);
      struct_.set(ValueLayout.ADDRESS, OFFSET_FILE_SOURCE_FN, fileSourceStub.segment());
      struct_.set(ValueLayout.ADDRESS, OFFSET_RELEASE_FN, releaseStub.segment());

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create FileFormatHandle", e);
    }
  }

  /** Get the callback struct pointer to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return callbackStruct;
  }

  /** Callback: Create a FileSource from the FileFormat. */
  @SuppressWarnings("unused") // Called via upcall stub
  int fileSource(MemorySegment javaObject, MemorySegment sourceOut, MemorySegment errorOut) {
    try {
      // Call Java FileFormat implementation
      FileSource source = format.fileSource();

      // Wrap the source using FileSourceHandle
      FileSourceHandle sourceHandle =
          new FileSourceHandle(source, schema, allocator, arena, fullStackTrace);

      // Return the callback struct pointer
      sourceHandle.setToPointer(sourceOut);

      return Errors.SUCCESS;
    } catch (Exception e) {
      return Errors.fromException(errorOut, e, arena, fullStackTrace);
    }
  }

  /** Callback: Release the format. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment javaObject) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // Callback struct freed by Rust when it drops the format
  }
}
