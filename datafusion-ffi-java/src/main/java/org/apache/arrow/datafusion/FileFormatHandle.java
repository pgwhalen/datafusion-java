package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for FileFormat.
 *
 * <p>This class allocates a {@code JavaFileFormatCallbacks} struct in Java arena memory and
 * populates it with upcall stub function pointers. Rust copies the struct via {@code ptr::read}.
 *
 * <p>Layout of JavaFileFormatCallbacks (24 bytes, align 8):
 *
 * <pre>
 * offset  0: java_object ptr     (ADDRESS)
 * offset  8: file_source_fn ptr  (ADDRESS)
 * offset 16: release_fn ptr      (ADDRESS)
 * </pre>
 */
final class FileFormatHandle implements TraitHandle {
  // ======== Struct layout and VarHandles ========

  private static final StructLayout CALLBACKS_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("java_object"),
          ValueLayout.ADDRESS.withName("file_source_fn"),
          ValueLayout.ADDRESS.withName("release_fn"));

  private static final VarHandle VH_JAVA_OBJECT =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("java_object"));
  private static final VarHandle VH_FILE_SOURCE_FN =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("file_source_fn"));
  private static final VarHandle VH_RELEASE_FN =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("release_fn"));

  // ======== Size validation ========

  private static final MethodHandle CALLBACKS_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_format_callbacks_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static void validateSizes() {
    NativeUtil.validateSize(
        CALLBACKS_LAYOUT.byteSize(), CALLBACKS_SIZE_MH, "JavaFileFormatCallbacks");
  }

  // ======== Callback FunctionDescriptors ========

  // file_source_fn: (ADDRESS, ADDRESS, ADDRESS) -> INT
  private static final FunctionDescriptor FILE_SOURCE_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  // release_fn: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== Static MethodHandles ========

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

  // ======== Instance fields ========

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

    // Create upcall stubs
    this.fileSourceStub = UpcallStub.create(FILE_SOURCE_MH.bindTo(this), FILE_SOURCE_DESC, arena);
    this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

    // Allocate and populate the callback struct in Java arena memory
    this.callbackStruct = arena.allocate(CALLBACKS_LAYOUT);
    VH_JAVA_OBJECT.set(callbackStruct, 0L, MemorySegment.NULL);
    VH_FILE_SOURCE_FN.set(callbackStruct, 0L, fileSourceStub.segment());
    VH_RELEASE_FN.set(callbackStruct, 0L, releaseStub.segment());
  }

  /** Get the callback struct pointer to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return callbackStruct;
  }

  /** Copy the callback struct bytes into a Rust-side output buffer. */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(CALLBACKS_LAYOUT.byteSize()).copyFrom(callbackStruct);
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

      // Copy the callback struct bytes into the output buffer
      sourceHandle.copyStructTo(sourceOut);

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
    // No-op: callback struct is in Java arena memory, freed when arena closes
  }
}
