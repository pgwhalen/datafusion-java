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
 * <p>This class allocates an {@code FFI_FileFormat} struct in Java arena memory and populates it
 * with upcall stub function pointers. Rust copies the struct via {@code ptr::read}.
 *
 * <p>Layout of FFI_FileFormat (48 bytes, align 8):
 *
 * <pre>
 * offset  0: file_source fn ptr        (ADDRESS)
 * offset  8: clone fn ptr              (ADDRESS)
 * offset 16: release fn ptr            (ADDRESS)
 * offset 24: version fn ptr            (ADDRESS)
 * offset 32: private_data ptr          (ADDRESS)
 * offset 40: library_marker_id fn ptr  (ADDRESS)
 * </pre>
 */
final class FileFormatHandle implements TraitHandle {
  // ======== Struct layout and VarHandles ========

  private static final StructLayout FFI_FILE_FORMAT_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("file_source"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_FILE_SOURCE =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("file_source"));
  private static final VarHandle VH_CLONE =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_FILE_FORMAT_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // ======== FFIResult<FFI_FileSource> layout ========
  // RResult<T, RString> with #[repr(C, u8)]: disc(u8 padded to 8B) + payload union
  // payload = max(sizeof(FFI_FileSource)=48, sizeof(RString)) = 48
  // Total = 8 + 48 = 56 bytes

  private static final StructLayout FFI_RESULT_FILE_SOURCE_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(6, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_RESULT_DISC =
      FFI_RESULT_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  private static final long RESULT_PAYLOAD_OFFSET =
      FFI_RESULT_FILE_SOURCE_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // ======== Rust symbol lookups ========

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_file_format_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_file_format_release").orElseThrow();

  // ======== Size validation ========

  private static final MethodHandle FFI_FILE_FORMAT_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_format_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_FILE_SOURCE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_format_result_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_FILE_FORMAT_LAYOUT.byteSize(), FFI_FILE_FORMAT_SIZE_MH, "FFI_FileFormat");
    NativeUtil.validateSize(
        FFI_RESULT_FILE_SOURCE_LAYOUT.byteSize(),
        FFI_RESULT_FILE_SOURCE_SIZE_MH,
        "FFIResult<FFI_FileSource>");
  }

  // ======== Callback FunctionDescriptors ========

  // file_source: (ADDRESS) -> STRUCT (FFIResult<FFI_FileSource>, 56 bytes)
  private static final FunctionDescriptor FILE_SOURCE_DESC =
      FunctionDescriptor.of(FFI_RESULT_FILE_SOURCE_LAYOUT, ValueLayout.ADDRESS);

  // release: (ADDRESS) -> void
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
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
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

    // Allocate and populate the FFI struct in Java arena memory
    this.callbackStruct = arena.allocate(FFI_FILE_FORMAT_LAYOUT);
    VH_FILE_SOURCE.set(callbackStruct, 0L, fileSourceStub.segment());
    VH_CLONE.set(callbackStruct, 0L, CLONE_FN);
    VH_RELEASE.set(callbackStruct, 0L, RELEASE_FN);
    VH_VERSION.set(callbackStruct, 0L, NativeUtil.VERSION_FN);
    VH_PRIVATE_DATA.set(callbackStruct, 0L, MemorySegment.NULL);
    VH_LIBRARY_MARKER_ID.set(callbackStruct, 0L, NativeUtil.JAVA_MARKER_ID_FN);
  }

  /** Get the FFI struct pointer to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return callbackStruct;
  }

  /** Copy the FFI struct bytes into a Rust-side output buffer. */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_FILE_FORMAT_LAYOUT.byteSize()).copyFrom(callbackStruct);
  }

  /**
   * Callback: Create a FileSource from the FileFormat. Returns an FFIResult&lt;FFI_FileSource&gt;
   * struct (56 bytes): discriminant (8B) + payload (48B).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment fileSource(MemorySegment selfPtr) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_FILE_SOURCE_LAYOUT);
    try {
      // Call Java FileFormat implementation
      FileSource source = format.fileSource();

      // Wrap the source using FileSourceHandle
      FileSourceHandle sourceHandle =
          new FileSourceHandle(source, schema, allocator, arena, fullStackTrace);

      // FFIResult::ROk(FFI_FileSource)
      VH_RESULT_DISC.set(buffer, 0L, 0L); // ROk
      sourceHandle.copyStructTo(buffer.asSlice(RESULT_PAYLOAD_OFFSET, FileSourceHandle.FFI_SIZE));

      return buffer;
    } catch (Exception e) {
      // FFIResult::RErr(RString)
      buffer.fill((byte) 0);
      VH_RESULT_DISC.set(buffer, 0L, 1L); // RErr
      NativeUtil.writeRString(
          Errors.getErrorMessage(e, fullStackTrace), buffer, RESULT_PAYLOAD_OFFSET, arena);

      return buffer;
    }
  }

  /** Callback: Release the format. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment selfPtr) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // No-op: FFI struct is in Java arena memory, freed when arena closes
  }
}
