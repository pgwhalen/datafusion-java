package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Internal FFI bridge for FileOpener.
 *
 * <p>This class allocates an {@code FFI_FileOpener} struct in Java arena memory and populates it
 * with upcall stub function pointers. Rust copies the struct via {@code ptr::read}.
 *
 * <p>Layout of FFI_FileOpener (48 bytes, align 8):
 *
 * <pre>
 * offset  0: open fn ptr              (ADDRESS)
 * offset  8: clone fn ptr             (ADDRESS)
 * offset 16: release fn ptr           (ADDRESS)
 * offset 24: version fn ptr           (ADDRESS)
 * offset 32: private_data ptr         (ADDRESS)
 * offset 40: library_marker_id fn ptr (ADDRESS)
 * </pre>
 */
final class FileOpenerHandle implements TraitHandle {
  // ======== Struct layout and VarHandles ========

  private static final StructLayout FFI_FILE_OPENER_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("open"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_OPEN =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("open"));
  private static final VarHandle VH_CLONE =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // ======== FFIResult<FFI_RecordBatchStream> layout ========
  // RResult<T, RString> with #[repr(C, u8)]: disc(u8 padded to 8B) + payload union
  // payload = max(sizeof(FFI_RecordBatchStream)=32, sizeof(RString)) = 32
  // Total = 8 + 32 = 40 bytes

  private static final StructLayout FFI_RESULT_STREAM_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_RESULT_DISC =
      FFI_RESULT_STREAM_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  private static final long RESULT_PAYLOAD_OFFSET =
      FFI_RESULT_STREAM_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // ======== Rust symbol lookups ========

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_file_opener_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_file_opener_release").orElseThrow();

  // ======== Size validation ========

  private static final MethodHandle FFI_FILE_OPENER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_opener_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_STREAM_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_opener_result_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  /** Struct size exposed for parent Handle classes. */
  static final long FFI_SIZE = querySize();

  private static long querySize() {
    try {
      return (long) FFI_FILE_OPENER_SIZE_MH.invokeExact();
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_FILE_OPENER_LAYOUT.byteSize(), FFI_FILE_OPENER_SIZE_MH, "FFI_FileOpener");
    NativeUtil.validateSize(
        FFI_RESULT_STREAM_LAYOUT.byteSize(),
        FFI_RESULT_STREAM_SIZE_MH,
        "FFIResult<FFI_RecordBatchStream>");
  }

  // ======== Callback FunctionDescriptors ========

  // open: (ADDRESS, ADDRESS, LONG, LONG, INT, LONG, LONG) -> STRUCT (FFIResult<stream>, 40 bytes)
  private static final FunctionDescriptor OPEN_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_STREAM_LAYOUT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_LONG);

  // release: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== Static MethodHandles ========

  private static final MethodHandle OPEN_MH = initOpenMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initOpenMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileOpenerHandle.class,
              "open",
              MethodType.methodType(
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  long.class,
                  long.class,
                  int.class,
                  long.class,
                  long.class));
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

  // ======== Instance fields ========

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

    // Create upcall stubs
    this.openStub = UpcallStub.create(OPEN_MH.bindTo(this), OPEN_DESC, arena);
    this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

    // Allocate and populate the FFI struct in Java arena memory
    this.callbackStruct = arena.allocate(FFI_FILE_OPENER_LAYOUT);
    VH_OPEN.set(callbackStruct, 0L, openStub.segment());
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
    out.reinterpret(FFI_FILE_OPENER_LAYOUT.byteSize()).copyFrom(callbackStruct);
  }

  /**
   * Callback: Open a file and return a RecordBatchReader. Returns an
   * FFIResult&lt;FFI_RecordBatchStream&gt; struct (40 bytes): discriminant (8B) + payload (32B).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment open(
      MemorySegment selfPtr,
      MemorySegment filePath,
      long filePathLen,
      long fileSize,
      int hasRange,
      long rangeStart,
      long rangeEnd) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_STREAM_LAYOUT);
    try {
      // Convert native UTF-8 bytes to Java String
      byte[] pathBytes = filePath.reinterpret(filePathLen).toArray(ValueLayout.JAVA_BYTE);
      String path = new String(pathBytes, StandardCharsets.UTF_8);

      // Build PartitionedFile from FFI parameters
      Long rangeStartObj = hasRange != 0 ? rangeStart : null;
      Long rangeEndObj = hasRange != 0 ? rangeEnd : null;
      PartitionedFile partitionedFile =
          new PartitionedFile(path, fileSize, rangeStartObj, rangeEndObj);

      // Call Java FileOpener implementation
      RecordBatchReader reader = opener.open(partitionedFile);

      // Wrap the reader using RecordBatchReaderHandle (reuse existing FFI bridge)
      RecordBatchReaderHandle readerHandle =
          new RecordBatchReaderHandle(reader, allocator, arena, fullStackTrace);

      // FFIResult::ROk(FFI_RecordBatchStream)
      VH_RESULT_DISC.set(buffer, 0L, 0L); // ROk
      readerHandle.copyStructTo(
          buffer.asSlice(RESULT_PAYLOAD_OFFSET, RecordBatchReaderHandle.streamStructSize()));

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

  /** Callback: Release the opener. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment selfPtr) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // No-op: FFI struct is in Java arena memory, freed when arena closes
  }
}
