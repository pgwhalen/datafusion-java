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
 * <p>This class allocates a {@code JavaFileOpenerCallbacks} struct in Java arena memory and
 * populates it with upcall stub function pointers. Rust copies the struct via {@code ptr::read}.
 *
 * <p>Layout of JavaFileOpenerCallbacks (24 bytes, align 8):
 *
 * <pre>
 * offset  0: java_object ptr  (ADDRESS)
 * offset  8: open_fn ptr      (ADDRESS)
 * offset 16: release_fn ptr   (ADDRESS)
 * </pre>
 */
final class FileOpenerHandle implements TraitHandle {
  // ======== Struct layout and VarHandles ========

  private static final StructLayout CALLBACKS_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("java_object"),
          ValueLayout.ADDRESS.withName("open_fn"),
          ValueLayout.ADDRESS.withName("release_fn"));

  private static final VarHandle VH_JAVA_OBJECT =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("java_object"));
  private static final VarHandle VH_OPEN_FN =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("open_fn"));
  private static final VarHandle VH_RELEASE_FN =
      CALLBACKS_LAYOUT.varHandle(PathElement.groupElement("release_fn"));

  // ======== Size validation ========

  private static final MethodHandle CALLBACKS_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_opener_callbacks_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static void validateSizes() {
    NativeUtil.validateSize(
        CALLBACKS_LAYOUT.byteSize(), CALLBACKS_SIZE_MH, "JavaFileOpenerCallbacks");
  }

  // ======== Callback FunctionDescriptors ========

  // open_fn: (ADDRESS, ADDRESS, LONG, LONG, INT, LONG, LONG, ADDRESS, ADDRESS) -> INT
  private static final FunctionDescriptor OPEN_DESC =
      FunctionDescriptor.of(
          ValueLayout.JAVA_INT,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_INT,
          ValueLayout.JAVA_LONG,
          ValueLayout.JAVA_LONG,
          ValueLayout.ADDRESS,
          ValueLayout.ADDRESS);

  // release_fn: (ADDRESS) -> void
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
                  int.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  long.class,
                  long.class,
                  int.class,
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

    // Allocate and populate the callback struct in Java arena memory
    this.callbackStruct = arena.allocate(CALLBACKS_LAYOUT);
    VH_JAVA_OBJECT.set(callbackStruct, 0L, MemorySegment.NULL);
    VH_OPEN_FN.set(callbackStruct, 0L, openStub.segment());
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

  /** Callback: Open a file and return a RecordBatchReader. */
  @SuppressWarnings("unused") // Called via upcall stub
  int open(
      MemorySegment javaObject,
      MemorySegment filePath,
      long filePathLen,
      long fileSize,
      int hasRange,
      long rangeStart,
      long rangeEnd,
      MemorySegment readerOut,
      MemorySegment errorOut) {
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

      // Copy the FFI_RecordBatchStream struct into the output buffer
      readerHandle.copyStructTo(readerOut);

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
    // No-op: callback struct is in Java arena memory, freed when arena closes
  }
}
