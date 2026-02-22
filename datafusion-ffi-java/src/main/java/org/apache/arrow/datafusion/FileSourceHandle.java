package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for FileSource.
 *
 * <p>This class allocates an {@code FFI_FileSource} struct in Java arena memory and populates it
 * with upcall stub function pointers. Rust copies the struct via {@code ptr::read}.
 *
 * <p>Layout of FFI_FileSource (56 bytes, align 8):
 *
 * <pre>
 * offset  0: create_file_opener fn ptr (ADDRESS)
 * offset  8: file_type fn ptr          (ADDRESS)
 * offset 16: clone fn ptr              (ADDRESS)
 * offset 24: release fn ptr            (ADDRESS)
 * offset 32: version fn ptr            (ADDRESS)
 * offset 40: private_data ptr          (ADDRESS)
 * offset 48: library_marker_id fn ptr  (ADDRESS)
 * </pre>
 */
final class FileSourceHandle implements TraitHandle {
  // ======== Struct layout and VarHandles ========

  private static final StructLayout FFI_FILE_SOURCE_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("create_file_opener"),
          ValueLayout.ADDRESS.withName("file_type"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("version"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final VarHandle VH_CREATE_FILE_OPENER =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("create_file_opener"));
  private static final VarHandle VH_FILE_TYPE =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("file_type"));
  private static final VarHandle VH_CLONE =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_VERSION =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("version"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_FILE_SOURCE_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // ======== FFIResult<FFI_FileOpener> layout ========
  // RResult<T, RString> with #[repr(C, u8)]: disc(u8 padded to 8B) + payload union
  // payload = max(sizeof(FFI_FileOpener)=48, sizeof(RString)) = 48
  // Total = 8 + 48 = 56 bytes

  private static final StructLayout FFI_RESULT_FILE_OPENER_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("discriminant"),
          MemoryLayout.sequenceLayout(6, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_RESULT_DISC =
      FFI_RESULT_FILE_OPENER_LAYOUT.varHandle(PathElement.groupElement("discriminant"));

  private static final long RESULT_PAYLOAD_OFFSET =
      FFI_RESULT_FILE_OPENER_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // ======== Rust symbol lookups ========

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_file_source_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_file_source_release").orElseThrow();

  // ======== Size validation ========

  private static final MethodHandle FFI_FILE_SOURCE_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_source_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_FILE_OPENER_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_file_source_result_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  /** Struct size exposed for parent Handle classes. */
  static final long FFI_SIZE = querySize();

  private static long querySize() {
    try {
      return (long) FFI_FILE_SOURCE_SIZE_MH.invokeExact();
    } catch (Throwable e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_FILE_SOURCE_LAYOUT.byteSize(), FFI_FILE_SOURCE_SIZE_MH, "FFI_FileSource");
    NativeUtil.validateSize(
        FFI_RESULT_FILE_OPENER_LAYOUT.byteSize(),
        FFI_RESULT_FILE_OPENER_SIZE_MH,
        "FFIResult<FFI_FileOpener>");
  }

  // ======== Callback FunctionDescriptors ========

  // create_file_opener: (ADDRESS, ADDRESS, LONG, INT, LONG, INT, LONG, INT, LONG) -> STRUCT
  //   selfPtr, projection_ptr, projection_len, has_limit, limit_value,
  //   has_batch_size, batch_size_value, partitioned_by_file_group, partition
  //   Returns FFIResult<FFI_FileOpener> (56 bytes)
  private static final FunctionDescriptor CREATE_FILE_OPENER_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_FILE_OPENER_LAYOUT,
          ValueLayout.ADDRESS, // selfPtr
          ValueLayout.ADDRESS, // projection_ptr
          ValueLayout.JAVA_LONG, // projection_len
          ValueLayout.JAVA_INT, // has_limit (i32 not bool per FFI rules)
          ValueLayout.JAVA_LONG, // limit_value
          ValueLayout.JAVA_INT, // has_batch_size (i32 not bool per FFI rules)
          ValueLayout.JAVA_LONG, // batch_size_value
          ValueLayout.JAVA_INT, // partitioned_by_file_group (i32 not bool per FFI rules)
          ValueLayout.JAVA_LONG); // partition

  // file_type: (ADDRESS) -> ADDRESS (null-terminated UTF-8 pointer)
  private static final FunctionDescriptor FILE_TYPE_DESC =
      FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  // release: (ADDRESS) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== Static MethodHandles ========

  private static final MethodHandle CREATE_FILE_OPENER_MH = initCreateFileOpenerMethodHandle();
  private static final MethodHandle FILE_TYPE_MH = initFileTypeMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initCreateFileOpenerMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileSourceHandle.class,
              "createFileOpener",
              MethodType.methodType(
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  long.class,
                  int.class,
                  long.class,
                  int.class,
                  long.class,
                  int.class,
                  long.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initFileTypeMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              FileSourceHandle.class,
              "getFileType",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
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

  // ======== Instance fields ========

  private final Arena arena;
  private final FileSource source;
  private final Schema schema;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment callbackStruct;
  private final MemorySegment fileTypePtr;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub createFileOpenerStub;
  private final UpcallStub fileTypeStub;
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

    // Pre-allocate null-terminated file type string in arena memory
    this.fileTypePtr = arena.allocateFrom(source.fileType());

    // Create upcall stubs
    this.createFileOpenerStub =
        UpcallStub.create(CREATE_FILE_OPENER_MH.bindTo(this), CREATE_FILE_OPENER_DESC, arena);
    this.fileTypeStub = UpcallStub.create(FILE_TYPE_MH.bindTo(this), FILE_TYPE_DESC, arena);
    this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

    // Allocate and populate the FFI struct in Java arena memory
    this.callbackStruct = arena.allocate(FFI_FILE_SOURCE_LAYOUT);
    VH_CREATE_FILE_OPENER.set(callbackStruct, 0L, createFileOpenerStub.segment());
    VH_FILE_TYPE.set(callbackStruct, 0L, fileTypeStub.segment());
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
    out.reinterpret(FFI_FILE_SOURCE_LAYOUT.byteSize()).copyFrom(callbackStruct);
  }

  /**
   * Callback: Create a FileOpener from the FileSource. Returns an FFIResult&lt;FFI_FileOpener&gt;
   * struct (56 bytes): discriminant (8B) + payload (48B).
   *
   * <p>Parameters from Rust: selfPtr, projection_ptr, projection_len, has_limit, limit_value,
   * has_batch_size, batch_size_value, partitioned_by_file_group, partition.
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment createFileOpener(
      MemorySegment selfPtr,
      MemorySegment projectionPtr,
      long projectionLen,
      int hasLimit,
      long limitValue,
      int hasBatchSize,
      long batchSizeValue,
      int partitionedByFileGroup,
      long partition) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_FILE_OPENER_LAYOUT);
    try {
      // Build projection list from Rust-side usize array
      List<Integer> projList;
      if (projectionLen > 0 && !projectionPtr.equals(MemorySegment.NULL)) {
        MemorySegment data = projectionPtr.reinterpret(projectionLen * 8);
        projList = new ArrayList<>((int) projectionLen);
        for (int i = 0; i < projectionLen; i++) {
          projList.add((int) data.getAtIndex(ValueLayout.JAVA_LONG, i));
        }
        projList = Collections.unmodifiableList(projList);
      } else {
        projList = Collections.emptyList();
      }

      Long limit = hasLimit != 0 ? limitValue : null;
      Long batchSize = hasBatchSize != 0 ? batchSizeValue : null;
      FileScanConfig scanConfig =
          new FileScanConfig(
              projList, limit, batchSize, partitionedByFileGroup != 0, (int) partition);

      // Call Java FileSource implementation
      FileOpener opener = source.createFileOpener(schema, allocator, scanConfig);

      // Wrap the opener using FileOpenerHandle
      FileOpenerHandle openerHandle =
          new FileOpenerHandle(opener, allocator, arena, fullStackTrace);

      // FFIResult::ROk(FFI_FileOpener)
      VH_RESULT_DISC.set(buffer, 0L, 0L); // ROk
      openerHandle.copyStructTo(buffer.asSlice(RESULT_PAYLOAD_OFFSET, FileOpenerHandle.FFI_SIZE));

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

  /** Callback: Return the file type as a null-terminated UTF-8 string pointer. */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getFileType(MemorySegment selfPtr) {
    return fileTypePtr;
  }

  /** Callback: Release the source. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment selfPtr) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // No-op: FFI struct is in Java arena memory, freed when arena closes
  }
}
