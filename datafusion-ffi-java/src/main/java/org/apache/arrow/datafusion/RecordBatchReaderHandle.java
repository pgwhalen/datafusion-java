package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Internal FFI bridge for RecordBatchReader.
 *
 * <p>This class constructs an {@code FFI_RecordBatchStream} struct directly in Java memory. The
 * struct contains function pointers ({@code poll_next}, {@code schema}, {@code release}) that Rust
 * invokes to read batches from a Java {@link RecordBatchReader}.
 *
 * <p>Layout of FFI_RecordBatchStream (32 bytes, align 8):
 *
 * <pre>
 * offset  0: poll_next fn ptr  (ADDRESS)
 * offset  8: schema fn ptr     (ADDRESS)
 * offset 16: release fn ptr    (ADDRESS)
 * offset 24: private_data ptr  (ADDRESS)
 * </pre>
 *
 * <p>The poll_next callback returns a 176-byte struct
 * (FfiPoll&lt;ROption&lt;RResult&lt;WrappedArray, RString&gt;&gt;&gt;) with discriminants at
 * offsets 0, 8, and 16.
 */
final class RecordBatchReaderHandle implements TraitHandle {
  private static final MethodHandle POLL_RESULT_SIZE =
      NativeUtil.downcall(
          "datafusion_poll_result_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle WRAPPED_SCHEMA_SIZE =
      NativeUtil.downcall(
          "datafusion_wrapped_schema_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RECORD_BATCH_STREAM_SIZE =
      NativeUtil.downcall(
          "datafusion_ffi_record_batch_stream_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  // Size of Arrow FFI structures (external constants, not defined by our layouts)
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;

  // FFI_RecordBatchStream struct layout (4 pointers = 32 bytes)
  private static final StructLayout FFI_RECORD_BATCH_STREAM_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("poll_next"),
          ValueLayout.ADDRESS.withName("schema"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("private_data"));

  private static final VarHandle VH_POLL_NEXT =
      FFI_RECORD_BATCH_STREAM_LAYOUT.varHandle(PathElement.groupElement("poll_next"));
  private static final VarHandle VH_SCHEMA =
      FFI_RECORD_BATCH_STREAM_LAYOUT.varHandle(PathElement.groupElement("schema"));
  private static final VarHandle VH_RELEASE =
      FFI_RECORD_BATCH_STREAM_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_RECORD_BATCH_STREAM_LAYOUT.varHandle(PathElement.groupElement("private_data"));

  // poll_next return type: FfiPoll<ROption<RResult<WrappedArray, RString>>>
  // Each discriminant is a u8 padded to 8 bytes by abi_stable. We model each slot as JAVA_LONG
  // because the FFM linker rejects non-natural padding between same-aligned members.
  // Layout: [poll disc (8B), option disc (8B), result disc (8B), payload (152B)]  Total = 176 bytes
  private static final StructLayout POLL_RESULT_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.JAVA_LONG.withName("poll_discriminant"),
          ValueLayout.JAVA_LONG.withName("option_discriminant"),
          ValueLayout.JAVA_LONG.withName("result_discriminant"),
          MemoryLayout.sequenceLayout(19, ValueLayout.JAVA_LONG).withName("payload"));

  private static final VarHandle VH_POLL_DISC =
      POLL_RESULT_LAYOUT.varHandle(PathElement.groupElement("poll_discriminant"));
  private static final VarHandle VH_OPTION_DISC =
      POLL_RESULT_LAYOUT.varHandle(PathElement.groupElement("option_discriminant"));
  private static final VarHandle VH_RESULT_DISC =
      POLL_RESULT_LAYOUT.varHandle(PathElement.groupElement("result_discriminant"));

  // Kept as numeric offset — used with asSlice() and writeRString(), not set/get
  private static final long PAYLOAD_OFFSET =
      POLL_RESULT_LAYOUT.byteOffset(PathElement.groupElement("payload"));

  // WrappedSchema return type = FFI_ArrowSchema (opaque, bulk-copied)
  private static final StructLayout WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(ARROW_SCHEMA_SIZE / 8, ValueLayout.JAVA_LONG));

  // poll_next: (ADDRESS stream, ADDRESS cx) -> STRUCT (176 bytes)
  private static final FunctionDescriptor POLL_NEXT_DESC =
      FunctionDescriptor.of(POLL_RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

  // schema: (ADDRESS stream) -> STRUCT (72 bytes)
  private static final FunctionDescriptor SCHEMA_DESC =
      FunctionDescriptor.of(WRAPPED_SCHEMA_LAYOUT, ValueLayout.ADDRESS);

  // release: (ADDRESS stream) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // Static MethodHandles - looked up once at class load
  private static final MethodHandle POLL_NEXT_MH = initPollNextMethodHandle();
  private static final MethodHandle SCHEMA_MH = initSchemaMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initPollNextMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              RecordBatchReaderHandle.class,
              "pollNext",
              MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initSchemaMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              RecordBatchReaderHandle.class,
              "getSchema",
              MethodType.methodType(MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              RecordBatchReaderHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final Arena arena;
  private final RecordBatchReader reader;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiStream;

  // Pre-allocated buffers for return values to avoid per-call allocation
  private final MemorySegment pollResultBuffer;
  private final MemorySegment schemaBuffer;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub pollNextStub;
  private final UpcallStub schemaStub;
  private final UpcallStub releaseStub;

  RecordBatchReaderHandle(
      RecordBatchReader reader, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.reader = reader;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Pre-allocate return buffers in arena
      this.pollResultBuffer = arena.allocate(POLL_RESULT_LAYOUT);
      this.schemaBuffer = arena.allocate(WRAPPED_SCHEMA_LAYOUT);

      // Create upcall stubs
      this.pollNextStub = UpcallStub.create(POLL_NEXT_MH.bindTo(this), POLL_NEXT_DESC, arena);
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Allocate FFI_RecordBatchStream struct in Java arena memory
      this.ffiStream = arena.allocate(FFI_RECORD_BATCH_STREAM_LAYOUT);
      VH_POLL_NEXT.set(ffiStream, 0L, pollNextStub.segment());
      VH_SCHEMA.set(ffiStream, 0L, schemaStub.segment());
      VH_RELEASE.set(ffiStream, 0L, releaseStub.segment());
      VH_PRIVATE_DATA.set(ffiStream, 0L, MemorySegment.NULL);

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create RecordBatchReaderHandle", e);
    }
  }

  static void validateSizes() {
    NativeUtil.validateSize(
        POLL_RESULT_LAYOUT.byteSize(), POLL_RESULT_SIZE, "poll_next return type");
    NativeUtil.validateSize(WRAPPED_SCHEMA_LAYOUT.byteSize(), WRAPPED_SCHEMA_SIZE, "WrappedSchema");
    NativeUtil.validateSize(
        FFI_RECORD_BATCH_STREAM_LAYOUT.byteSize(),
        FFI_RECORD_BATCH_STREAM_SIZE,
        "FFI_RecordBatchStream");
  }

  /** Get the FFI_RecordBatchStream struct to pass to Rust. */
  public MemorySegment getTraitStruct() {
    return ffiStream;
  }

  /**
   * Copy the FFI_RecordBatchStream bytes into a destination buffer. Used by callers that need to
   * write the struct into Rust stack memory (e.g., execute callback's stream_out parameter).
   */
  void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_RECORD_BATCH_STREAM_LAYOUT.byteSize()).copyFrom(ffiStream);
  }

  /** Returns the byte size of the FFI_RecordBatchStream struct layout. */
  static long streamStructSize() {
    return FFI_RECORD_BATCH_STREAM_LAYOUT.byteSize();
  }

  /**
   * Callback: poll_next. Returns a 176-byte FfiPoll struct.
   *
   * <p>Layout: [FfiPoll disc (1B), 7B pad, ROption disc (1B), 7B pad, RResult disc (1B), 7B pad,
   * payload...]
   *
   * <ul>
   *   <li>Batch available: [0, pad, 0, pad, 0, pad, ArrowArray(80B), ArrowSchema(72B)]
   *   <li>End of stream: [0, pad, 1, pad, ...]
   *   <li>Error: [0, pad, 0, pad, 1, pad, RString(varies)]
   * </ul>
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment pollNext(MemorySegment stream, MemorySegment context) {
    pollResultBuffer.fill((byte) 0);
    try {
      boolean hasNext = reader.loadNextBatch();
      if (!hasNext) {
        // FfiPoll::Ready(RNone) — end of stream
        VH_POLL_DISC.set(pollResultBuffer, 0L, 0L); // Ready
        VH_OPTION_DISC.set(pollResultBuffer, 0L, 1L); // RNone
        return pollResultBuffer;
      }

      // FfiPoll::Ready(RSome(ROk(WrappedArray{array, schema})))
      VH_POLL_DISC.set(pollResultBuffer, 0L, 0L); // Ready
      VH_OPTION_DISC.set(pollResultBuffer, 0L, 0L); // RSome
      VH_RESULT_DISC.set(pollResultBuffer, 0L, 0L); // ROk

      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // Export Arrow data at payload offset:
      //   offset 24: FFI_ArrowArray (80 bytes)
      //   offset 104: FFI_ArrowSchema (72 bytes) [= WrappedSchema]
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

        MemorySegment srcArray =
            MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);

        // Copy into poll result buffer at the correct offsets
        MemorySegment destArray = pollResultBuffer.asSlice(PAYLOAD_OFFSET, ARROW_ARRAY_SIZE);
        MemorySegment destSchema =
            pollResultBuffer.asSlice(PAYLOAD_OFFSET + ARROW_ARRAY_SIZE, ARROW_SCHEMA_SIZE);

        destArray.copyFrom(srcArray);
        destSchema.copyFrom(srcSchema);

        // Clear release callbacks in source to prevent double-free
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
        srcArray.set(ValueLayout.ADDRESS, 72, MemorySegment.NULL);
      }

      return pollResultBuffer;

    } catch (Exception e) {
      // FfiPoll::Ready(RSome(RErr(rstring)))
      pollResultBuffer.fill((byte) 0);
      VH_POLL_DISC.set(pollResultBuffer, 0L, 0L); // Ready
      VH_OPTION_DISC.set(pollResultBuffer, 0L, 0L); // RSome
      VH_RESULT_DISC.set(pollResultBuffer, 0L, 1L); // RErr

      // Write error RString at payload offset using Rust helper
      String errorMsg = fullStackTrace ? getStackTrace(e) : e.getMessage();
      if (errorMsg == null) {
        errorMsg = e.getClass().getName();
      }
      NativeUtil.writeRString(errorMsg, pollResultBuffer, PAYLOAD_OFFSET, arena);
      return pollResultBuffer;
    }
  }

  /** Callback: schema. Returns a 72-byte WrappedSchema (= FFI_ArrowSchema). */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment getSchema(MemorySegment stream) {
    schemaBuffer.fill((byte) 0);
    try {
      Schema schema = reader.getVectorSchemaRoot().getSchema();

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);

        MemorySegment srcSchema =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);

        schemaBuffer.copyFrom(srcSchema);

        // Clear release in source — dest (Rust copy) has it
        srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      }
    } catch (Exception e) {
      // schema() cannot return an error in the FFI protocol.
      // Return zeroed schema — will likely cause a downstream error.
    }
    return schemaBuffer;
  }

  /** Callback: release. Called by Rust when done with the stream. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment stream) {
    try {
      reader.close();
    } catch (Exception e) {
      // Best effort — don't throw from callback
    }
  }

  @Override
  public void close() {
    // The FFI_RecordBatchStream struct lives in the arena; Rust calls release() when done.
    // The arena will clean up the upcall stubs.
  }

  private static String getStackTrace(Exception e) {
    java.io.StringWriter sw = new java.io.StringWriter();
    e.printStackTrace(new java.io.PrintWriter(sw));
    return sw.toString();
  }
}
