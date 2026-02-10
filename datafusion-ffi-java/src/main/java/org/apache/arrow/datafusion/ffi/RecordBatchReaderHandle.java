package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.RecordBatchReader;
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
  // Size of Arrow FFI structures
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;

  // FFI_RecordBatchStream struct layout (4 pointers = 32 bytes)
  private static final long FFI_RECORD_BATCH_STREAM_SIZE = 32;
  private static final long OFFSET_POLL_NEXT = 0;
  private static final long OFFSET_SCHEMA = 8;
  private static final long OFFSET_RELEASE = 16;
  private static final long OFFSET_PRIVATE_DATA = 24;

  // poll_next return type: FfiPoll<ROption<RResult<WrappedArray, RString>>> = 176 bytes
  private static final long POLL_RESULT_SIZE = 176;
  // Discriminant offsets within poll result
  private static final long POLL_DISC_OFFSET = 0; // FfiPoll discriminant (0=Ready)
  private static final long OPTION_DISC_OFFSET = 8; // ROption discriminant (0=RSome, 1=RNone)
  private static final long RESULT_DISC_OFFSET = 16; // RResult discriminant (0=ROk, 1=RErr)
  private static final long PAYLOAD_OFFSET = 24; // Start of WrappedArray or RString

  // WrappedSchema return type = FFI_ArrowSchema = 72 bytes
  private static final long WRAPPED_SCHEMA_SIZE = 72;

  // Struct layouts for return types — use JAVA_LONG (8-byte aligned) to satisfy
  // the FFM natural alignment requirement. 176 bytes = 22 longs, 72 bytes = 9 longs.
  private static final StructLayout POLL_RESULT_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(POLL_RESULT_SIZE / 8, ValueLayout.JAVA_LONG));

  private static final StructLayout WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(WRAPPED_SCHEMA_SIZE / 8, ValueLayout.JAVA_LONG));

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

  // Runtime-validated sizes (set once at first construction)
  private static volatile boolean sizesValidated = false;

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
      // Validate sizes against Rust at first use
      if (!sizesValidated) {
        validateSizes();
        sizesValidated = true;
      }

      // Pre-allocate return buffers in arena
      this.pollResultBuffer = arena.allocate(POLL_RESULT_SIZE, 8);
      this.schemaBuffer = arena.allocate(WRAPPED_SCHEMA_SIZE, 8);

      // Create upcall stubs
      this.pollNextStub = UpcallStub.create(POLL_NEXT_MH.bindTo(this), POLL_NEXT_DESC, arena);
      this.schemaStub = UpcallStub.create(SCHEMA_MH.bindTo(this), SCHEMA_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Allocate FFI_RecordBatchStream struct in Java arena memory
      this.ffiStream = arena.allocate(FFI_RECORD_BATCH_STREAM_SIZE, 8);
      ffiStream.set(ValueLayout.ADDRESS, OFFSET_POLL_NEXT, pollNextStub.segment());
      ffiStream.set(ValueLayout.ADDRESS, OFFSET_SCHEMA, schemaStub.segment());
      ffiStream.set(ValueLayout.ADDRESS, OFFSET_RELEASE, releaseStub.segment());
      ffiStream.set(ValueLayout.ADDRESS, OFFSET_PRIVATE_DATA, MemorySegment.NULL);

    } catch (Throwable e) {
      throw new DataFusionException("Failed to create RecordBatchReaderHandle", e);
    }
  }

  private static void validateSizes() {
    NativeUtil.validateSize(
        POLL_RESULT_SIZE, DataFusionBindings.POLL_RESULT_SIZE, "poll_next return type");
    NativeUtil.validateSize(
        WRAPPED_SCHEMA_SIZE, DataFusionBindings.WRAPPED_SCHEMA_SIZE, "WrappedSchema");
    NativeUtil.validateSize(
        FFI_RECORD_BATCH_STREAM_SIZE,
        DataFusionBindings.FFI_RECORD_BATCH_STREAM_SIZE,
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
    out.reinterpret(FFI_RECORD_BATCH_STREAM_SIZE).copyFrom(ffiStream);
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
        pollResultBuffer.set(ValueLayout.JAVA_BYTE, POLL_DISC_OFFSET, (byte) 0); // Ready
        pollResultBuffer.set(ValueLayout.JAVA_BYTE, OPTION_DISC_OFFSET, (byte) 1); // RNone
        return pollResultBuffer;
      }

      // FfiPoll::Ready(RSome(ROk(WrappedArray{array, schema})))
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, POLL_DISC_OFFSET, (byte) 0); // Ready
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, OPTION_DISC_OFFSET, (byte) 0); // RSome
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, RESULT_DISC_OFFSET, (byte) 0); // ROk

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
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, POLL_DISC_OFFSET, (byte) 0); // Ready
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, OPTION_DISC_OFFSET, (byte) 0); // RSome
      pollResultBuffer.set(ValueLayout.JAVA_BYTE, RESULT_DISC_OFFSET, (byte) 1); // RErr

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
