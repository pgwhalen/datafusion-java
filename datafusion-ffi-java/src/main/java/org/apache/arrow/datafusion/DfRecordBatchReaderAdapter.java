package org.apache.arrow.datafusion;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Adapts a user-implemented {@link RecordBatchReader} to the Diplomat-generated {@link
 * DfRecordBatchReaderTrait} interface for FFI callbacks.
 */
final class DfRecordBatchReaderAdapter implements DfRecordBatchReaderTrait, AutoCloseable {
  private final RecordBatchReader reader;
  private final BufferAllocator allocator;

  // Cached FFI schema
  private final ArrowSchema ffiSchema;
  private final long schemaAddr;

  private boolean closed = false;

  DfRecordBatchReaderAdapter(RecordBatchReader reader, BufferAllocator allocator) {
    this.reader = reader;
    this.allocator = allocator;
    this.ffiSchema = ArrowSchema.allocateNew(allocator);
    Data.exportSchema(allocator, reader.getVectorSchemaRoot().getSchema(), null, ffiSchema);
    this.schemaAddr = ffiSchema.memoryAddress();
  }

  /** Close the cached FFI schema after Rust has imported it (call after createRaw). */
  void closeFfiSchema() {
    ffiSchema.release();
    ffiSchema.close();
  }

  @Override
  public long schemaAddress() {
    return schemaAddr;
  }

  @Override
  public int next(long arrayOutAddr, long schemaOutAddr, long errorAddr, long errorCap) {
    try {
      if (!reader.loadNextBatch()) {
        close(); // Free VectorSchemaRoot resources
        return 0; // End of stream
      }

      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      // Export the current batch via Arrow C Data Interface
      try (ArrowSchema batchSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray batchArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, null, batchArray, batchSchema);

        // Copy the FFI structs to the Rust-provided output addresses
        java.lang.foreign.MemorySegment srcSchema =
            java.lang.foreign.MemorySegment.ofAddress(batchSchema.memoryAddress())
                .reinterpret(ARROW_SCHEMA_SIZE);
        java.lang.foreign.MemorySegment destSchema =
            java.lang.foreign.MemorySegment.ofAddress(schemaOutAddr).reinterpret(ARROW_SCHEMA_SIZE);
        destSchema.copyFrom(srcSchema);
        // Clear source release to prevent double-free (dest now owns it)
        srcSchema.set(
            java.lang.foreign.ValueLayout.ADDRESS, 64, java.lang.foreign.MemorySegment.NULL);

        java.lang.foreign.MemorySegment srcArray =
            java.lang.foreign.MemorySegment.ofAddress(batchArray.memoryAddress())
                .reinterpret(ARROW_ARRAY_SIZE);
        java.lang.foreign.MemorySegment destArray =
            java.lang.foreign.MemorySegment.ofAddress(arrayOutAddr).reinterpret(ARROW_ARRAY_SIZE);
        destArray.copyFrom(srcArray);
        // Clear source release to prevent double-free
        srcArray.set(
            java.lang.foreign.ValueLayout.ADDRESS, 64, java.lang.foreign.MemorySegment.NULL);
      }

      return 1; // Data available
    } catch (Exception e) {
      DfCatalogAdapter.writeError(errorAddr, errorCap, e.getMessage());
      return -1;
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        reader.close();
      } catch (Exception e) {
        // Ignore close errors
      }
    }
  }

  // FFI_ArrowSchema size (standard C Data Interface)
  private static final long ARROW_SCHEMA_SIZE = 72;
  // FFI_ArrowArray size (standard C Data Interface)
  private static final long ARROW_ARRAY_SIZE = 80;
}
