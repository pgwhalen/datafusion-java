package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Utility class for exporting Arrow data to the C Data Interface for use with FFM.
 *
 * <p>This class provides a bridge between Arrow Java's VectorSchemaRoot and the memory addresses
 * needed for the Foreign Function and Memory API.
 */
final class ArrowExporter {

  private ArrowExporter() {}

  /**
   * Exports a VectorSchemaRoot to Arrow C Data Interface structures.
   *
   * @param allocator The buffer allocator
   * @param root The VectorSchemaRoot to export
   * @return An ExportResult containing the FFI structures and their memory addresses
   */
  static ExportResult export(BufferAllocator allocator, VectorSchemaRoot root) {
    ArrowSchema schema = ArrowSchema.allocateNew(allocator);
    ArrowArray array = ArrowArray.allocateNew(allocator);

    Data.exportVectorSchemaRoot(allocator, root, null, array, schema);

    return new ExportResult(
        MemorySegment.ofAddress(schema.memoryAddress()),
        MemorySegment.ofAddress(array.memoryAddress()),
        schema,
        array);
  }

  /** Result of exporting Arrow data to C Data Interface. */
  static class ExportResult implements AutoCloseable {
    private final MemorySegment schemaAddress;
    private final MemorySegment arrayAddress;
    private final ArrowSchema schema;
    private final ArrowArray array;

    ExportResult(
        MemorySegment schemaAddress,
        MemorySegment arrayAddress,
        ArrowSchema schema,
        ArrowArray array) {
      this.schemaAddress = schemaAddress;
      this.arrayAddress = arrayAddress;
      this.schema = schema;
      this.array = array;
    }

    /**
     * Gets the memory address of the exported schema.
     *
     * @return The schema memory segment
     */
    MemorySegment getSchemaAddress() {
      return schemaAddress;
    }

    /**
     * Gets the memory address of the exported array.
     *
     * @return The array memory segment
     */
    MemorySegment getArrayAddress() {
      return arrayAddress;
    }

    @Override
    public void close() {
      schema.close();
      array.close();
    }
  }
}
