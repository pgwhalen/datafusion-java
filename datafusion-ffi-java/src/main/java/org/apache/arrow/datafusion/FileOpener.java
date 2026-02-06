package org.apache.arrow.datafusion;

/**
 * Interface for opening files and producing record batches.
 *
 * <p>A FileOpener is created by a {@link FileSource} with a specific schema and allocator already
 * bound. When DataFusion needs to read a file, it calls {@link #open(PartitionedFile)} with the
 * file metadata.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class TsvFileOpener implements FileOpener {
 *     private final Schema schema;
 *     private final BufferAllocator allocator;
 *
 *     TsvFileOpener(Schema schema, BufferAllocator allocator) {
 *         this.schema = schema;
 *         this.allocator = allocator;
 *     }
 *
 *     @Override
 *     public RecordBatchReader open(PartitionedFile file) {
 *         // Read file and parse into record batches using schema and allocator
 *     }
 * }
 * }</pre>
 */
public interface FileOpener {
  /**
   * Opens a file and returns a RecordBatchReader yielding batches.
   *
   * @param file the partitioned file metadata including path, size, and optional byte range
   * @return a reader that produces record batches
   */
  RecordBatchReader open(PartitionedFile file);
}
