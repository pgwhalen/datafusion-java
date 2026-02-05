package org.apache.arrow.datafusion;

/**
 * Interface for opening files by path and producing record batches.
 *
 * <p>A FileOpener is created by a {@link FileSource} with a specific schema and allocator already
 * bound. When DataFusion needs to read a file, it calls {@link #open(String)} with the file path.
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
 *     public RecordBatchReader open(String filePath) {
 *         // Read file and parse into record batches using schema and allocator
 *     }
 * }
 * }</pre>
 */
public interface FileOpener {
  /**
   * Opens a file by path and returns a RecordBatchReader yielding batches.
   *
   * @param filePath absolute path to the file
   * @return a reader that produces record batches
   */
  RecordBatchReader open(String filePath);
}
