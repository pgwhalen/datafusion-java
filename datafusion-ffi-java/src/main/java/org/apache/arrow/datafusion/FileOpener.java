package org.apache.arrow.datafusion;

/**
 * Interface for opening file content and producing record batches.
 *
 * <p>A FileOpener is created by a {@link FileSource} with a specific schema and allocator already
 * bound. When DataFusion needs to read a file, it calls {@link #open(byte[])} with the raw file
 * bytes.
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
 *     public RecordBatchReader open(byte[] fileContent) {
 *         // Parse file content into record batches using schema and allocator
 *     }
 * }
 * }</pre>
 */
public interface FileOpener {
  /**
   * Opens file content and returns a RecordBatchReader yielding batches.
   *
   * @param fileContent raw bytes of the file
   * @return a reader that produces record batches
   */
  RecordBatchReader open(byte[] fileContent);
}
