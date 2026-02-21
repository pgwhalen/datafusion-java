package org.apache.arrow.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Interface for creating file openers that can parse files of a specific format.
 *
 * <p>A FileSource is created by a {@link FileFormat} and is responsible for creating {@link
 * FileOpener} instances that are bound to a particular schema and allocator. This level of
 * indirection allows the opener to capture the schema and allocator without requiring them as
 * parameters on every file open call.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class TsvFileSource implements FileSource {
 *     @Override
 *     public FileOpener createFileOpener(Schema schema, BufferAllocator allocator,
 *                                        FileScanContext scanContext) {
 *         return new TsvFileOpener(schema, allocator);
 *     }
 *
 *     @Override
 *     public String fileType() {
 *         return "tsv";
 *     }
 * }
 * }</pre>
 */
public interface FileSource {
  /**
   * Creates a FileOpener bound to the given schema, allocator, and scan context.
   *
   * <p>The scan context provides query-specific parameters such as column projection, row limit,
   * and partition index that implementations can use to optimize file reading.
   *
   * @param schema the expected output schema
   * @param allocator allocator for Arrow vectors
   * @param scanContext scan context with projection, limit, and partition info
   * @return a FileOpener that can parse file content into record batches
   */
  FileOpener createFileOpener(
      Schema schema, BufferAllocator allocator, FileScanContext scanContext);

  /**
   * Returns a string identifier for this file source's type.
   *
   * <p>This is used by DataFusion for display and logging purposes. Common values include format
   * names like "csv", "json", "parquet", etc.
   *
   * @return the file type identifier
   */
  String fileType();
}
