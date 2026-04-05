package org.apache.arrow.datafusion.datasource;

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
 * <p>{@snippet : class TsvFileSource implements FileSource { @Override public FileOpener
 * createFileOpener(Schema schema, BufferAllocator allocator, FileScanConfig scanConfig) { return
 * new TsvFileOpener(schema, allocator); } @Override public String fileType() { return "tsv"; } } }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file/trait.FileSource.html">Rust
 *     DataFusion: FileSource</a>
 */
public interface FileSource {
  /**
   * Creates a FileOpener bound to the given schema, allocator, and scan configuration.
   *
   * <p>The scan configuration provides query-specific parameters such as column projection, row
   * limit, batch size, and partition index that implementations can use to optimize file reading.
   *
   * <p>Example:
   *
   * <p>{@snippet : FileSource source = myFormat.fileSource(); FileOpener opener =
   * source.createFileOpener( schema, allocator, scanConfig); RecordBatchReader reader =
   * opener.open( partitionedFile); }
   *
   * @param schema the expected output schema
   * @param allocator allocator for Arrow vectors
   * @param scanConfig scan configuration with projection, limit, batch size, and partition info
   * @return a FileOpener that can parse file content into record batches
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file/trait.FileSource.html#method.create_file_opener">Rust
   *     DataFusion: FileSource::create_file_opener</a>
   */
  FileOpener createFileOpener(Schema schema, BufferAllocator allocator, FileScanConfig scanConfig);

  /**
   * Returns a string identifier for this file source's type.
   *
   * <p>This is used by DataFusion for display and logging purposes. Common values include format
   * names like "csv", "json", "parquet", etc.
   *
   * <p>Example:
   *
   * <p>{@snippet : FileSource source = myFormat.fileSource(); String type = source.fileType(); //
   * e.g., "tsv" System.out.println("File source type: " + type); }
   *
   * @return the file type identifier
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file/trait.FileSource.html#method.file_type">Rust
   *     DataFusion: FileSource::file_type</a>
   */
  String fileType();
}
