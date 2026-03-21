package org.apache.arrow.datafusion;

/**
 * Interface for file format implementations that can parse file content into Arrow record batches.
 *
 * <p>Implement this interface to support custom file formats (e.g., TSV, JSON, Parquet) with
 * DataFusion's listing table feature. When a query scans a listing table, DataFusion calls {@link
 * #fileSource()} to obtain a {@link FileSource}, which in turn creates a {@link FileOpener} bound
 * to the table's schema and allocator.
 *
 * <p>Example:
 *
 * <pre>{@code
 * class TsvFormat implements FileFormat {
 *     @Override public String getExtension() { return ".tsv"; }
 *
 *     @Override
 *     public FileSource fileSource() {
 *         return new TsvFileSource();
 *     }
 * }
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_format/trait.FileFormat.html">Rust
 *     DataFusion: FileFormat</a>
 */
public interface FileFormat {
  /**
   * Returns the file extension including dot (e.g., ".tsv", ".csv").
   *
   * @return the file extension
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_format/trait.FileFormat.html#tymethod.get_ext">Rust
   *     DataFusion: FileFormat::get_ext</a>
   */
  String getExtension();

  /**
   * Creates a FileSource that can produce FileOpeners for this format.
   *
   * @return a FileSource for this format
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_format/trait.FileFormat.html#method.file_source">Rust
   *     DataFusion: FileFormat::file_source</a>
   */
  FileSource fileSource();
}
