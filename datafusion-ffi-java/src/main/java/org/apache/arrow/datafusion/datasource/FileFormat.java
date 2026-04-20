package org.apache.arrow.datafusion.datasource;

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
 * {@snippet :
 * class TsvFormat implements FileFormat {
 *     @Override public String getExtension() {
 *         return ".tsv";
 *     }
 *     @Override public FileSource fileSource() {
 *         return new TsvFileSource();
 *     }
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/53.1.0/datafusion_datasource/file_format/trait.FileFormat.html">Rust
 *     DataFusion: FileFormat</a>
 */
public interface FileFormat {
  /**
   * Returns the file extension including dot (e.g., ".tsv", ".csv").
   *
   * <p>Example:
   *
   * {@snippet :
   * FileFormat format = new TsvFormat();
   * String ext = format.getExtension(); // ".tsv"
   * ListingOptions options = ListingOptions.builder(format)
   *     .build();
   * }
   *
   * @return the file extension
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/53.1.0/datafusion_datasource/file_format/trait.FileFormat.html#tymethod.get_ext">Rust
   *     DataFusion: FileFormat::get_ext</a>
   */
  String getExtension();

  /**
   * Creates a FileSource that can produce FileOpeners for this format.
   *
   * <p>Example:
   *
   * {@snippet :
   * FileFormat format = new TsvFormat();
   * FileSource source = format.fileSource();
   * FileOpener opener = source.createFileOpener(
   *     schema, allocator, scanConfig);
   * }
   *
   * @return a FileSource for this format
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/53.1.0/datafusion_datasource/file_format/trait.FileFormat.html#method.file_source">Rust
   *     DataFusion: FileFormat::file_source</a>
   */
  FileSource fileSource();
}
