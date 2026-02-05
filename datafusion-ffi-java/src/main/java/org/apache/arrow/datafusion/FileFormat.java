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
 */
public interface FileFormat {
  /**
   * Returns the file extension including dot (e.g., ".tsv", ".csv").
   *
   * @return the file extension
   */
  String getExtension();

  /**
   * Creates a FileSource that can produce FileOpeners for this format.
   *
   * @return a FileSource for this format
   */
  FileSource fileSource();
}
