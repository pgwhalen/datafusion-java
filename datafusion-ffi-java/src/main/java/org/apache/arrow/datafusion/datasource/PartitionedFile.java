package org.apache.arrow.datafusion.datasource;

/**
 * Represents a partitioned file from DataFusion's listing table scan.
 *
 * <p>Contains the file path, size in bytes, and an optional byte range for partial file reads.
 *
 * @param path absolute path to the file
 * @param size file size in bytes
 * @param rangeStart start of byte range (null if no range)
 * @param rangeEnd end of byte range (null if no range)
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.PartitionedFile.html">Rust
 *     DataFusion: PartitionedFile</a>
 */
public record PartitionedFile(String path, long size, Long rangeStart, Long rangeEnd) {

  /**
   * Returns the absolute path to the file.
   *
   * @return the file path
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.PartitionedFile.html#method.path">Rust
   *     DataFusion: PartitionedFile::path</a>
   */
  public String path() {
    return path;
  }
}
