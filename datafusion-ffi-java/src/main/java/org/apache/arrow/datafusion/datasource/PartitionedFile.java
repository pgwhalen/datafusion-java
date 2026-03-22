package org.apache.arrow.datafusion.datasource;

/**
 * Represents a partitioned file from DataFusion's listing table scan.
 *
 * <p>Contains the file's object metadata and an optional byte range for partial file reads.
 *
 * @param objectMeta metadata for the file including its location and size
 * @param range byte range to read, or {@code null} to read the entire file
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.PartitionedFile.html">Rust
 *     DataFusion: PartitionedFile</a>
 */
public record PartitionedFile(ObjectMeta objectMeta, FileRange range) {

  /**
   * Returns metadata for the file including its location and size.
   *
   * @return the object metadata
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.PartitionedFile.html#structfield.object_meta">Rust
   *     DataFusion: PartitionedFile::object_meta</a>
   */
  public ObjectMeta objectMeta() {
    return objectMeta;
  }

  /**
   * Returns the byte range to read, or {@code null} to read the entire file.
   *
   * @return the file range, or null
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.PartitionedFile.html#structfield.range">Rust
   *     DataFusion: PartitionedFile::range</a>
   */
  public FileRange range() {
    return range;
  }
}
