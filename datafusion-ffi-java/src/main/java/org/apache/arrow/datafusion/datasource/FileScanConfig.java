package org.apache.arrow.datafusion.datasource;

import java.util.List;

/**
 * Scan configuration passed from DataFusion to {@link FileSource#createFileOpener} containing
 * query-specific parameters extracted from the upstream {@code FileScanConfig} Rust struct.
 *
 * <p>Example:
 *
 * <p>{@snippet : // FileScanConfig is provided by DataFusion to FileSource.createFileOpener
 * FileOpener opener = source.createFileOpener( schema, allocator, scanConfig); List<Integer>
 * projection = scanConfig.projection(); Long limit = scanConfig.limit(); }
 *
 * @param projection column indices to read (empty list means all columns)
 * @param limit maximum number of rows to return, or {@code null} for no limit
 * @param batchSize the batch size for reading, or {@code null} for the DataFusion default
 * @param partitionedByFileGroup whether the scan is partitioned by file group
 * @param partition the partition index being scanned
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_scan_config/struct.FileScanConfig.html">Rust
 *     DataFusion: FileScanConfig</a>
 */
public record FileScanConfig(
    List<Integer> projection,
    Long limit,
    Long batchSize,
    boolean partitionedByFileGroup,
    int partition) {

  /**
   * Returns the maximum number of rows to return, or {@code null} for no limit.
   *
   * @return the row limit, or null
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_scan_config/struct.FileScanConfig.html#structfield.limit">Rust
   *     DataFusion: FileScanConfig::limit</a>
   */
  public Long limit() {
    return limit;
  }

  /**
   * Returns the batch size for reading, or {@code null} for the DataFusion default.
   *
   * @return the batch size, or null
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_scan_config/struct.FileScanConfig.html#structfield.batch_size">Rust
   *     DataFusion: FileScanConfig::batch_size</a>
   */
  public Long batchSize() {
    return batchSize;
  }

  /**
   * Returns whether the scan is partitioned by file group.
   *
   * @return true if partitioned by file group
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/file_scan_config/struct.FileScanConfig.html#structfield.partitioned_by_file_group">Rust
   *     DataFusion: FileScanConfig::partitioned_by_file_group</a>
   */
  public boolean partitionedByFileGroup() {
    return partitionedByFileGroup;
  }
}
