package org.apache.arrow.datafusion;

import java.util.List;

/**
 * Scan configuration passed from DataFusion to {@link FileSource#createFileOpener} containing
 * query-specific parameters extracted from the upstream {@code FileScanConfig} Rust struct.
 *
 * @param projection column indices to read (empty list means all columns)
 * @param limit maximum number of rows to return, or {@code null} for no limit
 * @param batchSize the batch size for reading, or {@code null} for the DataFusion default
 * @param partitionedByFileGroup whether the scan is partitioned by file group
 * @param partition the partition index being scanned
 */
public record FileScanConfig(
    List<Integer> projection,
    Long limit,
    Long batchSize,
    boolean partitionedByFileGroup,
    int partition) {}
