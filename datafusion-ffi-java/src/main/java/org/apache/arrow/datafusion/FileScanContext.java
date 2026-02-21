package org.apache.arrow.datafusion;

import java.util.List;

/**
 * Scan context passed from DataFusion to {@link FileSource#createFileOpener} containing
 * query-specific parameters such as column projection, row limit, and partition index.
 *
 * @param projection column indices to read (empty list means all columns)
 * @param limit maximum number of rows to return, or {@code null} for no limit
 * @param partition the partition index being scanned
 */
public record FileScanContext(List<Integer> projection, Long limit, int partition) {}
