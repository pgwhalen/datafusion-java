package org.apache.arrow.datafusion;

/**
 * Represents a partitioned file from DataFusion's listing table scan.
 *
 * <p>Contains the file path, size in bytes, and an optional byte range for partial file reads.
 *
 * @param path absolute path to the file
 * @param size file size in bytes
 * @param rangeStart start of byte range (null if no range)
 * @param rangeEnd end of byte range (null if no range)
 */
public record PartitionedFile(String path, long size, Long rangeStart, Long rangeEnd) {}
