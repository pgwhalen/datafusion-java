package org.apache.arrow.datafusion.config;

import java.util.Map;

/**
 * Execution configuration options. Maps to DataFusion's {@code ExecutionOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param batchSize Number of rows processed per batch (default 8192)
 * @param coalesceBatches Whether to coalesce small batches into larger ones
 * @param collectStatistics Whether to collect statistics during query execution
 * @param targetPartitions Number of target partitions for repartitioning (default: CPU cores)
 * @param timeZone Default time zone for temporal operations (default "+00:00")
 * @param planningConcurrency Number of threads to use for planning concurrency
 * @param skipPhysicalAggregateSchemaCheck Whether to skip the physical aggregate schema check
 * @param spillCompression Compression codec used for spilling intermediate results to disk
 * @param sortSpillReservationBytes Memory reserved for sort spill operations in bytes
 * @param sortInPlaceThresholdBytes Threshold below which sorts are performed in-place (in bytes)
 * @param maxSpillFileSizeBytes Maximum spill file size in bytes
 * @param metaFetchConcurrency Number of concurrent metadata fetch operations
 * @param minimumParallelOutputFiles Minimum number of parallel output files
 * @param softMaxRowsPerOutputFile Soft maximum number of rows per output file
 * @param maxBufferedBatchesPerOutputFile Maximum number of buffered batches per output file
 * @param listingTableIgnoreSubdirectory Whether to ignore subdirectories when listing table files
 * @param listingTableFactoryInferPartitions Whether to infer partitions from directory structure
 * @param enableRecursiveCtes Whether to enable recursive common table expressions
 * @param splitFileGroupsByStatistics Whether to split file groups by statistics for better
 *     parallelism
 * @param keepPartitionByColumns Whether to keep partition-by columns in query output
 * @param skipPartialAggregationProbeRatioThreshold Probe ratio threshold for skipping partial
 *     aggregation
 * @param skipPartialAggregationProbeRowsThreshold Probe rows threshold for skipping partial
 *     aggregation
 * @param useRowNumberEstimatesToOptimizePartitioning Whether to use row number estimates to
 *     optimize partitioning
 * @param enforceBatchSizeInJoins Whether to enforce batch size limits in join operations
 * @param objectstoreWriterBufferSize Buffer size for object store writer in bytes
 * @param enableAnsiMode Whether to enable ANSI SQL mode
 * @param perfectHashJoinSmallBuildThreshold Maximum entries for "small" side in a perfect hash join
 * @param perfectHashJoinMinKeyDensity Minimum key density for perfect hash join eligibility
 * @param parquet Parquet-specific options
 */
public record ExecutionOptions(
    Integer batchSize,
    Boolean coalesceBatches,
    Boolean collectStatistics,
    Integer targetPartitions,
    String timeZone,
    Integer planningConcurrency,
    Boolean skipPhysicalAggregateSchemaCheck,
    SpillCompression spillCompression,
    Long sortSpillReservationBytes,
    Long sortInPlaceThresholdBytes,
    Long maxSpillFileSizeBytes,
    Integer metaFetchConcurrency,
    Integer minimumParallelOutputFiles,
    Long softMaxRowsPerOutputFile,
    Integer maxBufferedBatchesPerOutputFile,
    Boolean listingTableIgnoreSubdirectory,
    Boolean listingTableFactoryInferPartitions,
    Boolean enableRecursiveCtes,
    Boolean splitFileGroupsByStatistics,
    Boolean keepPartitionByColumns,
    Double skipPartialAggregationProbeRatioThreshold,
    Integer skipPartialAggregationProbeRowsThreshold,
    Boolean useRowNumberEstimatesToOptimizePartitioning,
    Boolean enforceBatchSizeInJoins,
    Long objectstoreWriterBufferSize,
    Boolean enableAnsiMode,
    Integer perfectHashJoinSmallBuildThreshold,
    Double perfectHashJoinMinKeyDensity,
    ParquetOptions parquet) {

  private static final String PREFIX = "datafusion.execution.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "batch_size", batchSize);
    putIfPresent(map, PREFIX + "coalesce_batches", coalesceBatches);
    putIfPresent(map, PREFIX + "collect_statistics", collectStatistics);
    putIfPresent(map, PREFIX + "target_partitions", targetPartitions);
    putIfPresent(map, PREFIX + "time_zone", timeZone);
    putIfPresent(map, PREFIX + "planning_concurrency", planningConcurrency);
    putIfPresent(
        map, PREFIX + "skip_physical_aggregate_schema_check", skipPhysicalAggregateSchemaCheck);
    if (spillCompression != null) {
      map.put(PREFIX + "spill_compression", spillCompression.toConfigValue());
    }
    putIfPresent(map, PREFIX + "sort_spill_reservation_bytes", sortSpillReservationBytes);
    putIfPresent(map, PREFIX + "sort_in_place_threshold_bytes", sortInPlaceThresholdBytes);
    putIfPresent(map, PREFIX + "max_spill_file_size_bytes", maxSpillFileSizeBytes);
    putIfPresent(map, PREFIX + "meta_fetch_concurrency", metaFetchConcurrency);
    putIfPresent(map, PREFIX + "minimum_parallel_output_files", minimumParallelOutputFiles);
    putIfPresent(map, PREFIX + "soft_max_rows_per_output_file", softMaxRowsPerOutputFile);
    putIfPresent(
        map, PREFIX + "max_buffered_batches_per_output_file", maxBufferedBatchesPerOutputFile);
    putIfPresent(map, PREFIX + "listing_table_ignore_subdirectory", listingTableIgnoreSubdirectory);
    putIfPresent(
        map, PREFIX + "listing_table_factory_infer_partitions", listingTableFactoryInferPartitions);
    putIfPresent(map, PREFIX + "enable_recursive_ctes", enableRecursiveCtes);
    putIfPresent(map, PREFIX + "split_file_groups_by_statistics", splitFileGroupsByStatistics);
    putIfPresent(map, PREFIX + "keep_partition_by_columns", keepPartitionByColumns);
    putIfPresent(
        map,
        PREFIX + "skip_partial_aggregation_probe_ratio_threshold",
        skipPartialAggregationProbeRatioThreshold);
    putIfPresent(
        map,
        PREFIX + "skip_partial_aggregation_probe_rows_threshold",
        skipPartialAggregationProbeRowsThreshold);
    putIfPresent(
        map,
        PREFIX + "use_row_number_estimates_to_optimize_partitioning",
        useRowNumberEstimatesToOptimizePartitioning);
    putIfPresent(map, PREFIX + "enforce_batch_size_in_joins", enforceBatchSizeInJoins);
    putIfPresent(map, PREFIX + "objectstore_writer_buffer_size", objectstoreWriterBufferSize);
    putIfPresent(map, PREFIX + "enable_ansi_mode", enableAnsiMode);
    putIfPresent(
        map,
        PREFIX + "perfect_hash_join_small_build_threshold",
        perfectHashJoinSmallBuildThreshold);
    putIfPresent(map, PREFIX + "perfect_hash_join_min_key_density", perfectHashJoinMinKeyDensity);
    if (parquet != null) {
      parquet.writeTo(map);
    }
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link ExecutionOptions}. */
  public static final class Builder {
    private Integer batchSize;
    private Boolean coalesceBatches;
    private Boolean collectStatistics;
    private Integer targetPartitions;
    private String timeZone;
    private Integer planningConcurrency;
    private Boolean skipPhysicalAggregateSchemaCheck;
    private SpillCompression spillCompression;
    private Long sortSpillReservationBytes;
    private Long sortInPlaceThresholdBytes;
    private Long maxSpillFileSizeBytes;
    private Integer metaFetchConcurrency;
    private Integer minimumParallelOutputFiles;
    private Long softMaxRowsPerOutputFile;
    private Integer maxBufferedBatchesPerOutputFile;
    private Boolean listingTableIgnoreSubdirectory;
    private Boolean listingTableFactoryInferPartitions;
    private Boolean enableRecursiveCtes;
    private Boolean splitFileGroupsByStatistics;
    private Boolean keepPartitionByColumns;
    private Double skipPartialAggregationProbeRatioThreshold;
    private Integer skipPartialAggregationProbeRowsThreshold;
    private Boolean useRowNumberEstimatesToOptimizePartitioning;
    private Boolean enforceBatchSizeInJoins;
    private Long objectstoreWriterBufferSize;
    private Boolean enableAnsiMode;
    private Integer perfectHashJoinSmallBuildThreshold;
    private Double perfectHashJoinMinKeyDensity;
    private ParquetOptions parquet;

    private Builder() {}

    /** Number of rows processed per batch. Default is 8192. */
    public Builder batchSize(int value) {
      this.batchSize = value;
      return this;
    }

    /** Whether to coalesce small batches into larger ones. */
    public Builder coalesceBatches(boolean value) {
      this.coalesceBatches = value;
      return this;
    }

    /** Whether to collect statistics during query execution. */
    public Builder collectStatistics(boolean value) {
      this.collectStatistics = value;
      return this;
    }

    /** Number of target partitions for repartitioning. Default is the number of CPU cores. */
    public Builder targetPartitions(int value) {
      this.targetPartitions = value;
      return this;
    }

    /** Default time zone for temporal operations. Default is "+00:00" (UTC). */
    public Builder timeZone(String value) {
      this.timeZone = value;
      return this;
    }

    /** Number of threads to use for planning concurrency. */
    public Builder planningConcurrency(int value) {
      this.planningConcurrency = value;
      return this;
    }

    /** Whether to skip the physical aggregate schema check. */
    public Builder skipPhysicalAggregateSchemaCheck(boolean value) {
      this.skipPhysicalAggregateSchemaCheck = value;
      return this;
    }

    /** Compression codec used for spilling intermediate results to disk. */
    public Builder spillCompression(SpillCompression value) {
      this.spillCompression = value;
      return this;
    }

    /** Memory reserved for sort spill operations in bytes. */
    public Builder sortSpillReservationBytes(long value) {
      this.sortSpillReservationBytes = value;
      return this;
    }

    /** Threshold below which sorts are performed in-place (in bytes). */
    public Builder sortInPlaceThresholdBytes(long value) {
      this.sortInPlaceThresholdBytes = value;
      return this;
    }

    /** Maximum spill file size in bytes. */
    public Builder maxSpillFileSizeBytes(long value) {
      this.maxSpillFileSizeBytes = value;
      return this;
    }

    /** Number of concurrent metadata fetch operations. */
    public Builder metaFetchConcurrency(int value) {
      this.metaFetchConcurrency = value;
      return this;
    }

    /** Minimum number of parallel output files. */
    public Builder minimumParallelOutputFiles(int value) {
      this.minimumParallelOutputFiles = value;
      return this;
    }

    /** Soft maximum number of rows per output file. */
    public Builder softMaxRowsPerOutputFile(long value) {
      this.softMaxRowsPerOutputFile = value;
      return this;
    }

    /** Maximum number of buffered batches per output file. */
    public Builder maxBufferedBatchesPerOutputFile(int value) {
      this.maxBufferedBatchesPerOutputFile = value;
      return this;
    }

    /** Whether to ignore subdirectories when listing table files. */
    public Builder listingTableIgnoreSubdirectory(boolean value) {
      this.listingTableIgnoreSubdirectory = value;
      return this;
    }

    /** Whether to infer partitions from directory structure. */
    public Builder listingTableFactoryInferPartitions(boolean value) {
      this.listingTableFactoryInferPartitions = value;
      return this;
    }

    /** Whether to enable recursive common table expressions. */
    public Builder enableRecursiveCtes(boolean value) {
      this.enableRecursiveCtes = value;
      return this;
    }

    /** Whether to split file groups by statistics for better parallelism. */
    public Builder splitFileGroupsByStatistics(boolean value) {
      this.splitFileGroupsByStatistics = value;
      return this;
    }

    /** Whether to keep partition-by columns in query output. */
    public Builder keepPartitionByColumns(boolean value) {
      this.keepPartitionByColumns = value;
      return this;
    }

    /** Probe ratio threshold for skipping partial aggregation. */
    public Builder skipPartialAggregationProbeRatioThreshold(double value) {
      this.skipPartialAggregationProbeRatioThreshold = value;
      return this;
    }

    /** Probe rows threshold for skipping partial aggregation. */
    public Builder skipPartialAggregationProbeRowsThreshold(int value) {
      this.skipPartialAggregationProbeRowsThreshold = value;
      return this;
    }

    /** Whether to use row number estimates to optimize partitioning. */
    public Builder useRowNumberEstimatesToOptimizePartitioning(boolean value) {
      this.useRowNumberEstimatesToOptimizePartitioning = value;
      return this;
    }

    /** Whether to enforce batch size limits in join operations. */
    public Builder enforceBatchSizeInJoins(boolean value) {
      this.enforceBatchSizeInJoins = value;
      return this;
    }

    /** Buffer size for object store writer in bytes. */
    public Builder objectstoreWriterBufferSize(long value) {
      this.objectstoreWriterBufferSize = value;
      return this;
    }

    /** Whether to enable ANSI SQL mode. */
    public Builder enableAnsiMode(boolean value) {
      this.enableAnsiMode = value;
      return this;
    }

    /** Maximum number of entries for the "small" side in a perfect hash join. */
    public Builder perfectHashJoinSmallBuildThreshold(int value) {
      this.perfectHashJoinSmallBuildThreshold = value;
      return this;
    }

    /** Minimum key density for perfect hash join eligibility. */
    public Builder perfectHashJoinMinKeyDensity(double value) {
      this.perfectHashJoinMinKeyDensity = value;
      return this;
    }

    /** Parquet-specific options. */
    public Builder parquet(ParquetOptions value) {
      this.parquet = value;
      return this;
    }

    /** Builds the {@link ExecutionOptions}. */
    public ExecutionOptions build() {
      return new ExecutionOptions(
          batchSize,
          coalesceBatches,
          collectStatistics,
          targetPartitions,
          timeZone,
          planningConcurrency,
          skipPhysicalAggregateSchemaCheck,
          spillCompression,
          sortSpillReservationBytes,
          sortInPlaceThresholdBytes,
          maxSpillFileSizeBytes,
          metaFetchConcurrency,
          minimumParallelOutputFiles,
          softMaxRowsPerOutputFile,
          maxBufferedBatchesPerOutputFile,
          listingTableIgnoreSubdirectory,
          listingTableFactoryInferPartitions,
          enableRecursiveCtes,
          splitFileGroupsByStatistics,
          keepPartitionByColumns,
          skipPartialAggregationProbeRatioThreshold,
          skipPartialAggregationProbeRowsThreshold,
          useRowNumberEstimatesToOptimizePartitioning,
          enforceBatchSizeInJoins,
          objectstoreWriterBufferSize,
          enableAnsiMode,
          perfectHashJoinSmallBuildThreshold,
          perfectHashJoinMinKeyDensity,
          parquet);
    }
  }
}
