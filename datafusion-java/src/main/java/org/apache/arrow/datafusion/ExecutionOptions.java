package org.apache.arrow.datafusion;

/**
 * Configures options related to query execution
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.config.ExecutionOptions} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("UnusedReturnValue")
public class ExecutionOptions {
  private final SessionConfig config;

  ExecutionOptions(SessionConfig config) {
    this.config = config;
  }

  /**
   * Get execution options related to reading Parquet data
   *
   * @return {@link ParquetOptions} instance for this config
   */
  public ParquetOptions parquet() {
    return new ParquetOptions(config);
  }

  /**
   * Get the batch size
   *
   * @return batch size
   */
  public long batchSize() {
    return config.batchSize;
  }

  /**
   * Set the size of batches to use when creating new data batches
   *
   * @param batchSize the batch size to set
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withBatchSize(long batchSize) {
    config.batchSize = batchSize;
    config.batchSizeSet = true;
    return this;
  }

  /**
   * Get whether batch coalescing is enabled
   *
   * @return whether batch coalescing is enabled
   */
  public boolean coalesceBatches() {
    return config.coalesceBatches;
  }

  /**
   * Set whether to enable batch coalescing
   *
   * @param enabled whether to enable batch coalescing
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withCoalesceBatches(boolean enabled) {
    config.coalesceBatches = enabled;
    config.coalesceBatchesSet = true;
    return this;
  }

  /**
   * Get whether statistics collection is enabled
   *
   * @return whether statistics collection is enabled
   */
  public boolean collectStatistics() {
    return config.collectStatistics;
  }

  /**
   * Set whether to enable statistics collection
   *
   * @param enabled whether to enable statistics collection
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withCollectStatistics(boolean enabled) {
    config.collectStatistics = enabled;
    config.collectStatisticsSet = true;
    return this;
  }

  /**
   * Get the target number of partitions
   *
   * @return number of partitions
   */
  public long targetPartitions() {
    return config.targetPartitions;
  }

  /**
   * Set the target number of partitions
   *
   * @param targetPartitions the number of partitions to set
   * @return the modified {@link ExecutionOptions} instance
   */
  public ExecutionOptions withTargetPartitions(long targetPartitions) {
    config.targetPartitions = targetPartitions;
    config.targetPartitionsSet = true;
    return this;
  }
}
