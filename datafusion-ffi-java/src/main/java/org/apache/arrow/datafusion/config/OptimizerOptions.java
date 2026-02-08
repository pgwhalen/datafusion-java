package org.apache.arrow.datafusion.config;

import java.util.Map;

/**
 * Optimizer configuration options. Maps to DataFusion's {@code OptimizerOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param enableDistinctAggregationSoftLimit Whether to enable the distinct aggregation soft limit
 *     optimization
 * @param enableRoundRobinRepartition Whether to use round-robin repartitioning (default true)
 * @param enableTopkAggregation Whether to enable top-K aggregation optimization (default true)
 * @param enableWindowLimits Whether to enable window function limit optimizations
 * @param enableTopkDynamicFilterPushdown Whether to enable top-K dynamic filter pushdown
 * @param enableJoinDynamicFilterPushdown Whether to enable join dynamic filter pushdown
 * @param enableAggregateDynamicFilterPushdown Whether to enable aggregate dynamic filter pushdown
 * @param enableDynamicFilterPushdown Whether to enable dynamic filter pushdown
 * @param filterNullJoinKeys Whether to filter null join keys during planning (default true)
 * @param repartitionAggregations Whether to repartition aggregation inputs (default true)
 * @param repartitionFileMinSize Minimum file size (in bytes) for file repartitioning
 * @param repartitionJoins Whether to repartition join inputs (default true)
 * @param allowSymmetricJoinsWithoutPruning Whether to allow symmetric joins without pruning
 * @param repartitionFileScans Whether to repartition file scans (default true)
 * @param preserveFilePartitions Number of file partitions to preserve
 * @param repartitionWindows Whether to repartition window function inputs (default true)
 * @param repartitionSorts Whether to repartition sort inputs (default true)
 * @param subsetRepartitionThreshold Subset repartition threshold
 * @param preferExistingSort Whether to prefer existing sort order
 * @param skipFailedRules Whether to skip failed optimizer rules instead of erroring (default true)
 * @param maxPasses Maximum number of optimizer passes (default 3)
 * @param topDownJoinKeyReordering Whether to use top-down join key reordering (default true)
 * @param preferHashJoin Whether to prefer hash joins over sort-merge joins (default true)
 * @param enablePiecewiseMergeJoin Whether to enable piecewise merge join
 * @param hashJoinSinglePartitionThreshold Threshold (in bytes) for using a single partition in hash
 *     joins
 * @param hashJoinSinglePartitionThresholdRows Threshold (in rows) for using a single partition in
 *     hash joins
 * @param hashJoinInlistPushdownMaxSize Maximum in-list size for hash join pushdown (in bytes)
 * @param hashJoinInlistPushdownMaxDistinctValues Maximum distinct values for hash join in-list
 *     pushdown
 * @param defaultFilterSelectivity Default filter selectivity percentage (0-100, default 20)
 * @param preferExistingUnion Whether to prefer existing union ordering
 * @param expandViewsAtOutput Whether to expand views at the output level
 * @param enableSortPushdown Whether to enable sort pushdown optimization
 */
public record OptimizerOptions(
    Boolean enableDistinctAggregationSoftLimit,
    Boolean enableRoundRobinRepartition,
    Boolean enableTopkAggregation,
    Boolean enableWindowLimits,
    Boolean enableTopkDynamicFilterPushdown,
    Boolean enableJoinDynamicFilterPushdown,
    Boolean enableAggregateDynamicFilterPushdown,
    Boolean enableDynamicFilterPushdown,
    Boolean filterNullJoinKeys,
    Boolean repartitionAggregations,
    Long repartitionFileMinSize,
    Boolean repartitionJoins,
    Boolean allowSymmetricJoinsWithoutPruning,
    Boolean repartitionFileScans,
    Integer preserveFilePartitions,
    Boolean repartitionWindows,
    Boolean repartitionSorts,
    Integer subsetRepartitionThreshold,
    Boolean preferExistingSort,
    Boolean skipFailedRules,
    Integer maxPasses,
    Boolean topDownJoinKeyReordering,
    Boolean preferHashJoin,
    Boolean enablePiecewiseMergeJoin,
    Long hashJoinSinglePartitionThreshold,
    Long hashJoinSinglePartitionThresholdRows,
    Long hashJoinInlistPushdownMaxSize,
    Integer hashJoinInlistPushdownMaxDistinctValues,
    Integer defaultFilterSelectivity,
    Boolean preferExistingUnion,
    Boolean expandViewsAtOutput,
    Boolean enableSortPushdown) {

  private static final String PREFIX = "datafusion.optimizer.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(
        map, PREFIX + "enable_distinct_aggregation_soft_limit", enableDistinctAggregationSoftLimit);
    putIfPresent(map, PREFIX + "enable_round_robin_repartition", enableRoundRobinRepartition);
    putIfPresent(map, PREFIX + "enable_topk_aggregation", enableTopkAggregation);
    putIfPresent(map, PREFIX + "enable_window_limits", enableWindowLimits);
    putIfPresent(
        map, PREFIX + "enable_topk_dynamic_filter_pushdown", enableTopkDynamicFilterPushdown);
    putIfPresent(
        map, PREFIX + "enable_join_dynamic_filter_pushdown", enableJoinDynamicFilterPushdown);
    putIfPresent(
        map,
        PREFIX + "enable_aggregate_dynamic_filter_pushdown",
        enableAggregateDynamicFilterPushdown);
    putIfPresent(map, PREFIX + "enable_dynamic_filter_pushdown", enableDynamicFilterPushdown);
    putIfPresent(map, PREFIX + "filter_null_join_keys", filterNullJoinKeys);
    putIfPresent(map, PREFIX + "repartition_aggregations", repartitionAggregations);
    putIfPresent(map, PREFIX + "repartition_file_min_size", repartitionFileMinSize);
    putIfPresent(map, PREFIX + "repartition_joins", repartitionJoins);
    putIfPresent(
        map, PREFIX + "allow_symmetric_joins_without_pruning", allowSymmetricJoinsWithoutPruning);
    putIfPresent(map, PREFIX + "repartition_file_scans", repartitionFileScans);
    putIfPresent(map, PREFIX + "preserve_file_partitions", preserveFilePartitions);
    putIfPresent(map, PREFIX + "repartition_windows", repartitionWindows);
    putIfPresent(map, PREFIX + "repartition_sorts", repartitionSorts);
    putIfPresent(map, PREFIX + "subset_repartition_threshold", subsetRepartitionThreshold);
    putIfPresent(map, PREFIX + "prefer_existing_sort", preferExistingSort);
    putIfPresent(map, PREFIX + "skip_failed_rules", skipFailedRules);
    putIfPresent(map, PREFIX + "max_passes", maxPasses);
    putIfPresent(map, PREFIX + "top_down_join_key_reordering", topDownJoinKeyReordering);
    putIfPresent(map, PREFIX + "prefer_hash_join", preferHashJoin);
    putIfPresent(map, PREFIX + "enable_piecewise_merge_join", enablePiecewiseMergeJoin);
    putIfPresent(
        map, PREFIX + "hash_join_single_partition_threshold", hashJoinSinglePartitionThreshold);
    putIfPresent(
        map,
        PREFIX + "hash_join_single_partition_threshold_rows",
        hashJoinSinglePartitionThresholdRows);
    putIfPresent(map, PREFIX + "hash_join_inlist_pushdown_max_size", hashJoinInlistPushdownMaxSize);
    putIfPresent(
        map,
        PREFIX + "hash_join_inlist_pushdown_max_distinct_values",
        hashJoinInlistPushdownMaxDistinctValues);
    putIfPresent(map, PREFIX + "default_filter_selectivity", defaultFilterSelectivity);
    putIfPresent(map, PREFIX + "prefer_existing_union", preferExistingUnion);
    putIfPresent(map, PREFIX + "expand_views_at_output", expandViewsAtOutput);
    putIfPresent(map, PREFIX + "enable_sort_pushdown", enableSortPushdown);
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link OptimizerOptions}. */
  public static final class Builder {
    private Boolean enableDistinctAggregationSoftLimit;
    private Boolean enableRoundRobinRepartition;
    private Boolean enableTopkAggregation;
    private Boolean enableWindowLimits;
    private Boolean enableTopkDynamicFilterPushdown;
    private Boolean enableJoinDynamicFilterPushdown;
    private Boolean enableAggregateDynamicFilterPushdown;
    private Boolean enableDynamicFilterPushdown;
    private Boolean filterNullJoinKeys;
    private Boolean repartitionAggregations;
    private Long repartitionFileMinSize;
    private Boolean repartitionJoins;
    private Boolean allowSymmetricJoinsWithoutPruning;
    private Boolean repartitionFileScans;
    private Integer preserveFilePartitions;
    private Boolean repartitionWindows;
    private Boolean repartitionSorts;
    private Integer subsetRepartitionThreshold;
    private Boolean preferExistingSort;
    private Boolean skipFailedRules;
    private Integer maxPasses;
    private Boolean topDownJoinKeyReordering;
    private Boolean preferHashJoin;
    private Boolean enablePiecewiseMergeJoin;
    private Long hashJoinSinglePartitionThreshold;
    private Long hashJoinSinglePartitionThresholdRows;
    private Long hashJoinInlistPushdownMaxSize;
    private Integer hashJoinInlistPushdownMaxDistinctValues;
    private Integer defaultFilterSelectivity;
    private Boolean preferExistingUnion;
    private Boolean expandViewsAtOutput;
    private Boolean enableSortPushdown;

    private Builder() {}

    /** Whether to enable the distinct aggregation soft limit optimization. */
    public Builder enableDistinctAggregationSoftLimit(boolean value) {
      this.enableDistinctAggregationSoftLimit = value;
      return this;
    }

    /** Whether to use round-robin repartitioning. Default is true. */
    public Builder enableRoundRobinRepartition(boolean value) {
      this.enableRoundRobinRepartition = value;
      return this;
    }

    /** Whether to enable top-K aggregation optimization. Default is true. */
    public Builder enableTopkAggregation(boolean value) {
      this.enableTopkAggregation = value;
      return this;
    }

    /** Whether to enable window function limit optimizations. */
    public Builder enableWindowLimits(boolean value) {
      this.enableWindowLimits = value;
      return this;
    }

    /** Whether to enable top-K dynamic filter pushdown. */
    public Builder enableTopkDynamicFilterPushdown(boolean value) {
      this.enableTopkDynamicFilterPushdown = value;
      return this;
    }

    /** Whether to enable join dynamic filter pushdown. */
    public Builder enableJoinDynamicFilterPushdown(boolean value) {
      this.enableJoinDynamicFilterPushdown = value;
      return this;
    }

    /** Whether to enable aggregate dynamic filter pushdown. */
    public Builder enableAggregateDynamicFilterPushdown(boolean value) {
      this.enableAggregateDynamicFilterPushdown = value;
      return this;
    }

    /** Whether to enable dynamic filter pushdown. */
    public Builder enableDynamicFilterPushdown(boolean value) {
      this.enableDynamicFilterPushdown = value;
      return this;
    }

    /** Whether to filter null join keys during planning. Default is true. */
    public Builder filterNullJoinKeys(boolean value) {
      this.filterNullJoinKeys = value;
      return this;
    }

    /** Whether to repartition aggregation inputs. Default is true. */
    public Builder repartitionAggregations(boolean value) {
      this.repartitionAggregations = value;
      return this;
    }

    /** Minimum file size (in bytes) for file repartitioning. */
    public Builder repartitionFileMinSize(long value) {
      this.repartitionFileMinSize = value;
      return this;
    }

    /** Whether to repartition join inputs. Default is true. */
    public Builder repartitionJoins(boolean value) {
      this.repartitionJoins = value;
      return this;
    }

    /** Whether to allow symmetric joins without pruning. */
    public Builder allowSymmetricJoinsWithoutPruning(boolean value) {
      this.allowSymmetricJoinsWithoutPruning = value;
      return this;
    }

    /** Whether to repartition file scans. Default is true. */
    public Builder repartitionFileScans(boolean value) {
      this.repartitionFileScans = value;
      return this;
    }

    /** Number of file partitions to preserve. */
    public Builder preserveFilePartitions(int value) {
      this.preserveFilePartitions = value;
      return this;
    }

    /** Whether to repartition window function inputs. Default is true. */
    public Builder repartitionWindows(boolean value) {
      this.repartitionWindows = value;
      return this;
    }

    /** Whether to repartition sort inputs. Default is true. */
    public Builder repartitionSorts(boolean value) {
      this.repartitionSorts = value;
      return this;
    }

    /** Subset repartition threshold. */
    public Builder subsetRepartitionThreshold(int value) {
      this.subsetRepartitionThreshold = value;
      return this;
    }

    /** Whether to prefer existing sort order. */
    public Builder preferExistingSort(boolean value) {
      this.preferExistingSort = value;
      return this;
    }

    /** Whether to skip failed optimizer rules instead of erroring. Default is true. */
    public Builder skipFailedRules(boolean value) {
      this.skipFailedRules = value;
      return this;
    }

    /** Maximum number of optimizer passes. Default is 3. */
    public Builder maxPasses(int value) {
      this.maxPasses = value;
      return this;
    }

    /** Whether to use top-down join key reordering. Default is true. */
    public Builder topDownJoinKeyReordering(boolean value) {
      this.topDownJoinKeyReordering = value;
      return this;
    }

    /** Whether to prefer hash joins over sort-merge joins. Default is true. */
    public Builder preferHashJoin(boolean value) {
      this.preferHashJoin = value;
      return this;
    }

    /** Whether to enable piecewise merge join. */
    public Builder enablePiecewiseMergeJoin(boolean value) {
      this.enablePiecewiseMergeJoin = value;
      return this;
    }

    /** Threshold (in bytes) for using a single partition in hash joins. */
    public Builder hashJoinSinglePartitionThreshold(long value) {
      this.hashJoinSinglePartitionThreshold = value;
      return this;
    }

    /** Threshold (in rows) for using a single partition in hash joins. */
    public Builder hashJoinSinglePartitionThresholdRows(long value) {
      this.hashJoinSinglePartitionThresholdRows = value;
      return this;
    }

    /** Maximum in-list size for hash join pushdown (in bytes). */
    public Builder hashJoinInlistPushdownMaxSize(long value) {
      this.hashJoinInlistPushdownMaxSize = value;
      return this;
    }

    /** Maximum number of distinct values for hash join in-list pushdown. */
    public Builder hashJoinInlistPushdownMaxDistinctValues(int value) {
      this.hashJoinInlistPushdownMaxDistinctValues = value;
      return this;
    }

    /** Default filter selectivity percentage (0-100). Default is 20. */
    public Builder defaultFilterSelectivity(int value) {
      this.defaultFilterSelectivity = value;
      return this;
    }

    /** Whether to prefer existing union ordering. */
    public Builder preferExistingUnion(boolean value) {
      this.preferExistingUnion = value;
      return this;
    }

    /** Whether to expand views at the output level. */
    public Builder expandViewsAtOutput(boolean value) {
      this.expandViewsAtOutput = value;
      return this;
    }

    /** Whether to enable sort pushdown optimization. */
    public Builder enableSortPushdown(boolean value) {
      this.enableSortPushdown = value;
      return this;
    }

    /** Builds the {@link OptimizerOptions}. */
    public OptimizerOptions build() {
      return new OptimizerOptions(
          enableDistinctAggregationSoftLimit,
          enableRoundRobinRepartition,
          enableTopkAggregation,
          enableWindowLimits,
          enableTopkDynamicFilterPushdown,
          enableJoinDynamicFilterPushdown,
          enableAggregateDynamicFilterPushdown,
          enableDynamicFilterPushdown,
          filterNullJoinKeys,
          repartitionAggregations,
          repartitionFileMinSize,
          repartitionJoins,
          allowSymmetricJoinsWithoutPruning,
          repartitionFileScans,
          preserveFilePartitions,
          repartitionWindows,
          repartitionSorts,
          subsetRepartitionThreshold,
          preferExistingSort,
          skipFailedRules,
          maxPasses,
          topDownJoinKeyReordering,
          preferHashJoin,
          enablePiecewiseMergeJoin,
          hashJoinSinglePartitionThreshold,
          hashJoinSinglePartitionThresholdRows,
          hashJoinInlistPushdownMaxSize,
          hashJoinInlistPushdownMaxDistinctValues,
          defaultFilterSelectivity,
          preferExistingUnion,
          expandViewsAtOutput,
          enableSortPushdown);
    }
  }
}
