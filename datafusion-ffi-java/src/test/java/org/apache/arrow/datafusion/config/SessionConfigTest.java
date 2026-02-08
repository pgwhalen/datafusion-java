package org.apache.arrow.datafusion.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.RecordBatchStream;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;

/** Tests for SessionConfig and DataFusion configuration options. */
public class SessionConfigTest {

  @Test
  void testToOptionsMapSerializesCorrectly() {
    SessionConfig config =
        SessionConfig.builder()
            .execution(ExecutionOptions.builder().batchSize(4096).targetPartitions(8).build())
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .option("datafusion.optimizer.max_passes", "5")
            .build();

    Map<String, String> map = config.toOptionsMap();

    assertEquals("4096", map.get("datafusion.execution.batch_size"));
    assertEquals("8", map.get("datafusion.execution.target_partitions"));
    assertEquals("true", map.get("datafusion.catalog.information_schema"));
    assertEquals("5", map.get("datafusion.optimizer.max_passes"));
    // Fields not set should not be in the map
    assertFalse(map.containsKey("datafusion.execution.time_zone"));
  }

  @Test
  void testFromStringMap() {
    Map<String, String> options =
        Map.of(
            "datafusion.execution.batch_size", "2048",
            "datafusion.execution.target_partitions", "4",
            "datafusion.catalog.information_schema", "true");

    SessionConfig config = SessionConfig.fromStringMap(options);

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      try (DataFrame df = ctx.sql("SHOW datafusion.execution.batch_size");
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VarCharVector valueVector = (VarCharVector) stream.getVectorSchemaRoot().getVector("value");
        assertEquals("2048", new String(valueVector.get(0)));
      }
    }
  }

  @Test
  void testEmptyConfigSameAsDefault() {
    // Builder with no options should create a default context (no FFI config call)
    SessionConfig config = SessionConfig.builder().build();
    assertFalse(config.hasOptions());

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      try (DataFrame df = ctx.sql("SELECT 42 as answer");
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        BigIntVector answerValues = (BigIntVector) stream.getVectorSchemaRoot().getVector("answer");
        assertEquals(42, answerValues.get(0));
      }
    }
  }

  @Test
  void testAllCatalogOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(
                CatalogOptions.builder()
                    .createDefaultCatalogAndSchema(true)
                    .informationSchema(true)
                    .defaultCatalog("my_catalog")
                    .defaultSchema("my_schema")
                    .location("file:///tmp/test")
                    .format("parquet")
                    .hasHeader(true)
                    .newlinesInValues(true)
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      assertShow(ctx, allocator, "datafusion.catalog.create_default_catalog_and_schema", "true");
      assertShow(ctx, allocator, "datafusion.catalog.information_schema", "true");
      assertShow(ctx, allocator, "datafusion.catalog.default_catalog", "my_catalog");
      assertShow(ctx, allocator, "datafusion.catalog.default_schema", "my_schema");
      assertShow(ctx, allocator, "datafusion.catalog.location", "file:///tmp/test");
      assertShow(ctx, allocator, "datafusion.catalog.format", "parquet");
      assertShow(ctx, allocator, "datafusion.catalog.has_header", "true");
      assertShow(ctx, allocator, "datafusion.catalog.newlines_in_values", "true");
    }
  }

  @Test
  void testAllExecutionOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .execution(
                ExecutionOptions.builder()
                    .batchSize(2048)
                    .coalesceBatches(false)
                    .collectStatistics(true)
                    .targetPartitions(4)
                    .timeZone("+05:00")
                    .planningConcurrency(2)
                    .skipPhysicalAggregateSchemaCheck(true)
                    .spillCompression(SpillCompression.ZSTD)
                    .sortSpillReservationBytes(1048576L)
                    .sortInPlaceThresholdBytes(2048L)
                    .maxSpillFileSizeBytes(10485760L)
                    .metaFetchConcurrency(16)
                    .minimumParallelOutputFiles(2)
                    .softMaxRowsPerOutputFile(50000L)
                    .maxBufferedBatchesPerOutputFile(4)
                    .listingTableIgnoreSubdirectory(false)
                    .listingTableFactoryInferPartitions(false)
                    .enableRecursiveCtes(false)
                    .splitFileGroupsByStatistics(true)
                    .keepPartitionByColumns(false)
                    .skipPartialAggregationProbeRatioThreshold(0.5)
                    .skipPartialAggregationProbeRowsThreshold(5000)
                    .useRowNumberEstimatesToOptimizePartitioning(false)
                    .enforceBatchSizeInJoins(false)
                    .objectstoreWriterBufferSize(5242880L)
                    .enableAnsiMode(true)
                    // perfectHashJoinSmallBuildThreshold and perfectHashJoinMinKeyDensity
                    // are not available in DataFusion 52.1
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      assertShow(ctx, allocator, "datafusion.execution.batch_size", "2048");
      assertShow(ctx, allocator, "datafusion.execution.coalesce_batches", "false");
      assertShow(ctx, allocator, "datafusion.execution.collect_statistics", "true");
      assertShow(ctx, allocator, "datafusion.execution.target_partitions", "4");
      assertShow(ctx, allocator, "datafusion.execution.time_zone", "+05:00");
      assertShow(ctx, allocator, "datafusion.execution.planning_concurrency", "2");
      assertShow(
          ctx, allocator, "datafusion.execution.skip_physical_aggregate_schema_check", "true");
      assertShow(ctx, allocator, "datafusion.execution.spill_compression", "zstd");
      assertShow(ctx, allocator, "datafusion.execution.sort_spill_reservation_bytes", "1048576");
      assertShow(ctx, allocator, "datafusion.execution.sort_in_place_threshold_bytes", "2048");
      assertShow(ctx, allocator, "datafusion.execution.max_spill_file_size_bytes", "10485760");
      assertShow(ctx, allocator, "datafusion.execution.meta_fetch_concurrency", "16");
      assertShow(ctx, allocator, "datafusion.execution.minimum_parallel_output_files", "2");
      assertShow(ctx, allocator, "datafusion.execution.soft_max_rows_per_output_file", "50000");
      assertShow(ctx, allocator, "datafusion.execution.max_buffered_batches_per_output_file", "4");
      assertShow(ctx, allocator, "datafusion.execution.listing_table_ignore_subdirectory", "false");
      assertShow(
          ctx, allocator, "datafusion.execution.listing_table_factory_infer_partitions", "false");
      assertShow(ctx, allocator, "datafusion.execution.enable_recursive_ctes", "false");
      assertShow(ctx, allocator, "datafusion.execution.split_file_groups_by_statistics", "true");
      assertShow(ctx, allocator, "datafusion.execution.keep_partition_by_columns", "false");
      assertShow(
          ctx,
          allocator,
          "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
          "0.5");
      assertShow(
          ctx,
          allocator,
          "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
          "5000");
      assertShow(
          ctx,
          allocator,
          "datafusion.execution.use_row_number_estimates_to_optimize_partitioning",
          "false");
      assertShow(ctx, allocator, "datafusion.execution.enforce_batch_size_in_joins", "false");
      assertShow(ctx, allocator, "datafusion.execution.objectstore_writer_buffer_size", "5242880");
      assertShow(ctx, allocator, "datafusion.execution.enable_ansi_mode", "true");
      // perfectHashJoinSmallBuildThreshold and perfectHashJoinMinKeyDensity
      // are not available in DataFusion 52.1
    }
  }

  @Test
  void testAllParquetOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .execution(
                ExecutionOptions.builder()
                    .parquet(
                        ParquetOptions.builder()
                            .enablePageIndex(false)
                            .pruning(false)
                            .skipMetadata(false)
                            .metadataSizeHint(256)
                            .pushdownFilters(true)
                            .reorderFilters(true)
                            .forceFilterSelections(true)
                            .schemaForceViewTypes(false)
                            .binaryAsString(true)
                            .coerceInt96("timestamp_micros")
                            .bloomFilterOnRead(false)
                            .maxPredicateCacheSize(64)
                            .dataPagesizeLimit(2097152L)
                            .writeBatchSize(512)
                            .writerVersion("2.0")
                            .skipArrowMetadata(true)
                            .compression("snappy")
                            .dictionaryEnabled(false)
                            .dictionaryPageSizeLimit(524288L)
                            .statisticsEnabled("page")
                            .maxRowGroupSize(500000)
                            .createdBy("test-suite")
                            .columnIndexTruncateLength(32)
                            .statisticsTruncateLength(16)
                            .dataPageRowCountLimit(10000)
                            .encoding("plain")
                            .bloomFilterOnWrite(true)
                            .bloomFilterFpp(0.125)
                            .bloomFilterNdv(1000000L)
                            .allowSingleFileParallelism(false)
                            .maximumParallelRowGroupWriters(2)
                            .maximumBufferedRecordBatchesPerStream(4)
                            .build())
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      String p = "datafusion.execution.parquet.";
      assertShow(ctx, allocator, p + "enable_page_index", "false");
      assertShow(ctx, allocator, p + "pruning", "false");
      assertShow(ctx, allocator, p + "skip_metadata", "false");
      assertShow(ctx, allocator, p + "metadata_size_hint", "256");
      assertShow(ctx, allocator, p + "pushdown_filters", "true");
      assertShow(ctx, allocator, p + "reorder_filters", "true");
      assertShow(ctx, allocator, p + "force_filter_selections", "true");
      assertShow(ctx, allocator, p + "schema_force_view_types", "false");
      assertShow(ctx, allocator, p + "binary_as_string", "true");
      assertShow(ctx, allocator, p + "coerce_int96", "timestamp_micros");
      assertShow(ctx, allocator, p + "bloom_filter_on_read", "false");
      assertShow(ctx, allocator, p + "max_predicate_cache_size", "64");
      assertShow(ctx, allocator, p + "data_pagesize_limit", "2097152");
      assertShow(ctx, allocator, p + "write_batch_size", "512");
      assertShow(ctx, allocator, p + "writer_version", "2.0");
      assertShow(ctx, allocator, p + "skip_arrow_metadata", "true");
      assertShow(ctx, allocator, p + "compression", "snappy");
      assertShow(ctx, allocator, p + "dictionary_enabled", "false");
      assertShow(ctx, allocator, p + "dictionary_page_size_limit", "524288");
      assertShow(ctx, allocator, p + "statistics_enabled", "page");
      assertShow(ctx, allocator, p + "max_row_group_size", "500000");
      assertShow(ctx, allocator, p + "created_by", "test-suite");
      assertShow(ctx, allocator, p + "column_index_truncate_length", "32");
      assertShow(ctx, allocator, p + "statistics_truncate_length", "16");
      assertShow(ctx, allocator, p + "data_page_row_count_limit", "10000");
      assertShow(ctx, allocator, p + "encoding", "plain");
      assertShow(ctx, allocator, p + "bloom_filter_on_write", "true");
      assertShow(ctx, allocator, p + "bloom_filter_fpp", "0.125");
      assertShow(ctx, allocator, p + "bloom_filter_ndv", "1000000");
      assertShow(ctx, allocator, p + "allow_single_file_parallelism", "false");
      assertShow(ctx, allocator, p + "maximum_parallel_row_group_writers", "2");
      assertShow(ctx, allocator, p + "maximum_buffered_record_batches_per_stream", "4");
    }
  }

  @Test
  void testAllOptimizerOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .optimizer(
                OptimizerOptions.builder()
                    .enableDistinctAggregationSoftLimit(true)
                    .enableRoundRobinRepartition(false)
                    .enableTopkAggregation(false)
                    .enableWindowLimits(true)
                    .enableTopkDynamicFilterPushdown(true)
                    .enableJoinDynamicFilterPushdown(true)
                    .enableAggregateDynamicFilterPushdown(true)
                    // enable_dynamic_filter_pushdown cascades to override the three
                    // sub-options above, so set it to true to match
                    .enableDynamicFilterPushdown(true)
                    .filterNullJoinKeys(false)
                    .repartitionAggregations(false)
                    .repartitionFileMinSize(1048576L)
                    .repartitionJoins(false)
                    .allowSymmetricJoinsWithoutPruning(true)
                    .repartitionFileScans(false)
                    .preserveFilePartitions(4)
                    .repartitionWindows(false)
                    .repartitionSorts(false)
                    .subsetRepartitionThreshold(16)
                    .preferExistingSort(true)
                    .skipFailedRules(false)
                    .maxPasses(5)
                    .topDownJoinKeyReordering(false)
                    .preferHashJoin(false)
                    .enablePiecewiseMergeJoin(true)
                    .hashJoinSinglePartitionThreshold(2097152L)
                    .hashJoinSinglePartitionThresholdRows(131072L)
                    .hashJoinInlistPushdownMaxSize(1048576L)
                    .hashJoinInlistPushdownMaxDistinctValues(64)
                    .defaultFilterSelectivity(50)
                    .preferExistingUnion(true)
                    .expandViewsAtOutput(true)
                    .enableSortPushdown(false)
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      String p = "datafusion.optimizer.";
      assertShow(ctx, allocator, p + "enable_distinct_aggregation_soft_limit", "true");
      assertShow(ctx, allocator, p + "enable_round_robin_repartition", "false");
      assertShow(ctx, allocator, p + "enable_topk_aggregation", "false");
      assertShow(ctx, allocator, p + "enable_window_limits", "true");
      assertShow(ctx, allocator, p + "enable_topk_dynamic_filter_pushdown", "true");
      assertShow(ctx, allocator, p + "enable_join_dynamic_filter_pushdown", "true");
      assertShow(ctx, allocator, p + "enable_aggregate_dynamic_filter_pushdown", "true");
      assertShow(ctx, allocator, p + "enable_dynamic_filter_pushdown", "true");
      assertShow(ctx, allocator, p + "filter_null_join_keys", "false");
      assertShow(ctx, allocator, p + "repartition_aggregations", "false");
      assertShow(ctx, allocator, p + "repartition_file_min_size", "1048576");
      assertShow(ctx, allocator, p + "repartition_joins", "false");
      assertShow(ctx, allocator, p + "allow_symmetric_joins_without_pruning", "true");
      assertShow(ctx, allocator, p + "repartition_file_scans", "false");
      assertShow(ctx, allocator, p + "preserve_file_partitions", "4");
      assertShow(ctx, allocator, p + "repartition_windows", "false");
      assertShow(ctx, allocator, p + "repartition_sorts", "false");
      assertShow(ctx, allocator, p + "subset_repartition_threshold", "16");
      assertShow(ctx, allocator, p + "prefer_existing_sort", "true");
      assertShow(ctx, allocator, p + "skip_failed_rules", "false");
      assertShow(ctx, allocator, p + "max_passes", "5");
      assertShow(ctx, allocator, p + "top_down_join_key_reordering", "false");
      assertShow(ctx, allocator, p + "prefer_hash_join", "false");
      assertShow(ctx, allocator, p + "enable_piecewise_merge_join", "true");
      assertShow(ctx, allocator, p + "hash_join_single_partition_threshold", "2097152");
      assertShow(ctx, allocator, p + "hash_join_single_partition_threshold_rows", "131072");
      assertShow(ctx, allocator, p + "hash_join_inlist_pushdown_max_size", "1048576");
      assertShow(ctx, allocator, p + "hash_join_inlist_pushdown_max_distinct_values", "64");
      assertShow(ctx, allocator, p + "default_filter_selectivity", "50");
      assertShow(ctx, allocator, p + "prefer_existing_union", "true");
      assertShow(ctx, allocator, p + "expand_views_at_output", "true");
      assertShow(ctx, allocator, p + "enable_sort_pushdown", "false");
    }
  }

  @Test
  void testAllSqlParserOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .sqlParser(
                SqlParserOptions.builder()
                    .parseFloatAsDecimal(true)
                    .enableIdentNormalization(false)
                    .dialect(SqlDialect.POSTGRESQL)
                    .supportVarcharWithLength(true)
                    .mapStringTypesToUtf8view(true)
                    .collectSpans(true)
                    .recursionLimit(128)
                    .defaultNullOrdering("nulls_last")
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      String p = "datafusion.sql_parser.";
      assertShow(ctx, allocator, p + "parse_float_as_decimal", "true");
      assertShow(ctx, allocator, p + "enable_ident_normalization", "false");
      assertShow(ctx, allocator, p + "dialect", "postgresql");
      assertShow(ctx, allocator, p + "support_varchar_with_length", "true");
      assertShow(ctx, allocator, p + "map_string_types_to_utf8view", "true");
      assertShow(ctx, allocator, p + "collect_spans", "true");
      assertShow(ctx, allocator, p + "recursion_limit", "128");
      assertShow(ctx, allocator, p + "default_null_ordering", "nulls_last");
    }
  }

  @Test
  void testAllExplainOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .explain(
                ExplainOptions.builder()
                    .logicalPlanOnly(true)
                    .physicalPlanOnly(false)
                    .showStatistics(true)
                    .showSizes(false)
                    .showSchema(true)
                    .format(ExplainFormat.TREE)
                    .treeMaximumRenderWidth(120)
                    .analyzeLevel(ExplainAnalyzeLevel.DEV)
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      String p = "datafusion.explain.";
      assertShow(ctx, allocator, p + "logical_plan_only", "true");
      assertShow(ctx, allocator, p + "physical_plan_only", "false");
      assertShow(ctx, allocator, p + "show_statistics", "true");
      assertShow(ctx, allocator, p + "show_sizes", "false");
      assertShow(ctx, allocator, p + "show_schema", "true");
      assertShow(ctx, allocator, p + "format", "tree");
      assertShow(ctx, allocator, p + "tree_maximum_render_width", "120");
      assertShow(ctx, allocator, p + "analyze_level", "dev");
    }
  }

  @Test
  void testAllFormatOptionsViaShow() {
    SessionConfig config =
        SessionConfig.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .format(
                FormatOptions.builder()
                    .safe(false)
                    .nullString("N/A")
                    .dateFormat("%Y-%m-%d")
                    .datetimeFormat("%Y-%m-%d %H:%M:%S")
                    .timestampFormat("%Y-%m-%dT%H:%M:%S")
                    .timestampTzFormat("%Y-%m-%dT%H:%M:%S%z")
                    .timeFormat("%H:%M:%S")
                    .durationFormat("pretty")
                    .typesInfo(true)
                    .build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {
      String p = "datafusion.format.";
      assertShow(ctx, allocator, p + "safe", "false");
      assertShow(ctx, allocator, p + "null", "N/A");
      assertShow(ctx, allocator, p + "date_format", "%Y-%m-%d");
      assertShow(ctx, allocator, p + "datetime_format", "%Y-%m-%d %H:%M:%S");
      assertShow(ctx, allocator, p + "timestamp_format", "%Y-%m-%dT%H:%M:%S");
      assertShow(ctx, allocator, p + "timestamp_tz_format", "%Y-%m-%dT%H:%M:%S%z");
      assertShow(ctx, allocator, p + "time_format", "%H:%M:%S");
      assertShow(ctx, allocator, p + "duration_format", "pretty");
      assertShow(ctx, allocator, p + "types_info", "true");
    }
  }

  private static void assertShow(
      SessionContext ctx, BufferAllocator allocator, String key, String expected) {
    try (DataFrame df = ctx.sql("SHOW " + key);
        RecordBatchStream stream = df.executeStream(allocator)) {
      assertTrue(stream.loadNextBatch(), "No data for SHOW " + key);
      VarCharVector valueVector = (VarCharVector) stream.getVectorSchemaRoot().getVector("value");
      assertEquals(expected, new String(valueVector.get(0)), "Mismatch for " + key);
    }
  }
}
