package org.apache.arrow.datafusion;

import java.util.Map;

/**
 * Parquet-specific configuration options. Maps to DataFusion's {@code ParquetOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param enablePageIndex Whether to use the page index when reading parquet files
 * @param pruning Whether to use row group pruning when reading parquet files
 * @param skipMetadata Whether to skip reading parquet file metadata from the footer
 * @param metadataSizeHint Hint for the size of the parquet metadata in bytes
 * @param pushdownFilters Whether to push filters down to the parquet scan
 * @param reorderFilters Whether to reorder filters for efficiency
 * @param forceFilterSelections Whether to force filter selectivity estimation
 * @param schemaForceViewTypes Whether to use view types for string/binary columns in the schema
 * @param binaryAsString Whether to read binary columns as strings
 * @param coerceInt96 How to coerce Int96 timestamps (e.g., "timestamp_micros")
 * @param bloomFilterOnRead Whether to use bloom filters when reading parquet files
 * @param maxPredicateCacheSize Maximum number of entries in the predicate cache
 * @param dataPagesizeLimit Maximum size of a data page in bytes
 * @param writeBatchSize Number of rows to write per batch
 * @param writerVersion Parquet writer version (e.g., "1.0", "2.0")
 * @param skipArrowMetadata Whether to skip writing Arrow metadata to the parquet file
 * @param compression Compression codec (e.g., "snappy", "gzip", "zstd(3)")
 * @param dictionaryEnabled Whether to use dictionary encoding
 * @param dictionaryPageSizeLimit Maximum size of a dictionary page in bytes
 * @param statisticsEnabled Statistics level (e.g., "none", "chunk", "page")
 * @param maxRowGroupSize Maximum number of rows in a row group
 * @param createdBy Creator string written into the parquet metadata
 * @param columnIndexTruncateLength Maximum length of column index values in bytes
 * @param statisticsTruncateLength Maximum length of statistics values in bytes
 * @param dataPageRowCountLimit Maximum number of rows per data page
 * @param encoding Encoding (e.g., "plain", "rle", "delta_binary_packed")
 * @param bloomFilterOnWrite Whether to write bloom filters
 * @param bloomFilterFpp False positive probability for bloom filters
 * @param bloomFilterNdv Number of distinct values for bloom filter sizing
 * @param allowSingleFileParallelism Whether to allow parallelism within a single file
 * @param maximumParallelRowGroupWriters Maximum number of parallel row group writers
 * @param maximumBufferedRecordBatchesPerStream Maximum number of buffered record batches per stream
 */
public record ParquetOptions(
    Boolean enablePageIndex,
    Boolean pruning,
    Boolean skipMetadata,
    Integer metadataSizeHint,
    Boolean pushdownFilters,
    Boolean reorderFilters,
    Boolean forceFilterSelections,
    Boolean schemaForceViewTypes,
    Boolean binaryAsString,
    String coerceInt96,
    Boolean bloomFilterOnRead,
    Integer maxPredicateCacheSize,
    Long dataPagesizeLimit,
    Integer writeBatchSize,
    String writerVersion,
    Boolean skipArrowMetadata,
    String compression,
    Boolean dictionaryEnabled,
    Long dictionaryPageSizeLimit,
    String statisticsEnabled,
    Integer maxRowGroupSize,
    String createdBy,
    Integer columnIndexTruncateLength,
    Integer statisticsTruncateLength,
    Integer dataPageRowCountLimit,
    String encoding,
    Boolean bloomFilterOnWrite,
    Double bloomFilterFpp,
    Long bloomFilterNdv,
    Boolean allowSingleFileParallelism,
    Integer maximumParallelRowGroupWriters,
    Integer maximumBufferedRecordBatchesPerStream) {

  private static final String PREFIX = "datafusion.execution.parquet.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "enable_page_index", enablePageIndex);
    putIfPresent(map, PREFIX + "pruning", pruning);
    putIfPresent(map, PREFIX + "skip_metadata", skipMetadata);
    putIfPresent(map, PREFIX + "metadata_size_hint", metadataSizeHint);
    putIfPresent(map, PREFIX + "pushdown_filters", pushdownFilters);
    putIfPresent(map, PREFIX + "reorder_filters", reorderFilters);
    putIfPresent(map, PREFIX + "force_filter_selections", forceFilterSelections);
    putIfPresent(map, PREFIX + "schema_force_view_types", schemaForceViewTypes);
    putIfPresent(map, PREFIX + "binary_as_string", binaryAsString);
    putIfPresent(map, PREFIX + "coerce_int96", coerceInt96);
    putIfPresent(map, PREFIX + "bloom_filter_on_read", bloomFilterOnRead);
    putIfPresent(map, PREFIX + "max_predicate_cache_size", maxPredicateCacheSize);
    putIfPresent(map, PREFIX + "data_pagesize_limit", dataPagesizeLimit);
    putIfPresent(map, PREFIX + "write_batch_size", writeBatchSize);
    putIfPresent(map, PREFIX + "writer_version", writerVersion);
    putIfPresent(map, PREFIX + "skip_arrow_metadata", skipArrowMetadata);
    putIfPresent(map, PREFIX + "compression", compression);
    putIfPresent(map, PREFIX + "dictionary_enabled", dictionaryEnabled);
    putIfPresent(map, PREFIX + "dictionary_page_size_limit", dictionaryPageSizeLimit);
    putIfPresent(map, PREFIX + "statistics_enabled", statisticsEnabled);
    putIfPresent(map, PREFIX + "max_row_group_size", maxRowGroupSize);
    putIfPresent(map, PREFIX + "created_by", createdBy);
    putIfPresent(map, PREFIX + "column_index_truncate_length", columnIndexTruncateLength);
    putIfPresent(map, PREFIX + "statistics_truncate_length", statisticsTruncateLength);
    putIfPresent(map, PREFIX + "data_page_row_count_limit", dataPageRowCountLimit);
    putIfPresent(map, PREFIX + "encoding", encoding);
    putIfPresent(map, PREFIX + "bloom_filter_on_write", bloomFilterOnWrite);
    putIfPresent(map, PREFIX + "bloom_filter_fpp", bloomFilterFpp);
    putIfPresent(map, PREFIX + "bloom_filter_ndv", bloomFilterNdv);
    putIfPresent(map, PREFIX + "allow_single_file_parallelism", allowSingleFileParallelism);
    putIfPresent(
        map, PREFIX + "maximum_parallel_row_group_writers", maximumParallelRowGroupWriters);
    putIfPresent(
        map,
        PREFIX + "maximum_buffered_record_batches_per_stream",
        maximumBufferedRecordBatchesPerStream);
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link ParquetOptions}. */
  public static final class Builder {
    private Boolean enablePageIndex;
    private Boolean pruning;
    private Boolean skipMetadata;
    private Integer metadataSizeHint;
    private Boolean pushdownFilters;
    private Boolean reorderFilters;
    private Boolean forceFilterSelections;
    private Boolean schemaForceViewTypes;
    private Boolean binaryAsString;
    private String coerceInt96;
    private Boolean bloomFilterOnRead;
    private Integer maxPredicateCacheSize;
    private Long dataPagesizeLimit;
    private Integer writeBatchSize;
    private String writerVersion;
    private Boolean skipArrowMetadata;
    private String compression;
    private Boolean dictionaryEnabled;
    private Long dictionaryPageSizeLimit;
    private String statisticsEnabled;
    private Integer maxRowGroupSize;
    private String createdBy;
    private Integer columnIndexTruncateLength;
    private Integer statisticsTruncateLength;
    private Integer dataPageRowCountLimit;
    private String encoding;
    private Boolean bloomFilterOnWrite;
    private Double bloomFilterFpp;
    private Long bloomFilterNdv;
    private Boolean allowSingleFileParallelism;
    private Integer maximumParallelRowGroupWriters;
    private Integer maximumBufferedRecordBatchesPerStream;

    private Builder() {}

    /** Whether to use the page index when reading parquet files. */
    public Builder enablePageIndex(boolean value) {
      this.enablePageIndex = value;
      return this;
    }

    /** Whether to use row group pruning when reading parquet files. */
    public Builder pruning(boolean value) {
      this.pruning = value;
      return this;
    }

    /** Whether to skip reading parquet file metadata from the footer. */
    public Builder skipMetadata(boolean value) {
      this.skipMetadata = value;
      return this;
    }

    /** Hint for the size of the parquet metadata in bytes. */
    public Builder metadataSizeHint(int value) {
      this.metadataSizeHint = value;
      return this;
    }

    /** Whether to push filters down to the parquet scan. */
    public Builder pushdownFilters(boolean value) {
      this.pushdownFilters = value;
      return this;
    }

    /** Whether to reorder filters for efficiency. */
    public Builder reorderFilters(boolean value) {
      this.reorderFilters = value;
      return this;
    }

    /** Whether to force filter selectivity estimation. */
    public Builder forceFilterSelections(boolean value) {
      this.forceFilterSelections = value;
      return this;
    }

    /** Whether to use view types for string/binary columns in the schema. */
    public Builder schemaForceViewTypes(boolean value) {
      this.schemaForceViewTypes = value;
      return this;
    }

    /** Whether to read binary columns as strings. */
    public Builder binaryAsString(boolean value) {
      this.binaryAsString = value;
      return this;
    }

    /** How to coerce Int96 timestamps (e.g., "timestamp_micros"). */
    public Builder coerceInt96(String value) {
      this.coerceInt96 = value;
      return this;
    }

    /** Whether to use bloom filters when reading parquet files. */
    public Builder bloomFilterOnRead(boolean value) {
      this.bloomFilterOnRead = value;
      return this;
    }

    /** Maximum number of entries in the predicate cache. */
    public Builder maxPredicateCacheSize(int value) {
      this.maxPredicateCacheSize = value;
      return this;
    }

    /** Maximum size of a data page in bytes. */
    public Builder dataPagesizeLimit(long value) {
      this.dataPagesizeLimit = value;
      return this;
    }

    /** Number of rows to write per batch. */
    public Builder writeBatchSize(int value) {
      this.writeBatchSize = value;
      return this;
    }

    /** Parquet writer version (e.g., "1.0", "2.0"). */
    public Builder writerVersion(String value) {
      this.writerVersion = value;
      return this;
    }

    /** Whether to skip writing Arrow metadata to the parquet file. */
    public Builder skipArrowMetadata(boolean value) {
      this.skipArrowMetadata = value;
      return this;
    }

    /** Compression codec (e.g., "snappy", "gzip", "zstd(3)"). */
    public Builder compression(String value) {
      this.compression = value;
      return this;
    }

    /** Whether to use dictionary encoding. */
    public Builder dictionaryEnabled(boolean value) {
      this.dictionaryEnabled = value;
      return this;
    }

    /** Maximum size of a dictionary page in bytes. */
    public Builder dictionaryPageSizeLimit(long value) {
      this.dictionaryPageSizeLimit = value;
      return this;
    }

    /** Statistics level (e.g., "none", "chunk", "page"). */
    public Builder statisticsEnabled(String value) {
      this.statisticsEnabled = value;
      return this;
    }

    /** Maximum number of rows in a row group. */
    public Builder maxRowGroupSize(int value) {
      this.maxRowGroupSize = value;
      return this;
    }

    /** Creator string written into the parquet metadata. */
    public Builder createdBy(String value) {
      this.createdBy = value;
      return this;
    }

    /** Maximum length of column index values in bytes. */
    public Builder columnIndexTruncateLength(int value) {
      this.columnIndexTruncateLength = value;
      return this;
    }

    /** Maximum length of statistics values in bytes. */
    public Builder statisticsTruncateLength(int value) {
      this.statisticsTruncateLength = value;
      return this;
    }

    /** Maximum number of rows per data page. */
    public Builder dataPageRowCountLimit(int value) {
      this.dataPageRowCountLimit = value;
      return this;
    }

    /** Encoding (e.g., "plain", "rle", "delta_binary_packed"). */
    public Builder encoding(String value) {
      this.encoding = value;
      return this;
    }

    /** Whether to write bloom filters. */
    public Builder bloomFilterOnWrite(boolean value) {
      this.bloomFilterOnWrite = value;
      return this;
    }

    /** False positive probability for bloom filters. */
    public Builder bloomFilterFpp(double value) {
      this.bloomFilterFpp = value;
      return this;
    }

    /** Number of distinct values for bloom filter sizing. */
    public Builder bloomFilterNdv(long value) {
      this.bloomFilterNdv = value;
      return this;
    }

    /** Whether to allow parallelism within a single file. */
    public Builder allowSingleFileParallelism(boolean value) {
      this.allowSingleFileParallelism = value;
      return this;
    }

    /** Maximum number of parallel row group writers. */
    public Builder maximumParallelRowGroupWriters(int value) {
      this.maximumParallelRowGroupWriters = value;
      return this;
    }

    /** Maximum number of buffered record batches per stream. */
    public Builder maximumBufferedRecordBatchesPerStream(int value) {
      this.maximumBufferedRecordBatchesPerStream = value;
      return this;
    }

    /** Builds the {@link ParquetOptions}. */
    public ParquetOptions build() {
      return new ParquetOptions(
          enablePageIndex,
          pruning,
          skipMetadata,
          metadataSizeHint,
          pushdownFilters,
          reorderFilters,
          forceFilterSelections,
          schemaForceViewTypes,
          binaryAsString,
          coerceInt96,
          bloomFilterOnRead,
          maxPredicateCacheSize,
          dataPagesizeLimit,
          writeBatchSize,
          writerVersion,
          skipArrowMetadata,
          compression,
          dictionaryEnabled,
          dictionaryPageSizeLimit,
          statisticsEnabled,
          maxRowGroupSize,
          createdBy,
          columnIndexTruncateLength,
          statisticsTruncateLength,
          dataPageRowCountLimit,
          encoding,
          bloomFilterOnWrite,
          bloomFilterFpp,
          bloomFilterNdv,
          allowSingleFileParallelism,
          maximumParallelRowGroupWriters,
          maximumBufferedRecordBatchesPerStream);
    }
  }
}
