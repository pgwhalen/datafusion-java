package org.apache.arrow.datafusion;

import java.util.HashMap;
import java.util.Map;

/**
 * Options for writing Parquet files.
 *
 * <p>Use {@link #builder()} to create instances.
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/config/struct.ParquetOptions.html">Rust
 *     DataFusion: ParquetOptions</a>
 */
public final class ParquetOptions {
  private final String compression;
  private final String writerVersion;
  private final String encoding;
  private final String statisticsEnabled;
  private final String createdBy;
  private final Boolean dictionaryEnabled;
  private final Boolean bloomFilterOnWrite;
  private final Boolean skipArrowMetadata;
  private final Boolean allowSingleFileParallelism;
  private final Long dataPagesizeLimit;
  private final Long writeBatchSize;
  private final Long dictionaryPageSizeLimit;
  private final Long maxRowGroupSize;
  private final Long dataPageRowCountLimit;
  private final Long maximumParallelRowGroupWriters;
  private final Long maximumBufferedRecordBatchesPerStream;
  private final Double bloomFilterFpp;
  private final Long bloomFilterNdv;
  private final Map<String, ParquetColumnOptions> columnOptions;

  private ParquetOptions(Builder builder) {
    this.compression = builder.compression;
    this.writerVersion = builder.writerVersion;
    this.encoding = builder.encoding;
    this.statisticsEnabled = builder.statisticsEnabled;
    this.createdBy = builder.createdBy;
    this.dictionaryEnabled = builder.dictionaryEnabled;
    this.bloomFilterOnWrite = builder.bloomFilterOnWrite;
    this.skipArrowMetadata = builder.skipArrowMetadata;
    this.allowSingleFileParallelism = builder.allowSingleFileParallelism;
    this.dataPagesizeLimit = builder.dataPagesizeLimit;
    this.writeBatchSize = builder.writeBatchSize;
    this.dictionaryPageSizeLimit = builder.dictionaryPageSizeLimit;
    this.maxRowGroupSize = builder.maxRowGroupSize;
    this.dataPageRowCountLimit = builder.dataPageRowCountLimit;
    this.maximumParallelRowGroupWriters = builder.maximumParallelRowGroupWriters;
    this.maximumBufferedRecordBatchesPerStream = builder.maximumBufferedRecordBatchesPerStream;
    this.bloomFilterFpp = builder.bloomFilterFpp;
    this.bloomFilterNdv = builder.bloomFilterNdv;
    this.columnOptions = new HashMap<>(builder.columnOptions);
  }

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /** Encodes the options as protobuf bytes (TableParquetOptions proto). */
  byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.ParquetOptions.Builder global =
        org.apache.arrow.datafusion.proto.ParquetOptions.newBuilder();

    boolean hasGlobalOptions = false;
    if (compression != null) {
      global.setCompression(compression);
      hasGlobalOptions = true;
    }
    if (writerVersion != null) {
      global.setWriterVersion(writerVersion);
      hasGlobalOptions = true;
    }
    if (encoding != null) {
      global.setEncoding(encoding);
      hasGlobalOptions = true;
    }
    if (statisticsEnabled != null) {
      global.setStatisticsEnabled(statisticsEnabled);
      hasGlobalOptions = true;
    }
    if (createdBy != null) {
      global.setCreatedBy(createdBy);
      hasGlobalOptions = true;
    }
    if (dictionaryEnabled != null) {
      global.setDictionaryEnabled(dictionaryEnabled);
      hasGlobalOptions = true;
    }
    if (bloomFilterOnWrite != null) {
      global.setBloomFilterOnWrite(bloomFilterOnWrite);
      hasGlobalOptions = true;
    }
    if (skipArrowMetadata != null) {
      global.setSkipArrowMetadata(skipArrowMetadata);
      hasGlobalOptions = true;
    }
    if (allowSingleFileParallelism != null) {
      global.setAllowSingleFileParallelism(allowSingleFileParallelism);
      hasGlobalOptions = true;
    }
    if (dataPagesizeLimit != null) {
      global.setDataPagesizeLimit(dataPagesizeLimit);
      hasGlobalOptions = true;
    }
    if (writeBatchSize != null) {
      global.setWriteBatchSize(writeBatchSize);
      hasGlobalOptions = true;
    }
    if (dictionaryPageSizeLimit != null) {
      global.setDictionaryPageSizeLimit(dictionaryPageSizeLimit);
      hasGlobalOptions = true;
    }
    if (maxRowGroupSize != null) {
      global.setMaxRowGroupSize(maxRowGroupSize);
      hasGlobalOptions = true;
    }
    if (dataPageRowCountLimit != null) {
      global.setDataPageRowCountLimit(dataPageRowCountLimit);
      hasGlobalOptions = true;
    }
    if (maximumParallelRowGroupWriters != null) {
      global.setMaximumParallelRowGroupWriters(maximumParallelRowGroupWriters);
      hasGlobalOptions = true;
    }
    if (maximumBufferedRecordBatchesPerStream != null) {
      global.setMaximumBufferedRecordBatchesPerStream(maximumBufferedRecordBatchesPerStream);
      hasGlobalOptions = true;
    }
    if (bloomFilterFpp != null) {
      global.setBloomFilterFpp(bloomFilterFpp);
      hasGlobalOptions = true;
    }
    if (bloomFilterNdv != null) {
      global.setBloomFilterNdv(bloomFilterNdv);
      hasGlobalOptions = true;
    }

    org.apache.arrow.datafusion.proto.TableParquetOptions.Builder table =
        org.apache.arrow.datafusion.proto.TableParquetOptions.newBuilder();
    if (hasGlobalOptions) {
      table.setGlobal(global);
    }

    for (Map.Entry<String, ParquetColumnOptions> entry : columnOptions.entrySet()) {
      org.apache.arrow.datafusion.proto.ParquetColumnOptions.Builder colOpts =
          org.apache.arrow.datafusion.proto.ParquetColumnOptions.newBuilder();
      ParquetColumnOptions opts = entry.getValue();
      if (opts.compression() != null) {
        colOpts.setCompression(opts.compression());
      }
      if (opts.encoding() != null) {
        colOpts.setEncoding(opts.encoding());
      }
      if (opts.dictionaryEnabled() != null) {
        colOpts.setDictionaryEnabled(opts.dictionaryEnabled());
      }
      if (opts.statisticsEnabled() != null) {
        colOpts.setStatisticsEnabled(opts.statisticsEnabled());
      }
      if (opts.bloomFilterEnabled() != null) {
        colOpts.setBloomFilterEnabled(opts.bloomFilterEnabled());
      }
      if (opts.bloomFilterFpp() != null) {
        colOpts.setBloomFilterFpp(opts.bloomFilterFpp());
      }
      if (opts.bloomFilterNdv() != null) {
        colOpts.setBloomFilterNdv(opts.bloomFilterNdv());
      }
      table.addColumnSpecificOptions(
          org.apache.arrow.datafusion.proto.ParquetColumnSpecificOptions.newBuilder()
              .setColumnName(entry.getKey())
              .setOptions(colOpts));
    }

    return table.build().toByteArray();
  }

  /** Per-column Parquet options. */
  public record ParquetColumnOptions(
      String compression,
      String encoding,
      Boolean dictionaryEnabled,
      String statisticsEnabled,
      Boolean bloomFilterEnabled,
      Double bloomFilterFpp,
      Long bloomFilterNdv) {}

  /** Builder for {@link ParquetOptions}. */
  public static final class Builder {
    private String compression = null;
    private String writerVersion = null;
    private String encoding = null;
    private String statisticsEnabled = null;
    private String createdBy = null;
    private Boolean dictionaryEnabled = null;
    private Boolean bloomFilterOnWrite = null;
    private Boolean skipArrowMetadata = null;
    private Boolean allowSingleFileParallelism = null;
    private Long dataPagesizeLimit = null;
    private Long writeBatchSize = null;
    private Long dictionaryPageSizeLimit = null;
    private Long maxRowGroupSize = null;
    private Long dataPageRowCountLimit = null;
    private Long maximumParallelRowGroupWriters = null;
    private Long maximumBufferedRecordBatchesPerStream = null;
    private Double bloomFilterFpp = null;
    private Long bloomFilterNdv = null;
    private Map<String, ParquetColumnOptions> columnOptions = new HashMap<>();

    private Builder() {}

    /** Set the compression codec (e.g., "SNAPPY", "GZIP", "ZSTD", "LZ4", "BROTLI"). */
    public Builder compression(String compression) {
      this.compression = compression;
      return this;
    }

    /** Set the Parquet writer version (e.g., "1.0", "2.0"). */
    public Builder writerVersion(String writerVersion) {
      this.writerVersion = writerVersion;
      return this;
    }

    /** Set the encoding (e.g., "PLAIN", "RLE", "DELTA_BINARY_PACKED"). */
    public Builder encoding(String encoding) {
      this.encoding = encoding;
      return this;
    }

    /** Set the statistics level (e.g., "none", "chunk", "page"). */
    public Builder statisticsEnabled(String statisticsEnabled) {
      this.statisticsEnabled = statisticsEnabled;
      return this;
    }

    /** Set the created_by string in the Parquet metadata. */
    public Builder createdBy(String createdBy) {
      this.createdBy = createdBy;
      return this;
    }

    /** Enable or disable dictionary encoding. */
    public Builder dictionaryEnabled(Boolean dictionaryEnabled) {
      this.dictionaryEnabled = dictionaryEnabled;
      return this;
    }

    /** Enable or disable bloom filter on write. */
    public Builder bloomFilterOnWrite(Boolean bloomFilterOnWrite) {
      this.bloomFilterOnWrite = bloomFilterOnWrite;
      return this;
    }

    /** Skip writing Arrow schema metadata. */
    public Builder skipArrowMetadata(Boolean skipArrowMetadata) {
      this.skipArrowMetadata = skipArrowMetadata;
      return this;
    }

    /** Allow single-file parallelism for writing. */
    public Builder allowSingleFileParallelism(Boolean allowSingleFileParallelism) {
      this.allowSingleFileParallelism = allowSingleFileParallelism;
      return this;
    }

    /** Set the data page size limit in bytes. */
    public Builder dataPagesizeLimit(Long dataPagesizeLimit) {
      this.dataPagesizeLimit = dataPagesizeLimit;
      return this;
    }

    /** Set the write batch size. */
    public Builder writeBatchSize(Long writeBatchSize) {
      this.writeBatchSize = writeBatchSize;
      return this;
    }

    /** Set the dictionary page size limit in bytes. */
    public Builder dictionaryPageSizeLimit(Long dictionaryPageSizeLimit) {
      this.dictionaryPageSizeLimit = dictionaryPageSizeLimit;
      return this;
    }

    /** Set the maximum row group size. */
    public Builder maxRowGroupSize(Long maxRowGroupSize) {
      this.maxRowGroupSize = maxRowGroupSize;
      return this;
    }

    /** Set the data page row count limit. */
    public Builder dataPageRowCountLimit(Long dataPageRowCountLimit) {
      this.dataPageRowCountLimit = dataPageRowCountLimit;
      return this;
    }

    /** Set the maximum parallel row group writers. */
    public Builder maximumParallelRowGroupWriters(Long maximumParallelRowGroupWriters) {
      this.maximumParallelRowGroupWriters = maximumParallelRowGroupWriters;
      return this;
    }

    /** Set the maximum buffered record batches per stream. */
    public Builder maximumBufferedRecordBatchesPerStream(
        Long maximumBufferedRecordBatchesPerStream) {
      this.maximumBufferedRecordBatchesPerStream = maximumBufferedRecordBatchesPerStream;
      return this;
    }

    /** Set the bloom filter false positive probability. */
    public Builder bloomFilterFpp(Double bloomFilterFpp) {
      this.bloomFilterFpp = bloomFilterFpp;
      return this;
    }

    /** Set the bloom filter number of distinct values. */
    public Builder bloomFilterNdv(Long bloomFilterNdv) {
      this.bloomFilterNdv = bloomFilterNdv;
      return this;
    }

    /** Set per-column options. */
    public Builder columnOptions(Map<String, ParquetColumnOptions> columnOptions) {
      this.columnOptions = new HashMap<>(columnOptions);
      return this;
    }

    public ParquetOptions build() {
      return new ParquetOptions(this);
    }
  }
}
