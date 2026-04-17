package org.apache.arrow.datafusion;

import java.util.function.Consumer;
import org.apache.arrow.datafusion.config.ConfigOptions;

/**
 * Configuration for creating a {@link SessionContext} using {@link SessionContexts#withConfig}
 *
 * @deprecated Use {@link ConfigOptions} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
public class SessionConfig extends AbstractProxy implements AutoCloseable {

  // Execution options (with DF 25 defaults)
  long batchSize = 8192;
  boolean batchSizeSet = false;
  boolean coalesceBatches = true;
  boolean coalesceBatchesSet = false;
  boolean collectStatistics = false;
  boolean collectStatisticsSet = false;
  long targetPartitions = java.lang.Runtime.getRuntime().availableProcessors();
  boolean targetPartitionsSet = false;

  // Parquet options (with DF 25 defaults)
  boolean enablePageIndex = true;
  boolean enablePageIndexSet = false;
  boolean pruning = true;
  boolean pruningSet = false;
  boolean skipMetadata = true;
  boolean skipMetadataSet = false;
  long metadataSizeHint = -1; // -1 means absent
  boolean metadataSizeHintSet = false;
  boolean pushdownFilters = false;
  boolean pushdownFiltersSet = false;
  boolean reorderFilters = false;
  boolean reorderFiltersSet = false;

  // SQL parser options (with DF 25 defaults)
  boolean parseFloatAsDecimal = false;
  boolean parseFloatAsDecimalSet = false;
  boolean enableIdentNormalization = true;
  boolean enableIdentNormalizationSet = false;
  String dialect = "generic";
  boolean dialectSet = false;

  /** Create a new default {@link SessionConfig} */
  public SessionConfig() {
    super();
  }

  /**
   * Get options related to query execution
   *
   * @return {@link ExecutionOptions} instance for this config
   */
  public ExecutionOptions executionOptions() {
    return new ExecutionOptions(this);
  }

  /**
   * Get options specific to parsing SQL queries
   *
   * @return {@link SqlParserOptions} instance for this config
   */
  public SqlParserOptions sqlParserOptions() {
    return new SqlParserOptions(this);
  }

  /**
   * Modify this session configuration and then return it, to simplify use in a try-with-resources
   * statement
   *
   * @param configurationCallback Callback used to update the configuration
   * @return This {@link SessionConfig} instance after being updated
   */
  public SessionConfig withConfiguration(Consumer<SessionConfig> configurationCallback) {
    configurationCallback.accept(this);
    return this;
  }

  /** Build a {@link ConfigOptions} from the explicitly-set values in this config. */
  ConfigOptions toConfigOptions() {
    ConfigOptions.Builder builder = ConfigOptions.builder();

    org.apache.arrow.datafusion.config.ExecutionOptions.Builder execBuilder = null;
    if (batchSizeSet || coalesceBatchesSet || collectStatisticsSet || targetPartitionsSet) {
      execBuilder = org.apache.arrow.datafusion.config.ExecutionOptions.builder();
      if (batchSizeSet) execBuilder.batchSize((int) batchSize);
      if (coalesceBatchesSet) execBuilder.coalesceBatches(coalesceBatches);
      if (collectStatisticsSet) execBuilder.collectStatistics(collectStatistics);
      if (targetPartitionsSet) execBuilder.targetPartitions((int) targetPartitions);
    }

    if (enablePageIndexSet
        || pruningSet
        || skipMetadataSet
        || pushdownFiltersSet
        || reorderFiltersSet) {
      org.apache.arrow.datafusion.config.ParquetOptions.Builder pqBuilder =
          org.apache.arrow.datafusion.config.ParquetOptions.builder();
      if (enablePageIndexSet) pqBuilder.enablePageIndex(enablePageIndex);
      if (pruningSet) pqBuilder.pruning(pruning);
      if (skipMetadataSet) pqBuilder.skipMetadata(skipMetadata);
      if (pushdownFiltersSet) pqBuilder.pushdownFilters(pushdownFilters);
      if (reorderFiltersSet) pqBuilder.reorderFilters(reorderFilters);
      if (execBuilder == null)
        execBuilder = org.apache.arrow.datafusion.config.ExecutionOptions.builder();
      execBuilder.parquet(pqBuilder.build());
    }

    if (execBuilder != null) {
      builder.execution(execBuilder.build());
    }

    if (parseFloatAsDecimalSet || enableIdentNormalizationSet || dialectSet) {
      org.apache.arrow.datafusion.config.SqlParserOptions.Builder spBuilder =
          org.apache.arrow.datafusion.config.SqlParserOptions.builder();
      if (parseFloatAsDecimalSet) spBuilder.parseFloatAsDecimal(parseFloatAsDecimal);
      if (enableIdentNormalizationSet) spBuilder.enableIdentNormalization(enableIdentNormalization);
      if (dialectSet) {
        for (org.apache.arrow.datafusion.config.Dialect d :
            org.apache.arrow.datafusion.config.Dialect.values()) {
          if (d.name().equalsIgnoreCase(dialect)) {
            spBuilder.dialect(d);
            break;
          }
        }
      }
      builder.sqlParser(spBuilder.build());
    }

    return builder.build();
  }

  @Override
  void doClose(long pointer) {
    // no-op: pure Java config
  }
}
