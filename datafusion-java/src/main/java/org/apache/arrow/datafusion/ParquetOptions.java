package org.apache.arrow.datafusion;

import java.util.Optional;

/**
 * Configures options specific to reading Parquet data
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.config.ParquetOptions} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("UnusedReturnValue")
public class ParquetOptions {
  private final SessionConfig config;

  ParquetOptions(SessionConfig config) {
    this.config = config;
  }

  public boolean enablePageIndex() {
    return config.enablePageIndex;
  }

  public ParquetOptions withEnablePageIndex(boolean enabled) {
    config.enablePageIndex = enabled;
    config.enablePageIndexSet = true;
    return this;
  }

  public boolean pruning() {
    return config.pruning;
  }

  public ParquetOptions withPruning(boolean enabled) {
    config.pruning = enabled;
    config.pruningSet = true;
    return this;
  }

  public boolean skipMetadata() {
    return config.skipMetadata;
  }

  public ParquetOptions withSkipMetadata(boolean enabled) {
    config.skipMetadata = enabled;
    config.skipMetadataSet = true;
    return this;
  }

  public Optional<Long> metadataSizeHint() {
    return config.metadataSizeHint < 0 ? Optional.empty() : Optional.of(config.metadataSizeHint);
  }

  public ParquetOptions withMetadataSizeHint(Optional<Long> metadataSizeHint) {
    long value = -1L;
    if (metadataSizeHint.isPresent()) {
      value = metadataSizeHint.get();
      if (value < 0) {
        throw new RuntimeException("metadataSizeHint cannot be negative");
      }
    }
    config.metadataSizeHint = value;
    config.metadataSizeHintSet = true;
    return this;
  }

  public boolean pushdownFilters() {
    return config.pushdownFilters;
  }

  public ParquetOptions withPushdownFilters(boolean enabled) {
    config.pushdownFilters = enabled;
    config.pushdownFiltersSet = true;
    return this;
  }

  public boolean reorderFilters() {
    return config.reorderFilters;
  }

  public ParquetOptions withReorderFilters(boolean enabled) {
    config.reorderFilters = enabled;
    config.reorderFiltersSet = true;
    return this;
  }
}
