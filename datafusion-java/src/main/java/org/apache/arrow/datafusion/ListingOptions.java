package org.apache.arrow.datafusion;

/**
 * Configures options for a {@link ListingTable}
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.ListingOptions} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class ListingOptions implements AutoCloseable, NativeProxy {

  private final FileFormat format;
  private final String fileExtension;
  private final boolean collectStat;

  /** A Builder for {@link ListingOptions} instances */
  public static class Builder {
    private final FileFormat format;
    private String fileExtension = "";
    private boolean collectStat = true;

    /**
     * Create a new {@link ListingOptions} builder
     *
     * @param format The file format used by data files in the listing table
     */
    public Builder(FileFormat format) {
      this.format = format;
    }

    /**
     * Specify a suffix used to filter files in the listing location
     *
     * @param fileExtension The file suffix to filter on
     * @return This builder
     */
    public Builder withFileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    /**
     * Specify whether to collect statistics from files
     *
     * @param collectStat whether to collect statistics
     * @return This builder
     */
    public Builder withCollectStat(boolean collectStat) {
      this.collectStat = collectStat;
      return this;
    }

    /**
     * Build a new {@link ListingOptions} instance from the configured builder
     *
     * @return The built {@link ListingOptions}
     */
    public ListingOptions build() {
      return new ListingOptions(this);
    }
  }

  /**
   * Create a builder for listing options
   *
   * @param format The file format used by data files in the listing table
   * @return A new {@link Builder} instance
   */
  public static Builder builder(FileFormat format) {
    return new Builder(format);
  }

  private ListingOptions(Builder builder) {
    this.format = builder.format;
    this.fileExtension = builder.fileExtension;
    this.collectStat = builder.collectStat;
  }

  FileFormat getFormat() {
    return format;
  }

  String getFileExtension() {
    return fileExtension;
  }

  boolean isCollectStat() {
    return collectStat;
  }

  @Override
  public long getPointer() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
