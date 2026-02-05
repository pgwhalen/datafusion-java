package org.apache.arrow.datafusion;

/**
 * Configuration options for a listing table.
 *
 * <p>Use {@link #builder(FileFormat)} to create a new configuration.
 */
public class ListingOptions {
  private final FileFormat format;
  private final String fileExtension;
  private final boolean collectStat;
  private final int targetPartitions;

  private ListingOptions(
      FileFormat format, String fileExtension, boolean collectStat, int targetPartitions) {
    this.format = format;
    this.fileExtension = fileExtension;
    this.collectStat = collectStat;
    this.targetPartitions = targetPartitions;
  }

  /**
   * Creates a builder for ListingOptions.
   *
   * @param format the file format to use
   * @return a new builder
   */
  public static Builder builder(FileFormat format) {
    return new Builder(format);
  }

  /** Returns the file format. */
  public FileFormat format() {
    return format;
  }

  /** Returns the file extension filter. */
  public String fileExtension() {
    return fileExtension;
  }

  /** Returns whether to collect file statistics. */
  public boolean collectStat() {
    return collectStat;
  }

  /** Returns the target number of partitions. */
  public int targetPartitions() {
    return targetPartitions;
  }

  /** Builder for ListingOptions. */
  public static final class Builder {
    private final FileFormat format;
    private String fileExtension;
    private boolean collectStat = false;
    private int targetPartitions = 1;

    private Builder(FileFormat format) {
      this.format = format;
      this.fileExtension = format.getExtension();
    }

    /**
     * Sets the file extension filter.
     *
     * @param fileExtension the file extension (e.g., ".tsv")
     * @return this builder
     */
    public Builder fileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    /**
     * Sets whether to collect file statistics.
     *
     * @param collectStat true to collect statistics
     * @return this builder
     */
    public Builder collectStat(boolean collectStat) {
      this.collectStat = collectStat;
      return this;
    }

    /**
     * Sets the target number of partitions.
     *
     * @param targetPartitions the target partition count
     * @return this builder
     */
    public Builder targetPartitions(int targetPartitions) {
      this.targetPartitions = targetPartitions;
      return this;
    }

    /** Builds the ListingOptions. */
    public ListingOptions build() {
      return new ListingOptions(format, fileExtension, collectStat, targetPartitions);
    }
  }
}
