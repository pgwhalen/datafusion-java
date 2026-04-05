package org.apache.arrow.datafusion.datasource;

/**
 * Configuration options for a listing table.
 *
 * <p>Use {@link #builder(FileFormat)} to create a new configuration.
 *
 * <p>Example:
 *
 * <p>{@snippet : ListingOptions options = ListingOptions.builder(myFormat) .fileExtension(".tsv")
 * .collectStat(true) .targetPartitions(4) .build(); ListingTableConfig config = new
 * ListingTableConfig(url) .withListingOptions(options); }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingOptions.html">Rust
 *     DataFusion: ListingOptions</a>
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

  /**
   * Returns the file format.
   *
   * <p>Example:
   *
   * <p>{@snippet : ListingOptions options = ListingOptions.builder(myFormat).build(); FileFormat
   * format = options.format(); String extension = format.getExtension(); System.out.println("Format
   * extension: " + extension); }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingOptions.html#structfield.format">Rust
   *     DataFusion: ListingOptions::format</a>
   */
  public FileFormat format() {
    return format;
  }

  /**
   * Returns the file extension filter.
   *
   * <p>Example:
   *
   * <p>{@snippet : ListingOptions options = ListingOptions.builder(myFormat) .fileExtension(".tsv")
   * .build(); String ext = options.fileExtension(); // ".tsv" }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingOptions.html#structfield.file_extension">Rust
   *     DataFusion: ListingOptions::file_extension</a>
   */
  public String fileExtension() {
    return fileExtension;
  }

  /**
   * Returns whether to collect file statistics.
   *
   * <p>Example:
   *
   * <p>{@snippet : ListingOptions options = ListingOptions.builder(myFormat) .collectStat(true)
   * .build(); boolean collect = options.collectStat(); // true }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingOptions.html#structfield.collect_stat">Rust
   *     DataFusion: ListingOptions::collect_stat</a>
   */
  public boolean collectStat() {
    return collectStat;
  }

  /**
   * Returns the target number of partitions.
   *
   * <p>Example:
   *
   * <p>{@snippet : ListingOptions options = ListingOptions.builder(myFormat) .targetPartitions(4)
   * .build(); int partitions = options.targetPartitions(); // 4 }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingOptions.html#structfield.target_partitions">Rust
   *     DataFusion: ListingOptions::target_partitions</a>
   */
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
