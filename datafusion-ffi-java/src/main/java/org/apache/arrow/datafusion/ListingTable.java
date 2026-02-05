package org.apache.arrow.datafusion;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Configuration for a file-backed listing table.
 *
 * <p>A listing table scans files in a directory, parsing them using a {@link FileFormat}. Use
 * {@link #builder(ListingTableUrl, ListingOptions)} to create a new configuration.
 *
 * <p>Example:
 *
 * <pre>{@code
 * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
 * ListingOptions options = ListingOptions.builder(myFormat).build();
 * ListingTable table = ListingTable.builder(url, options).schema(mySchema).build();
 * ctx.registerListingTable("my_table", table, allocator);
 * }</pre>
 */
public class ListingTable {
  private final ListingTableUrl url;
  private final ListingOptions options;
  private final Schema schema;

  private ListingTable(ListingTableUrl url, ListingOptions options, Schema schema) {
    this.url = url;
    this.options = options;
    this.schema = schema;
  }

  /**
   * Creates a builder for ListingTable.
   *
   * @param url the directory URL
   * @param options the listing options
   * @return a new builder
   */
  public static Builder builder(ListingTableUrl url, ListingOptions options) {
    return new Builder(url, options);
  }

  /** Returns the directory URL. */
  public ListingTableUrl url() {
    return url;
  }

  /** Returns the listing options. */
  public ListingOptions options() {
    return options;
  }

  /** Returns the table schema. */
  public Schema schema() {
    return schema;
  }

  /** Builder for ListingTable. */
  public static final class Builder {
    private final ListingTableUrl url;
    private final ListingOptions options;
    private Schema schema;

    private Builder(ListingTableUrl url, ListingOptions options) {
      this.url = url;
      this.options = options;
    }

    /**
     * Sets the table schema.
     *
     * @param schema the Arrow schema
     * @return this builder
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /** Builds the ListingTable. */
    public ListingTable build() {
      if (schema == null) {
        throw new IllegalStateException("Schema is required");
      }
      return new ListingTable(url, options, schema);
    }
  }
}
