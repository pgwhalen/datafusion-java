package org.apache.arrow.datafusion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Configuration for a file-backed listing table.
 *
 * <p>Mirrors Rust's {@code ListingTableConfig}. A listing table scans files in one or more
 * directories, parsing them using a {@link FileFormat}. Use {@link #builder(ListingTableUrl)} or
 * {@link #builderWithMultiPaths(List)} to create a new configuration.
 *
 * <p>Example (single path):
 *
 * <pre>{@code
 * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
 * ListingOptions options = ListingOptions.builder(myFormat).build();
 * ListingTable table = ListingTable.builder(url)
 *     .withListingOptions(options)
 *     .withSchema(mySchema)
 *     .build();
 * ctx.registerListingTable("my_table", table, allocator);
 * }</pre>
 *
 * <p>Example (multiple paths):
 *
 * <pre>{@code
 * ListingTable table = ListingTable.builderWithMultiPaths(List.of(url1, url2))
 *     .withListingOptions(options)
 *     .withSchema(mySchema)
 *     .build();
 * }</pre>
 */
public class ListingTable {
  private final List<ListingTableUrl> tablePaths;
  private final ListingOptions options;
  private final Schema schema;

  private ListingTable(List<ListingTableUrl> tablePaths, ListingOptions options, Schema schema) {
    this.tablePaths = Collections.unmodifiableList(new ArrayList<>(tablePaths));
    this.options = options;
    this.schema = schema;
  }

  /**
   * Creates a builder for a single table path.
   *
   * <p>Mirrors Rust's {@code ListingTableConfig::new(table_path)}.
   *
   * @param tablePath the directory URL
   * @return a new builder
   */
  public static Builder builder(ListingTableUrl tablePath) {
    return new Builder(List.of(tablePath));
  }

  /**
   * Creates a builder for multiple table paths.
   *
   * <p>Mirrors Rust's {@code ListingTableConfig::new_with_multi_paths(table_paths)}.
   *
   * @param tablePaths the directory URLs
   * @return a new builder
   */
  public static Builder builderWithMultiPaths(List<ListingTableUrl> tablePaths) {
    return new Builder(tablePaths);
  }

  /** Returns the table paths. Mirrors Rust's {@code ListingTableConfig.table_paths}. */
  public List<ListingTableUrl> tablePaths() {
    return tablePaths;
  }

  /** Returns the listing options. */
  public ListingOptions options() {
    return options;
  }

  /** Returns the table schema. */
  public Schema schema() {
    return schema;
  }

  /** Builder for ListingTable, mirroring the fluent API of Rust's {@code ListingTableConfig}. */
  public static final class Builder {
    private final List<ListingTableUrl> tablePaths;
    private ListingOptions options;
    private Schema schema;

    private Builder(List<ListingTableUrl> tablePaths) {
      this.tablePaths = tablePaths;
    }

    /**
     * Sets the listing options.
     *
     * <p>Mirrors Rust's {@code ListingTableConfig::with_listing_options(options)}.
     *
     * @param options the listing options
     * @return this builder
     */
    public Builder withListingOptions(ListingOptions options) {
      this.options = options;
      return this;
    }

    /**
     * Sets the table schema.
     *
     * <p>Mirrors Rust's {@code ListingTableConfig::with_schema(schema)}.
     *
     * @param schema the Arrow schema
     * @return this builder
     */
    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /** Builds the ListingTable. */
    public ListingTable build() {
      if (tablePaths == null || tablePaths.isEmpty()) {
        throw new IllegalStateException("At least one table path is required");
      }
      if (options == null) {
        throw new IllegalStateException("ListingOptions is required (call withListingOptions)");
      }
      if (schema == null) {
        throw new IllegalStateException("Schema is required (call withSchema)");
      }
      return new ListingTable(tablePaths, options, schema);
    }
  }
}
