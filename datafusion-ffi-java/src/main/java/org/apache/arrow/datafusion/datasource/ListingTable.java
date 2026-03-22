package org.apache.arrow.datafusion.datasource;

import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A file-backed listing table.
 *
 * <p>Created from a {@link ListingTableConfig} that specifies the paths, options, and schema. A
 * listing table scans files in one or more directories, parsing them using a {@link FileFormat}.
 *
 * <p>Example:
 *
 * <pre>{@code
 * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
 * ListingOptions options = ListingOptions.builder(myFormat).build();
 * ListingTableConfig config = new ListingTableConfig(url)
 *     .withListingOptions(options)
 *     .withSchema(mySchema);
 * ListingTable table = new ListingTable(config);
 * ctx.registerListingTable("my_table", table, allocator);
 * }</pre>
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTable.html">Rust
 *     DataFusion: ListingTable</a>
 */
public class ListingTable {
  private final ListingTableConfig config;

  /**
   * Creates a listing table from the given config.
   *
   * <p>The config must have listing options and schema set.
   *
   * @param config the listing table configuration
   * @throws IllegalStateException if options or schema are not set on the config
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTable.html#method.try_new">Rust
   *     DataFusion: ListingTable::try_new</a>
   */
  public ListingTable(ListingTableConfig config) {
    if (config.listingOptions() == null) {
      throw new IllegalStateException("ListingOptions is required (call withListingOptions)");
    }
    if (config.schema() == null) {
      throw new IllegalStateException("Schema is required (call withSchema)");
    }
    this.config = config;
  }

  /**
   * Returns the table paths. Mirrors Rust's {@code ListingTable.table_paths}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTable.html#method.table_paths">Rust
   *     DataFusion: ListingTable::table_paths</a>
   */
  public List<ListingTableUrl> tablePaths() {
    return config.tablePaths();
  }

  /**
   * Returns the listing options.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTable.html#method.options">Rust
   *     DataFusion: ListingTable::options</a>
   */
  public ListingOptions options() {
    return config.listingOptions();
  }

  /**
   * Returns the table schema.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTable.html#method.schema">Rust
   *     DataFusion: ListingTable::schema</a>
   */
  public Schema schema() {
    return config.schema();
  }
}
