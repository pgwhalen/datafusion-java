package org.apache.arrow.datafusion.datasource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Configuration for creating a {@link ListingTable}.
 *
 * <p>A listing table config specifies the paths, options, and schema for a file-backed table. Use
 * the single-path constructor for one directory, or the multi-path constructor for multiple
 * directories.
 *
 * <p>Example:
 *
 * {@snippet :
 * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
 * ListingOptions options = ListingOptions.builder(myFormat).build();
 * ListingTableConfig config = new ListingTableConfig(url)
 *     .withListingOptions(options)
 *     .withSchema(mySchema);
 * ListingTable table = new ListingTable(config);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html">Rust
 *     DataFusion: ListingTableConfig</a>
 */
public class ListingTableConfig {
  private final List<ListingTableUrl> tablePaths;
  private ListingOptions options;
  private Schema schema;

  /**
   * Creates a config for a single table path.
   *
   * @param tablePath the directory URL
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#method.new">Rust
   *     DataFusion: ListingTableConfig::new</a>
   */
  public ListingTableConfig(ListingTableUrl tablePath) {
    this(List.of(tablePath));
  }

  /**
   * Creates a config for multiple table paths.
   *
   * @param tablePaths the directory URLs
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#method.new_with_multi_paths">Rust
   *     DataFusion: ListingTableConfig::new_with_multi_paths</a>
   */
  public ListingTableConfig(List<ListingTableUrl> tablePaths) {
    if (tablePaths == null || tablePaths.isEmpty()) {
      throw new IllegalArgumentException("At least one table path is required");
    }
    this.tablePaths = Collections.unmodifiableList(new ArrayList<>(tablePaths));
  }

  /**
   * Sets the listing options.
   *
   * <p>Example:
   *
   * {@snippet :
   * ListingOptions options = ListingOptions.builder(myFormat)
   *     .collectStat(true)
   *     .build();
   * ListingTableConfig config = new ListingTableConfig(url)
   *     .withListingOptions(options);
   * }
   *
   * @param options the listing options
   * @return this config
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#method.with_listing_options">Rust
   *     DataFusion: ListingTableConfig::with_listing_options</a>
   */
  public ListingTableConfig withListingOptions(ListingOptions options) {
    this.options = options;
    return this;
  }

  /**
   * Sets the table schema.
   *
   * <p>Example:
   *
   * {@snippet :
   * Schema schema = new Schema(List.of(
   *     Field.nullable("id", new ArrowType.Int(32, true)),
   *     Field.nullable("name", ArrowType.Utf8.INSTANCE)));
   * ListingTableConfig config = new ListingTableConfig(url)
   *     .withSchema(schema);
   * }
   *
   * @param schema the Arrow schema
   * @return this config
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#method.with_schema">Rust
   *     DataFusion: ListingTableConfig::with_schema</a>
   */
  public ListingTableConfig withSchema(Schema schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Returns the table paths.
   *
   * <p>Example:
   *
   * {@snippet :
   * ListingTableConfig config = new ListingTableConfig(url);
   * List<ListingTableUrl> paths = config.tablePaths();
   * for (ListingTableUrl path : paths) {
   *     System.out.println(path.getUrl());
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#structfield.table_paths">Rust
   *     DataFusion: ListingTableConfig::table_paths</a>
   */
  public List<ListingTableUrl> tablePaths() {
    return tablePaths;
  }

  /**
   * Returns the listing options, or {@code null} if not yet set.
   *
   * <p>Example:
   *
   * {@snippet :
   * ListingTableConfig config = new ListingTableConfig(url)
   *     .withListingOptions(options);
   * ListingOptions opts = config.listingOptions();
   * FileFormat format = opts.format();
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#structfield.options">Rust
   *     DataFusion: ListingTableConfig::options</a>
   */
  public ListingOptions listingOptions() {
    return options;
  }

  /**
   * Returns the table schema, or {@code null} if not yet set.
   *
   * <p>Example:
   *
   * {@snippet :
   * ListingTableConfig config = new ListingTableConfig(url)
   *     .withSchema(mySchema);
   * Schema schema = config.schema();
   * List<Field> fields = schema.getFields();
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/listing/struct.ListingTableConfig.html#structfield.file_schema">Rust
   *     DataFusion: ListingTableConfig::file_schema</a>
   */
  public Schema schema() {
    return schema;
  }
}
