package org.apache.arrow.datafusion;

/**
 * A data source composed of multiple files that share a schema
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.ListingTable} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class ListingTable implements TableProvider, AutoCloseable {

  private final ListingTableConfig config;

  /**
   * Create a new listing table
   *
   * @param config The listing table configuration
   */
  public ListingTable(ListingTableConfig config) {
    this.config = config;
  }

  ListingTableConfig getConfig() {
    return config;
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
