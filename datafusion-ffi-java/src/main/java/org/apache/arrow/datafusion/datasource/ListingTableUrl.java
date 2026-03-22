package org.apache.arrow.datafusion.datasource;

/**
 * A URL pointing to a directory of files for a listing table.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTableUrl.html">Rust
 *     DataFusion: ListingTableUrl</a>
 */
public final class ListingTableUrl {

  private final String url;

  private ListingTableUrl(String url) {
    this.url = url;
  }

  /**
   * Returns the URL string.
   *
   * @return the URL string
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTableUrl.html#method.get_url">Rust
   *     DataFusion: ListingTableUrl::get_url</a>
   */
  public String getUrl() {
    return url;
  }

  /**
   * Parses a path into a ListingTableUrl.
   *
   * @param path the directory path or URL
   * @return a new ListingTableUrl
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/listing/struct.ListingTableUrl.html#method.parse">Rust
   *     DataFusion: ListingTableUrl::parse</a>
   */
  public static ListingTableUrl parse(String path) {
    return new ListingTableUrl(path);
  }
}
