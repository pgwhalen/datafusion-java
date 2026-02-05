package org.apache.arrow.datafusion;

/**
 * A URL pointing to a directory of files for a listing table.
 *
 * @param url the URL string (e.g., a local directory path or object store URL)
 */
public record ListingTableUrl(String url) {

  /**
   * Parses a path into a ListingTableUrl.
   *
   * @param path the directory path or URL
   * @return a new ListingTableUrl
   */
  public static ListingTableUrl parse(String path) {
    return new ListingTableUrl(path);
  }
}
