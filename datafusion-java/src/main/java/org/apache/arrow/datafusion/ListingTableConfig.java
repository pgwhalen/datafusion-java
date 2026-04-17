package org.apache.arrow.datafusion;

import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * Configuration for creating a {@link ListingTable}
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.ListingTableConfig} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class ListingTableConfig implements AutoCloseable, NativeProxy {

  private final String[] tablePaths;
  private final ListingOptions listingOptions;

  /** A Builder for {@link ListingTableConfig} instances */
  public static class Builder {
    private final String[] tablePaths;
    private ListingOptions options = null;

    /**
     * Create a new {@link Builder}
     *
     * @param tablePath The path where data files are stored.
     */
    public Builder(String tablePath) {
      this(new String[] {tablePath});
    }

    /**
     * Create a new {@link Builder}
     *
     * @param tablePaths The paths where data files are stored.
     */
    public Builder(String[] tablePaths) {
      this.tablePaths = tablePaths;
    }

    /**
     * Specify the {@link ListingOptions} to use
     *
     * @param options The {@link ListingOptions} to use
     * @return this Builder instance
     */
    public Builder withListingOptions(ListingOptions options) {
      this.options = options;
      return this;
    }

    /**
     * Create the listing table config.
     *
     * @param context The {@link SessionContext} to use (no longer used for schema inference)
     * @return Future that will complete with the table config
     */
    public CompletableFuture<ListingTableConfig> build(SessionContext context) {
      return CompletableFuture.completedFuture(new ListingTableConfig(tablePaths, options));
    }
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig}
   *
   * @param tablePath The path where data files are stored.
   * @return A new {@link Builder} instance
   */
  public static Builder builder(String tablePath) {
    return new Builder(tablePath);
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from a file path
   *
   * @param tablePath The path where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(Path tablePath) {
    return new Builder(tablePath.toString());
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from an array of paths
   *
   * @param tablePaths The path array where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(Path[] tablePaths) {
    String[] pathStrings =
        Arrays.stream(tablePaths)
            .map(path -> path.toString())
            .toArray(length -> new String[length]);
    return new Builder(pathStrings);
  }

  /**
   * Create a new {@link Builder} for a {@link ListingTableConfig} from a URI
   *
   * @param tablePath The location where data files are stored
   * @return A new {@link Builder} instance
   */
  public static Builder builder(URI tablePath) {
    return new Builder(tablePath.toString());
  }

  private ListingTableConfig(String[] tablePaths, ListingOptions listingOptions) {
    this.tablePaths = tablePaths;
    this.listingOptions = listingOptions;
  }

  String[] getTablePaths() {
    return tablePaths;
  }

  ListingOptions getListingOptions() {
    return listingOptions;
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
