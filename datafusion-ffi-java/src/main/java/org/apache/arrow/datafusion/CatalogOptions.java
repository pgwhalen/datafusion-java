package org.apache.arrow.datafusion;

import java.util.Map;

/**
 * Catalog configuration options. Maps to DataFusion's {@code CatalogOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param createDefaultCatalogAndSchema Whether the default catalog and schema should be created
 *     automatically
 * @param defaultCatalog The default catalog name (default "datafusion")
 * @param defaultSchema The default schema name (default "public")
 * @param informationSchema Whether to create information_schema virtual tables for schema
 *     information
 * @param location Location scanned to load tables for the default schema
 * @param format Type of TableProvider to use when loading default schema
 * @param hasHeader Default value for has_header for CREATE EXTERNAL TABLE CSV
 * @param newlinesInValues Specifies whether newlines in quoted CSV values are supported
 */
public record CatalogOptions(
    Boolean createDefaultCatalogAndSchema,
    Boolean informationSchema,
    String defaultCatalog,
    String defaultSchema,
    String location,
    String format,
    Boolean hasHeader,
    Boolean newlinesInValues) {

  private static final String PREFIX = "datafusion.catalog.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "create_default_catalog_and_schema", createDefaultCatalogAndSchema);
    putIfPresent(map, PREFIX + "default_catalog", defaultCatalog);
    putIfPresent(map, PREFIX + "default_schema", defaultSchema);
    putIfPresent(map, PREFIX + "information_schema", informationSchema);
    putIfPresent(map, PREFIX + "location", location);
    putIfPresent(map, PREFIX + "format", format);
    putIfPresent(map, PREFIX + "has_header", hasHeader);
    putIfPresent(map, PREFIX + "newlines_in_values", newlinesInValues);
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link CatalogOptions}. */
  public static final class Builder {
    private Boolean createDefaultCatalogAndSchema;
    private String defaultCatalog;
    private String defaultSchema;
    private Boolean informationSchema;
    private String location;
    private String format;
    private Boolean hasHeader;
    private Boolean newlinesInValues;

    private Builder() {}

    /**
     * Whether the default catalog and schema should be created automatically.
     *
     * @param value true to create default catalog and schema
     * @return this builder
     */
    public Builder createDefaultCatalogAndSchema(boolean value) {
      this.createDefaultCatalogAndSchema = value;
      return this;
    }

    /**
     * The default catalog name. Default is "datafusion".
     *
     * @param value the default catalog name
     * @return this builder
     */
    public Builder defaultCatalog(String value) {
      this.defaultCatalog = value;
      return this;
    }

    /**
     * The default schema name. Default is "public".
     *
     * @param value the default schema name
     * @return this builder
     */
    public Builder defaultSchema(String value) {
      this.defaultSchema = value;
      return this;
    }

    /**
     * Whether to create information_schema virtual tables for schema information.
     *
     * @param value true to enable information_schema
     * @return this builder
     */
    public Builder informationSchema(boolean value) {
      this.informationSchema = value;
      return this;
    }

    /**
     * Location scanned to load tables for the default schema.
     *
     * @param value the location path
     * @return this builder
     */
    public Builder location(String value) {
      this.location = value;
      return this;
    }

    /**
     * Type of TableProvider to use when loading default schema.
     *
     * @param value the format type
     * @return this builder
     */
    public Builder format(String value) {
      this.format = value;
      return this;
    }

    /**
     * Default value for has_header for CREATE EXTERNAL TABLE CSV.
     *
     * @param value true if CSV files have headers
     * @return this builder
     */
    public Builder hasHeader(boolean value) {
      this.hasHeader = value;
      return this;
    }

    /**
     * Specifies whether newlines in quoted CSV values are supported.
     *
     * @param value true to support newlines in values
     * @return this builder
     */
    public Builder newlinesInValues(boolean value) {
      this.newlinesInValues = value;
      return this;
    }

    /** Builds the {@link CatalogOptions}. */
    public CatalogOptions build() {
      return new CatalogOptions(
          createDefaultCatalogAndSchema,
          informationSchema,
          defaultCatalog,
          defaultSchema,
          location,
          format,
          hasHeader,
          newlinesInValues);
    }
  }
}
