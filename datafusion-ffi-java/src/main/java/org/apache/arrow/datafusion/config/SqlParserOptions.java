package org.apache.arrow.datafusion.config;

import java.util.Map;

/**
 * SQL parser configuration options. Maps to DataFusion's {@code SqlParserOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param parseFloatAsDecimal Whether to parse floating-point numbers as decimal type instead of
 *     float64
 * @param enableIdentNormalization Whether to normalize SQL identifiers (convert to lowercase,
 *     default true)
 * @param dialect SQL dialect to use for parsing (default GENERIC)
 * @param supportVarcharWithLength Whether to support VARCHAR with length specification
 * @param mapStringTypesToUtf8view Whether to map string types to Utf8View instead of Utf8
 * @param collectSpans Whether to collect span information during parsing
 * @param recursionLimit Maximum recursion depth for the SQL parser
 * @param defaultNullOrdering Default null ordering (e.g., "nulls_first", "nulls_last")
 */
public record SqlParserOptions(
    Boolean parseFloatAsDecimal,
    Boolean enableIdentNormalization,
    SqlDialect dialect,
    Boolean supportVarcharWithLength,
    Boolean mapStringTypesToUtf8view,
    Boolean collectSpans,
    Integer recursionLimit,
    String defaultNullOrdering) {

  private static final String PREFIX = "datafusion.sql_parser.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "parse_float_as_decimal", parseFloatAsDecimal);
    putIfPresent(map, PREFIX + "enable_ident_normalization", enableIdentNormalization);
    if (dialect != null) {
      map.put(PREFIX + "dialect", dialect.toConfigValue());
    }
    putIfPresent(map, PREFIX + "support_varchar_with_length", supportVarcharWithLength);
    putIfPresent(map, PREFIX + "map_string_types_to_utf8view", mapStringTypesToUtf8view);
    putIfPresent(map, PREFIX + "collect_spans", collectSpans);
    putIfPresent(map, PREFIX + "recursion_limit", recursionLimit);
    putIfPresent(map, PREFIX + "default_null_ordering", defaultNullOrdering);
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link SqlParserOptions}. */
  public static final class Builder {
    private Boolean parseFloatAsDecimal;
    private Boolean enableIdentNormalization;
    private SqlDialect dialect;
    private Boolean supportVarcharWithLength;
    private Boolean mapStringTypesToUtf8view;
    private Boolean collectSpans;
    private Integer recursionLimit;
    private String defaultNullOrdering;

    private Builder() {}

    /** Whether to parse floating-point numbers as decimal type instead of float64. */
    public Builder parseFloatAsDecimal(boolean value) {
      this.parseFloatAsDecimal = value;
      return this;
    }

    /** Whether to normalize SQL identifiers (convert to lowercase). Default is true. */
    public Builder enableIdentNormalization(boolean value) {
      this.enableIdentNormalization = value;
      return this;
    }

    /** SQL dialect to use for parsing. Default is GENERIC. */
    public Builder dialect(SqlDialect value) {
      this.dialect = value;
      return this;
    }

    /** Whether to support VARCHAR with length specification. */
    public Builder supportVarcharWithLength(boolean value) {
      this.supportVarcharWithLength = value;
      return this;
    }

    /** Whether to map string types to Utf8View instead of Utf8. */
    public Builder mapStringTypesToUtf8view(boolean value) {
      this.mapStringTypesToUtf8view = value;
      return this;
    }

    /** Whether to collect span information during parsing. */
    public Builder collectSpans(boolean value) {
      this.collectSpans = value;
      return this;
    }

    /** Maximum recursion depth for the SQL parser. */
    public Builder recursionLimit(int value) {
      this.recursionLimit = value;
      return this;
    }

    /** Default null ordering (e.g., "nulls_first", "nulls_last"). */
    public Builder defaultNullOrdering(String value) {
      this.defaultNullOrdering = value;
      return this;
    }

    /** Builds the {@link SqlParserOptions}. */
    public SqlParserOptions build() {
      return new SqlParserOptions(
          parseFloatAsDecimal,
          enableIdentNormalization,
          dialect,
          supportVarcharWithLength,
          mapStringTypesToUtf8view,
          collectSpans,
          recursionLimit,
          defaultNullOrdering);
    }
  }
}
