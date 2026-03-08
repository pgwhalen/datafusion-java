package org.apache.arrow.datafusion;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading CSV files.
 *
 * <p>Mirrors Rust's {@code CsvReadOptions}. Use {@link #builder()} to create instances.
 */
public final class CsvReadOptions {
  private final boolean hasHeader;
  private final byte delimiter;
  private final byte quote;
  private final Byte terminator;
  private final Byte escape;
  private final Byte comment;
  private final boolean newlinesInValues;
  private final int schemaInferMaxRecords;
  private final String fileExtension;
  private final String fileCompressionType;
  private final String nullRegex;
  private final boolean truncatedRows;
  private final Schema schema;

  private CsvReadOptions(Builder builder) {
    this.hasHeader = builder.hasHeader;
    this.delimiter = builder.delimiter;
    this.quote = builder.quote;
    this.terminator = builder.terminator;
    this.escape = builder.escape;
    this.comment = builder.comment;
    this.newlinesInValues = builder.newlinesInValues;
    this.schemaInferMaxRecords = builder.schemaInferMaxRecords;
    this.fileExtension = builder.fileExtension;
    this.fileCompressionType = builder.fileCompressionType;
    this.nullRegex = builder.nullRegex;
    this.truncatedRows = builder.truncatedRows;
    this.schema = builder.schema;
  }

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the schema, or null if not set. */
  public Schema schema() {
    return schema;
  }

  /** Encodes the options (excluding schema) as null-separated key-value bytes. */
  byte[] encodeOptions() {
    Map<String, String> kv = new LinkedHashMap<>();
    kv.put("has_header", String.valueOf(hasHeader));
    kv.put("delimiter", String.valueOf((char) delimiter));
    kv.put("quote", String.valueOf((char) quote));
    if (terminator != null) {
      kv.put("terminator", String.valueOf((char) terminator.byteValue()));
    }
    if (escape != null) {
      kv.put("escape", String.valueOf((char) escape.byteValue()));
    }
    if (comment != null) {
      kv.put("comment", String.valueOf((char) comment.byteValue()));
    }
    kv.put("newlines_in_values", String.valueOf(newlinesInValues));
    kv.put("schema_infer_max_records", String.valueOf(schemaInferMaxRecords));
    kv.put("file_extension", fileExtension);
    kv.put("file_compression_type", fileCompressionType);
    if (nullRegex != null) {
      kv.put("null_regex", nullRegex);
    }
    kv.put("truncated_rows", String.valueOf(truncatedRows));
    return encodeKeyValues(kv);
  }

  static byte[] encodeKeyValues(Map<String, String> kv) {
    if (kv.isEmpty()) return new byte[0];
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry : kv.entrySet()) {
      if (!first) sb.append('\0');
      sb.append(entry.getKey()).append('\0').append(entry.getValue());
      first = false;
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** Builder for {@link CsvReadOptions}. */
  public static final class Builder {
    private boolean hasHeader = true;
    private byte delimiter = ',';
    private byte quote = '"';
    private Byte terminator = null;
    private Byte escape = null;
    private Byte comment = null;
    private boolean newlinesInValues = false;
    private int schemaInferMaxRecords = 100;
    private String fileExtension = ".csv";
    private String fileCompressionType = "UNCOMPRESSED";
    private String nullRegex = null;
    private boolean truncatedRows = false;
    private Schema schema = null;

    private Builder() {}

    public Builder hasHeader(boolean hasHeader) {
      this.hasHeader = hasHeader;
      return this;
    }

    public Builder delimiter(byte delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder quote(byte quote) {
      this.quote = quote;
      return this;
    }

    public Builder terminator(Byte terminator) {
      this.terminator = terminator;
      return this;
    }

    public Builder escape(Byte escape) {
      this.escape = escape;
      return this;
    }

    public Builder comment(Byte comment) {
      this.comment = comment;
      return this;
    }

    public Builder newlinesInValues(boolean newlinesInValues) {
      this.newlinesInValues = newlinesInValues;
      return this;
    }

    public Builder schemaInferMaxRecords(int schemaInferMaxRecords) {
      this.schemaInferMaxRecords = schemaInferMaxRecords;
      return this;
    }

    public Builder fileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    public Builder fileCompressionType(String fileCompressionType) {
      this.fileCompressionType = fileCompressionType;
      return this;
    }

    public Builder nullRegex(String nullRegex) {
      this.nullRegex = nullRegex;
      return this;
    }

    public Builder truncatedRows(boolean truncatedRows) {
      this.truncatedRows = truncatedRows;
      return this;
    }

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public CsvReadOptions build() {
      return new CsvReadOptions(this);
    }
  }
}
