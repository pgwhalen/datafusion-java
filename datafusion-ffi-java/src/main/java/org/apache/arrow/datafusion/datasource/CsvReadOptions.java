package org.apache.arrow.datafusion.datasource;

import com.google.protobuf.ByteString;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading CSV files.
 *
 * <p>Mirrors Rust's {@code CsvReadOptions}. Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * <p>{@snippet : CsvReadOptions options = CsvReadOptions.builder() .hasHeader(true)
 * .delimiter((byte) '\t') .schemaInferMaxRecords(200) .build(); ctx.registerCsv("my_table",
 * "/path/to/data.csv", options); }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/file_format/options/struct.CsvReadOptions.html">Rust
 *     DataFusion: CsvReadOptions</a>
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
    this.nullRegex = builder.nullRegex;
    this.truncatedRows = builder.truncatedRows;
    this.schema = builder.schema;
  }

  /**
   * Creates a new builder with default values.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the schema, or null if not set.
   *
   * <p>Example:
   *
   * <p>{@snippet : CsvReadOptions options = CsvReadOptions.builder() .schema(mySchema)
   * .hasHeader(true) .build(); Schema schema = options.schema(); // mySchema }
   *
   * @return the schema, or null
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema">Rust
   *     DataFusion: CsvReadOptions::schema</a>
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Encodes the options (excluding schema) as protobuf bytes (CsvOptions proto).
   *
   * <p>Example:
   *
   * <p>{@snippet : CsvReadOptions options = CsvReadOptions.builder() .hasHeader(true) .build();
   * byte[] encoded = options.encodeOptions(); }
   *
   * @return the serialized protobuf bytes
   */
  public byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.CsvOptions.Builder b =
        org.apache.arrow.datafusion.proto.CsvOptions.newBuilder();
    b.setHasHeader(ByteString.copyFrom(new byte[] {(byte) (hasHeader ? 1 : 0)}));
    b.setDelimiter(ByteString.copyFrom(new byte[] {delimiter}));
    b.setQuote(ByteString.copyFrom(new byte[] {quote}));
    if (terminator != null) {
      b.setTerminator(ByteString.copyFrom(new byte[] {terminator}));
    }
    if (escape != null) {
      b.setEscape(ByteString.copyFrom(new byte[] {escape}));
    }
    if (comment != null) {
      b.setComment(ByteString.copyFrom(new byte[] {comment}));
    }
    b.setNewlinesInValues(ByteString.copyFrom(new byte[] {(byte) (newlinesInValues ? 1 : 0)}));
    b.setSchemaInferMaxRec(schemaInferMaxRecords);
    // UNCOMPRESSED = 4
    b.setCompressionValue(4);
    if (nullRegex != null) {
      b.setNullRegex(nullRegex);
    }
    b.setTruncatedRows(ByteString.copyFrom(new byte[] {(byte) (truncatedRows ? 1 : 0)}));
    return b.build().toByteArray();
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
    private String nullRegex = null;
    private boolean truncatedRows = false;
    private Schema schema = null;

    private Builder() {}

    /**
     * Sets whether the CSV has a header row.
     *
     * @param hasHeader true if CSV has a header
     * @return this builder
     */
    public Builder hasHeader(boolean hasHeader) {
      this.hasHeader = hasHeader;
      return this;
    }

    /**
     * Sets the field delimiter byte.
     *
     * @param delimiter the delimiter byte
     * @return this builder
     */
    public Builder delimiter(byte delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    /**
     * Sets the quote character byte.
     *
     * @param quote the quote byte
     * @return this builder
     */
    public Builder quote(byte quote) {
      this.quote = quote;
      return this;
    }

    /**
     * Sets the line terminator byte.
     *
     * @param terminator the terminator byte, or null for default
     * @return this builder
     */
    public Builder terminator(Byte terminator) {
      this.terminator = terminator;
      return this;
    }

    /**
     * Sets the escape character byte.
     *
     * @param escape the escape byte, or null for no escape
     * @return this builder
     */
    public Builder escape(Byte escape) {
      this.escape = escape;
      return this;
    }

    /**
     * Sets the comment character byte.
     *
     * @param comment the comment byte, or null for no comment support
     * @return this builder
     */
    public Builder comment(Byte comment) {
      this.comment = comment;
      return this;
    }

    /**
     * Sets whether newlines in values are supported.
     *
     * @param newlinesInValues true to support newlines in values
     * @return this builder
     */
    public Builder newlinesInValues(boolean newlinesInValues) {
      this.newlinesInValues = newlinesInValues;
      return this;
    }

    /**
     * Sets the maximum number of records to use for schema inference.
     *
     * @param schemaInferMaxRecords the maximum number of records
     * @return this builder
     */
    public Builder schemaInferMaxRecords(int schemaInferMaxRecords) {
      this.schemaInferMaxRecords = schemaInferMaxRecords;
      return this;
    }

    /**
     * Sets the regex pattern for null values.
     *
     * @param nullRegex the null value regex, or null for default
     * @return this builder
     */
    public Builder nullRegex(String nullRegex) {
      this.nullRegex = nullRegex;
      return this;
    }

    /**
     * Sets whether truncated rows are allowed.
     *
     * @param truncatedRows true to allow truncated rows
     * @return this builder
     */
    public Builder truncatedRows(boolean truncatedRows) {
      this.truncatedRows = truncatedRows;
      return this;
    }

    /**
     * Sets the schema for the CSV file.
     *
     * @param schema the Arrow schema, or null to infer
     * @return this builder
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Builds the {@link CsvReadOptions}.
     *
     * @return the built options
     */
    public CsvReadOptions build() {
      return new CsvReadOptions(this);
    }
  }
}
