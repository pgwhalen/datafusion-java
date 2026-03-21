package org.apache.arrow.datafusion.datasource;

import com.google.protobuf.ByteString;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading CSV files.
 *
 * <p>Mirrors Rust's {@code CsvReadOptions}. Use {@link #builder()} to create instances.
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

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the schema, or null if not set.
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/file_format/options/struct.CsvReadOptions.html#method.schema">Rust
   *     DataFusion: CsvReadOptions::schema</a>
   */
  public Schema schema() {
    return schema;
  }

  /** Encodes the options (excluding schema) as protobuf bytes (CsvOptions proto). */
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
