package org.apache.arrow.datafusion;

import com.google.protobuf.ByteString;

/**
 * Options for writing CSV files.
 *
 * <p>Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * {@snippet :
 * CsvOptions options = CsvOptions.builder()
 *     .hasHeader(true)
 *     .delimiter((byte) ',')
 *     .dateFormat("%Y-%m-%d")
 *     .build();
 * byte[] encoded = options.encodeOptions();
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/config/struct.CsvOptions.html">Rust
 *     DataFusion: CsvOptions</a>
 */
public final class CsvOptions {
  private final boolean hasHeader;
  private final byte delimiter;
  private final byte quote;
  private final Byte escape;
  private final Byte terminator;
  private final boolean doubleQuote;
  private final boolean newlinesInValues;
  private final String dateFormat;
  private final String datetimeFormat;
  private final String timestampFormat;
  private final String timestampTzFormat;
  private final String timeFormat;
  private final String nullValue;
  private final Integer compressionLevel;

  private CsvOptions(Builder builder) {
    this.hasHeader = builder.hasHeader;
    this.delimiter = builder.delimiter;
    this.quote = builder.quote;
    this.escape = builder.escape;
    this.terminator = builder.terminator;
    this.doubleQuote = builder.doubleQuote;
    this.newlinesInValues = builder.newlinesInValues;
    this.dateFormat = builder.dateFormat;
    this.datetimeFormat = builder.datetimeFormat;
    this.timestampFormat = builder.timestampFormat;
    this.timestampTzFormat = builder.timestampTzFormat;
    this.timeFormat = builder.timeFormat;
    this.nullValue = builder.nullValue;
    this.compressionLevel = builder.compressionLevel;
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
   * Encodes the options as protobuf bytes (CsvOptions proto).
   *
   * <p>Example:
   *
   * {@snippet :
   * CsvOptions options = CsvOptions.builder()
   *     .hasHeader(true)
   *     .build();
   * byte[] encoded = options.encodeOptions();
   * }
   *
   * @return the serialized protobuf bytes
   */
  public byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.CsvOptions.Builder b =
        org.apache.arrow.datafusion.proto.CsvOptions.newBuilder();
    b.setHasHeader(ByteString.copyFrom(new byte[] {(byte) (hasHeader ? 1 : 0)}));
    b.setDelimiter(ByteString.copyFrom(new byte[] {delimiter}));
    b.setQuote(ByteString.copyFrom(new byte[] {quote}));
    if (escape != null) {
      b.setEscape(ByteString.copyFrom(new byte[] {escape}));
    }
    if (terminator != null) {
      b.setTerminator(ByteString.copyFrom(new byte[] {terminator}));
    }
    b.setDoubleQuote(ByteString.copyFrom(new byte[] {(byte) (doubleQuote ? 1 : 0)}));
    b.setNewlinesInValues(ByteString.copyFrom(new byte[] {(byte) (newlinesInValues ? 1 : 0)}));
    // Compression is UNCOMPRESSED by default (enum value 4)
    b.setCompressionValue(4);
    if (dateFormat != null) {
      b.setDateFormat(dateFormat);
    }
    if (datetimeFormat != null) {
      b.setDatetimeFormat(datetimeFormat);
    }
    if (timestampFormat != null) {
      b.setTimestampFormat(timestampFormat);
    }
    if (timestampTzFormat != null) {
      b.setTimestampTzFormat(timestampTzFormat);
    }
    if (timeFormat != null) {
      b.setTimeFormat(timeFormat);
    }
    if (nullValue != null) {
      b.setNullValue(nullValue);
    }
    if (compressionLevel != null) {
      b.setCompressionLevel(compressionLevel);
    }
    return b.build().toByteArray();
  }

  /** Builder for {@link CsvOptions}. */
  public static final class Builder {
    private boolean hasHeader = true;
    private byte delimiter = ',';
    private byte quote = '"';
    private Byte escape = null;
    private Byte terminator = null;
    private boolean doubleQuote = true;
    private boolean newlinesInValues = false;
    private String dateFormat = null;
    private String datetimeFormat = null;
    private String timestampFormat = null;
    private String timestampTzFormat = null;
    private String timeFormat = null;
    private String nullValue = null;
    private Integer compressionLevel = null;

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
     * Sets whether to use double quotes for escaping.
     *
     * @param doubleQuote true to use double quotes
     * @return this builder
     */
    public Builder doubleQuote(boolean doubleQuote) {
      this.doubleQuote = doubleQuote;
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
     * Sets the date format string.
     *
     * @param dateFormat the date format
     * @return this builder
     */
    public Builder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    /**
     * Sets the datetime format string.
     *
     * @param datetimeFormat the datetime format
     * @return this builder
     */
    public Builder datetimeFormat(String datetimeFormat) {
      this.datetimeFormat = datetimeFormat;
      return this;
    }

    /**
     * Sets the timestamp format string.
     *
     * @param timestampFormat the timestamp format
     * @return this builder
     */
    public Builder timestampFormat(String timestampFormat) {
      this.timestampFormat = timestampFormat;
      return this;
    }

    /**
     * Sets the timestamp with timezone format string.
     *
     * @param timestampTzFormat the timestamp with timezone format
     * @return this builder
     */
    public Builder timestampTzFormat(String timestampTzFormat) {
      this.timestampTzFormat = timestampTzFormat;
      return this;
    }

    /**
     * Sets the time format string.
     *
     * @param timeFormat the time format
     * @return this builder
     */
    public Builder timeFormat(String timeFormat) {
      this.timeFormat = timeFormat;
      return this;
    }

    /**
     * Sets the null value representation string.
     *
     * @param nullValue the null value string
     * @return this builder
     */
    public Builder nullValue(String nullValue) {
      this.nullValue = nullValue;
      return this;
    }

    /**
     * Sets the compression level.
     *
     * @param compressionLevel the compression level, or null for default
     * @return this builder
     */
    public Builder compressionLevel(Integer compressionLevel) {
      this.compressionLevel = compressionLevel;
      return this;
    }

    /**
     * Builds the {@link CsvOptions}.
     *
     * @return the built options
     */
    public CsvOptions build() {
      return new CsvOptions(this);
    }
  }
}
