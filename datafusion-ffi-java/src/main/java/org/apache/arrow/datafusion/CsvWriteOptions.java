package org.apache.arrow.datafusion;

import com.google.protobuf.ByteString;

/**
 * Options for writing CSV files.
 *
 * <p>Use {@link #builder()} to create instances.
 */
public final class CsvWriteOptions {
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

  private CsvWriteOptions(Builder builder) {
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

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /** Encodes the options as protobuf bytes (CsvOptions proto). */
  byte[] encodeOptions() {
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

  /** Builder for {@link CsvWriteOptions}. */
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

    public Builder escape(Byte escape) {
      this.escape = escape;
      return this;
    }

    public Builder terminator(Byte terminator) {
      this.terminator = terminator;
      return this;
    }

    public Builder doubleQuote(boolean doubleQuote) {
      this.doubleQuote = doubleQuote;
      return this;
    }

    public Builder newlinesInValues(boolean newlinesInValues) {
      this.newlinesInValues = newlinesInValues;
      return this;
    }

    public Builder dateFormat(String dateFormat) {
      this.dateFormat = dateFormat;
      return this;
    }

    public Builder datetimeFormat(String datetimeFormat) {
      this.datetimeFormat = datetimeFormat;
      return this;
    }

    public Builder timestampFormat(String timestampFormat) {
      this.timestampFormat = timestampFormat;
      return this;
    }

    public Builder timestampTzFormat(String timestampTzFormat) {
      this.timestampTzFormat = timestampTzFormat;
      return this;
    }

    public Builder timeFormat(String timeFormat) {
      this.timeFormat = timeFormat;
      return this;
    }

    public Builder nullValue(String nullValue) {
      this.nullValue = nullValue;
      return this;
    }

    public Builder compressionLevel(Integer compressionLevel) {
      this.compressionLevel = compressionLevel;
      return this;
    }

    public CsvWriteOptions build() {
      return new CsvWriteOptions(this);
    }
  }
}
