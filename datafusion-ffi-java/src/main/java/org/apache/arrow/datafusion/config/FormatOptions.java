package org.apache.arrow.datafusion.config;

import java.util.Map;

/**
 * Display format configuration options. Maps to DataFusion's {@code FormatOptions}.
 *
 * <p>All fields are nullable. A null value means the DataFusion default is used.
 *
 * @param safe Whether to use safe formatting (replace errors with placeholder text)
 * @param nullString String representation of NULL values
 * @param dateFormat Format string for date values
 * @param datetimeFormat Format string for datetime values
 * @param timestampFormat Format string for timestamp values
 * @param timestampTzFormat Format string for timestamp with timezone values
 * @param timeFormat Format string for time values
 * @param durationFormat Format string for duration values
 * @param typesInfo Whether to include type information in formatted output
 */
public record FormatOptions(
    Boolean safe,
    String nullString,
    String dateFormat,
    String datetimeFormat,
    String timestampFormat,
    String timestampTzFormat,
    String timeFormat,
    String durationFormat,
    Boolean typesInfo) {

  private static final String PREFIX = "datafusion.format.";

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Writes non-null options to the map with proper dotted keys. */
  void writeTo(Map<String, String> map) {
    putIfPresent(map, PREFIX + "safe", safe);
    putIfPresent(map, PREFIX + "null", nullString);
    putIfPresent(map, PREFIX + "date_format", dateFormat);
    putIfPresent(map, PREFIX + "datetime_format", datetimeFormat);
    putIfPresent(map, PREFIX + "timestamp_format", timestampFormat);
    putIfPresent(map, PREFIX + "timestamp_tz_format", timestampTzFormat);
    putIfPresent(map, PREFIX + "time_format", timeFormat);
    putIfPresent(map, PREFIX + "duration_format", durationFormat);
    putIfPresent(map, PREFIX + "types_info", typesInfo);
  }

  private static void putIfPresent(Map<String, String> map, String key, Object value) {
    if (value != null) {
      map.put(key, value.toString());
    }
  }

  /** Builder for {@link FormatOptions}. */
  public static final class Builder {
    private Boolean safe;
    private String nullString;
    private String dateFormat;
    private String datetimeFormat;
    private String timestampFormat;
    private String timestampTzFormat;
    private String timeFormat;
    private String durationFormat;
    private Boolean typesInfo;

    private Builder() {}

    /** Whether to use safe formatting (replace errors with placeholder text). */
    public Builder safe(boolean value) {
      this.safe = value;
      return this;
    }

    /** String representation of NULL values. */
    public Builder nullString(String value) {
      this.nullString = value;
      return this;
    }

    /** Format string for date values. */
    public Builder dateFormat(String value) {
      this.dateFormat = value;
      return this;
    }

    /** Format string for datetime values. */
    public Builder datetimeFormat(String value) {
      this.datetimeFormat = value;
      return this;
    }

    /** Format string for timestamp values. */
    public Builder timestampFormat(String value) {
      this.timestampFormat = value;
      return this;
    }

    /** Format string for timestamp with timezone values. */
    public Builder timestampTzFormat(String value) {
      this.timestampTzFormat = value;
      return this;
    }

    /** Format string for time values. */
    public Builder timeFormat(String value) {
      this.timeFormat = value;
      return this;
    }

    /** Format string for duration values. */
    public Builder durationFormat(String value) {
      this.durationFormat = value;
      return this;
    }

    /** Whether to include type information in formatted output. */
    public Builder typesInfo(boolean value) {
      this.typesInfo = value;
      return this;
    }

    /** Builds the {@link FormatOptions}. */
    public FormatOptions build() {
      return new FormatOptions(
          safe,
          nullString,
          dateFormat,
          datetimeFormat,
          timestampFormat,
          timestampTzFormat,
          timeFormat,
          durationFormat,
          typesInfo);
    }
  }
}
