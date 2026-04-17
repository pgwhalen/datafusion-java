package org.apache.arrow.datafusion;

/**
 * Configures options specific to parsing SQL queries
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.config.SqlParserOptions} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("UnusedReturnValue")
public class SqlParserOptions {
  private final SessionConfig config;

  SqlParserOptions(SessionConfig config) {
    this.config = config;
  }

  public boolean parseFloatAsDecimal() {
    return config.parseFloatAsDecimal;
  }

  public SqlParserOptions withParseFloatAsDecimal(boolean enabled) {
    config.parseFloatAsDecimal = enabled;
    config.parseFloatAsDecimalSet = true;
    return this;
  }

  public boolean enableIdentNormalization() {
    return config.enableIdentNormalization;
  }

  public SqlParserOptions withEnableIdentNormalization(boolean enabled) {
    config.enableIdentNormalization = enabled;
    config.enableIdentNormalizationSet = true;
    return this;
  }

  public String dialect() {
    return config.dialect;
  }

  public SqlParserOptions withDialect(String dialect) {
    config.dialect = dialect;
    config.dialectSet = true;
    return this;
  }
}
