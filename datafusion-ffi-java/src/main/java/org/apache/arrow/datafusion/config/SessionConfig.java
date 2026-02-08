package org.apache.arrow.datafusion.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configuration options for a SessionContext.
 *
 * <p>This class mirrors DataFusion's {@code SessionConfig} and supports two modes of configuration:
 *
 * <ul>
 *   <li><b>Typed builders</b> -- Use nested option classes like {@link ExecutionOptions}, {@link
 *       OptimizerOptions}, etc. for type-safe configuration with IDE auto-complete.
 *   <li><b>Raw string map</b> -- Use {@link Builder#option(String, String)} or {@link
 *       #fromStringMap(Map)} to pass arbitrary dotted-key options (e.g., {@code
 *       "datafusion.execution.batch_size"}).
 * </ul>
 *
 * <p>Both modes can be combined. Typed options and raw options are merged, with raw options taking
 * precedence on conflict.
 *
 * <p>Use {@link #builder()} to create a new configuration, or {@link #defaults()} to use the
 * default configuration.
 *
 * <p>Example:
 *
 * <pre>{@code
 * SessionConfig config = SessionConfig.builder()
 *     .execution(ExecutionOptions.builder()
 *         .batchSize(4096)
 *         .targetPartitions(8)
 *         .build())
 *     .catalog(CatalogOptions.builder()
 *         .informationSchema(true)
 *         .build())
 *     .build();
 *
 * try (SessionContext ctx = new SessionContext(config)) {
 *     DataFrame df = ctx.sql("SELECT 1");
 * }
 * }</pre>
 */
public final class SessionConfig {

  private static final boolean DEFAULT_FULL_STACK_TRACE =
      System.getenv("FULL_JAVA_STACK_TRACE") != null
          && !System.getenv("FULL_JAVA_STACK_TRACE").isEmpty();

  private final boolean fullStackTrace;
  private final CatalogOptions catalog;
  private final ExecutionOptions execution;
  private final OptimizerOptions optimizer;
  private final SqlParserOptions sqlParser;
  private final ExplainOptions explain;
  private final FormatOptions format;
  private final Map<String, String> additionalOptions;
  private final Map<String, String> rawOptions;

  private SessionConfig(Builder builder) {
    this.fullStackTrace = builder.fullStackTrace;
    this.catalog = builder.catalog;
    this.execution = builder.execution;
    this.optimizer = builder.optimizer;
    this.sqlParser = builder.sqlParser;
    this.explain = builder.explain;
    this.format = builder.format;
    this.additionalOptions = Collections.unmodifiableMap(new LinkedHashMap<>(builder.additional));
    this.rawOptions = null;
  }

  private SessionConfig(Map<String, String> rawOptions, boolean fullStackTrace) {
    this.fullStackTrace = fullStackTrace;
    this.catalog = null;
    this.execution = null;
    this.optimizer = null;
    this.sqlParser = null;
    this.explain = null;
    this.format = null;
    this.additionalOptions = Collections.emptyMap();
    this.rawOptions = Collections.unmodifiableMap(new LinkedHashMap<>(rawOptions));
  }

  /** Returns the default configuration (reads from environment variables). */
  public static SessionConfig defaults() {
    return new Builder().build();
  }

  /** Returns a builder for creating custom configurations. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Creates a SessionConfig from a raw string map, bypassing typed option records.
   *
   * <p>Keys should use DataFusion's dotted notation (e.g., {@code
   * "datafusion.execution.batch_size"}).
   *
   * @param options the configuration options map
   * @return a new SessionConfig
   */
  public static SessionConfig fromStringMap(Map<String, String> options) {
    return new SessionConfig(options, DEFAULT_FULL_STACK_TRACE);
  }

  /**
   * When true, exceptions in FFI callbacks include the full Java stack trace. Defaults to the value
   * of the FULL_JAVA_STACK_TRACE environment variable.
   */
  public boolean fullStackTrace() {
    return fullStackTrace;
  }

  /**
   * Returns true if any DataFusion config options have been set (either via typed builders, raw
   * options, or additional options).
   */
  public boolean hasOptions() {
    if (rawOptions != null && !rawOptions.isEmpty()) {
      return true;
    }
    if (!additionalOptions.isEmpty()) {
      return true;
    }
    return catalog != null
        || execution != null
        || optimizer != null
        || sqlParser != null
        || explain != null
        || format != null;
  }

  /**
   * Serializes all configured options into a flat {@code Map<String, String>} suitable for passing
   * to the Rust FFI layer.
   *
   * <p>Typed options are written first, then additional raw options are merged on top (raw options
   * take precedence on conflict).
   *
   * @return the serialized options map
   */
  public Map<String, String> toOptionsMap() {
    if (rawOptions != null) {
      return new LinkedHashMap<>(rawOptions);
    }

    Map<String, String> map = new LinkedHashMap<>();

    if (catalog != null) {
      catalog.writeTo(map);
    }
    if (execution != null) {
      execution.writeTo(map);
    }
    if (optimizer != null) {
      optimizer.writeTo(map);
    }
    if (sqlParser != null) {
      sqlParser.writeTo(map);
    }
    if (explain != null) {
      explain.writeTo(map);
    }
    if (format != null) {
      format.writeTo(map);
    }

    map.putAll(additionalOptions);

    return map;
  }

  /** Builder for SessionConfig. */
  public static final class Builder {
    private boolean fullStackTrace = DEFAULT_FULL_STACK_TRACE;
    private CatalogOptions catalog;
    private ExecutionOptions execution;
    private OptimizerOptions optimizer;
    private SqlParserOptions sqlParser;
    private ExplainOptions explain;
    private FormatOptions format;
    private final Map<String, String> additional = new LinkedHashMap<>();

    private Builder() {}

    /**
     * Sets whether to include full stack traces in FFI callback errors.
     *
     * @param fullStackTrace true to include full stack traces
     * @return this builder
     */
    public Builder fullStackTrace(boolean fullStackTrace) {
      this.fullStackTrace = fullStackTrace;
      return this;
    }

    /**
     * Sets the catalog options.
     *
     * @param catalog the catalog options
     * @return this builder
     */
    public Builder catalog(CatalogOptions catalog) {
      this.catalog = catalog;
      return this;
    }

    /**
     * Sets the execution options.
     *
     * @param execution the execution options
     * @return this builder
     */
    public Builder execution(ExecutionOptions execution) {
      this.execution = execution;
      return this;
    }

    /**
     * Sets the optimizer options.
     *
     * @param optimizer the optimizer options
     * @return this builder
     */
    public Builder optimizer(OptimizerOptions optimizer) {
      this.optimizer = optimizer;
      return this;
    }

    /**
     * Sets the SQL parser options.
     *
     * @param sqlParser the SQL parser options
     * @return this builder
     */
    public Builder sqlParser(SqlParserOptions sqlParser) {
      this.sqlParser = sqlParser;
      return this;
    }

    /**
     * Sets the explain options.
     *
     * @param explain the explain options
     * @return this builder
     */
    public Builder explain(ExplainOptions explain) {
      this.explain = explain;
      return this;
    }

    /**
     * Sets the display format options.
     *
     * @param format the format options
     * @return this builder
     */
    public Builder format(FormatOptions format) {
      this.format = format;
      return this;
    }

    /**
     * Sets a raw configuration option using DataFusion's dotted-key notation.
     *
     * <p>This is useful for options not yet exposed by the typed builders, or for dynamic
     * configuration.
     *
     * @param key the option key (e.g., "datafusion.execution.batch_size")
     * @param value the option value
     * @return this builder
     */
    public Builder option(String key, String value) {
      this.additional.put(key, value);
      return this;
    }

    /** Builds the SessionConfig. */
    public SessionConfig build() {
      return new SessionConfig(this);
    }
  }
}
