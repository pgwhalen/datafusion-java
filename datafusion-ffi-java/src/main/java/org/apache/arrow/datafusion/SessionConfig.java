package org.apache.arrow.datafusion;

/**
 * Configuration options for a SessionContext.
 *
 * <p>Use {@link #builder()} to create a new configuration with custom settings, or {@link
 * #defaults()} to use the default configuration.
 *
 * @param fullStackTrace When true, exceptions in FFI callbacks include the full Java stack trace.
 *     Defaults to the value of the FULL_JAVA_STACK_TRACE environment variable.
 */
public record SessionConfig(boolean fullStackTrace) {

  private static final boolean DEFAULT_FULL_STACK_TRACE =
      System.getenv("FULL_JAVA_STACK_TRACE") != null
          && !System.getenv("FULL_JAVA_STACK_TRACE").isEmpty();

  /** Returns the default configuration (reads from environment variables). */
  public static SessionConfig defaults() {
    return new SessionConfig(DEFAULT_FULL_STACK_TRACE);
  }

  /** Returns a builder for creating custom configurations. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for SessionConfig. */
  public static final class Builder {
    private boolean fullStackTrace = DEFAULT_FULL_STACK_TRACE;

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

    /** Builds the SessionConfig. */
    public SessionConfig build() {
      return new SessionConfig(fullStackTrace);
    }
  }
}
