package org.apache.arrow.datafusion;

import java.util.function.Consumer;
import org.apache.arrow.datafusion.config.ConfigOptions;

/**
 * Manages session contexts
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.execution.SessionContext} directly.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
public class SessionContexts {

  private SessionContexts() {}

  /**
   * Create a new default session context
   *
   * @return The created context
   */
  public static SessionContext create() {
    return new DefaultSessionContext(ConfigOptions.defaults());
  }

  /**
   * Create a new session context using the provided configuration
   *
   * @param config the configuration for the session
   * @return The created context
   */
  public static SessionContext withConfig(SessionConfig config) {
    return new DefaultSessionContext(config.toConfigOptions());
  }

  /**
   * Create a new session context using the provided callback to configure the session
   *
   * @param configuration callback to modify the {@link SessionConfig} for the session
   * @return The created context
   * @throws Exception if an error is encountered closing the session config resource
   */
  public static SessionContext withConfig(Consumer<SessionConfig> configuration) throws Exception {
    try (SessionConfig config = new SessionConfig().withConfiguration(configuration)) {
      return new DefaultSessionContext(config.toConfigOptions());
    }
  }
}
