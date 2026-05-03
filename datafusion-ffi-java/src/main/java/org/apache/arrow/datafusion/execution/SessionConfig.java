package org.apache.arrow.datafusion.execution;

import java.util.Map;
import org.apache.arrow.datafusion.config.ConfigOptions;

/**
 * Snapshot of a DataFusion session's configuration.
 *
 * <p>Returned by {@link TaskContext#sessionConfig()} and accepted by {@link
 * TaskContext#withSessionConfig(SessionConfig)}. The Rust type wraps {@code ConfigOptions} plus
 * session-specific extras (query planner, expression planners, etc.); this Java binding currently
 * exposes only the flat options map via {@link #options()}. Use {@link
 * #fromConfigOptions(ConfigOptions)} to build one from an existing {@link ConfigOptions}.
 *
 * <p>Example:
 *
 * {@snippet :
 * SessionConfig cfg = taskContext.sessionConfig();
 * String batchSize = cfg.options().get("datafusion.execution.batch_size");
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/struct.SessionConfig.html">Rust
 *     DataFusion: SessionConfig</a>
 */
public class SessionConfig implements AutoCloseable {
  final SessionConfigBridge bridge;

  /** Package-private constructor — use {@link #fromConfigOptions(ConfigOptions)}. */
  SessionConfig(SessionConfigBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Creates a SessionConfig from a Java-side {@link ConfigOptions}. The options are flattened to
   * key/value strings and sent across FFI.
   *
   * <p>Example:
   *
   * {@snippet :
   * SessionConfig cfg = SessionConfig.fromConfigOptions(
   *     ConfigOptions.fromStringMap(Map.of("datafusion.execution.batch_size", "4096")));
   * }
   *
   * @param options the source config
   * @return a new SessionConfig
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/struct.SessionConfig.html#method.from_string_hash_map">Rust
   *     DataFusion: SessionConfig::from_string_hash_map</a>
   */
  public static SessionConfig fromConfigOptions(ConfigOptions options) {
    return new SessionConfig(SessionConfigBridge.fromOptionsMap(options.toOptionsMap()));
  }

  /**
   * Returns the configured options as a flat map.
   *
   * <p>Example:
   *
   * {@snippet :
   * Map<String, String> opts = sessionConfig.options();
   * }
   *
   * @return the options map
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/prelude/struct.SessionConfig.html#method.options">Rust
   *     DataFusion: SessionConfig::options</a>
   */
  public Map<String, String> options() {
    return bridge.options();
  }

  @Override
  public void close() {
    bridge.close();
  }
}
