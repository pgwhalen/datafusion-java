package org.apache.arrow.datafusion;

/**
 * An opaque handle to a DataFusion RuntimeEnv.
 *
 * <p>A RuntimeEnv provides shared services (memory pool, disk manager, object store registry) to a
 * {@link SessionContext}. Use {@link RuntimeEnvBuilder} to create instances with custom
 * configuration such as memory limits.
 *
 * <p>Example:
 *
 * <pre>{@code
 * RuntimeEnv rt = RuntimeEnvBuilder.builder()
 *     .withMemoryLimit(50_000_000, 1.0)
 *     .build();
 * SessionContext ctx = SessionContext.newWithConfigRt(SessionConfig.defaults(), rt);
 * // ... use ctx ...
 * ctx.close();
 * rt.close();
 * }</pre>
 */
public class RuntimeEnv implements AutoCloseable {
  final RuntimeEnvBridge bridge;

  /** Package-private constructor — use {@link RuntimeEnvBuilder} to create instances. */
  RuntimeEnv(RuntimeEnvBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public void close() {
    bridge.close();
  }
}
