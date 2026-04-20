package org.apache.arrow.datafusion.execution;

import org.apache.arrow.datafusion.common.DataFusionError;

/**
 * Builder for creating a {@link RuntimeEnv} with custom configuration.
 *
 * <p>This mirrors DataFusion's {@code RuntimeEnvBuilder} and supports configuring the memory pool
 * via {@link #withMemoryLimit(long, double)}.
 *
 * <p>Example:
 *
 * {@snippet :
 * // Create a runtime env with a 50MB memory limit
 * RuntimeEnv rt = RuntimeEnvBuilder.builder()
 *     .withMemoryLimit(50_000_000, 1.0)
 *     .build();
 *
 * // Create a default runtime env (no memory limit)
 * RuntimeEnv defaultRt = RuntimeEnvBuilder.builder().build();
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html">Rust
 *     DataFusion: RuntimeEnvBuilder</a>
 */
public final class RuntimeEnvBuilder {
  private long maxMemory = 0;
  private double memoryFraction = 1.0;

  private RuntimeEnvBuilder() {}

  /** Creates a new builder with default settings (no memory limit). */
  public static RuntimeEnvBuilder builder() {
    return new RuntimeEnvBuilder();
  }

  /**
   * Sets the memory limit for the runtime environment.
   *
   * <p>This configures a {@code GreedyMemoryPool} that limits total memory usage. Queries that
   * exceed this limit will fail with an out-of-memory error.
   *
   * <p>Example:
   *
   * {@snippet :
   * RuntimeEnv rt = RuntimeEnvBuilder.builder()
   *     .withMemoryLimit(50_000_000, 1.0)
   *     .build();
   * SessionContext ctx = SessionContext.newWithConfigRt(
   *     ConfigOptions.defaults(), rt);
   * }
   *
   * @param maxMemory the maximum memory in bytes (must be positive)
   * @param memoryFraction the fraction of maxMemory to use (must be between 0.0 exclusive and 1.0
   *     inclusive)
   * @return this builder
   * @throws IllegalArgumentException if maxMemory is not positive or memoryFraction is out of range
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html#method.with_memory_limit">Rust
   *     DataFusion: RuntimeEnvBuilder::with_memory_limit</a>
   */
  public RuntimeEnvBuilder withMemoryLimit(long maxMemory, double memoryFraction) {
    if (maxMemory <= 0) {
      throw new IllegalArgumentException("maxMemory must be positive, got: " + maxMemory);
    }
    if (memoryFraction <= 0.0 || memoryFraction > 1.0) {
      throw new IllegalArgumentException(
          "memoryFraction must be in (0.0, 1.0], got: " + memoryFraction);
    }
    this.maxMemory = maxMemory;
    this.memoryFraction = memoryFraction;
    return this;
  }

  /**
   * Builds the {@link RuntimeEnv}.
   *
   * @return a new RuntimeEnv
   * @throws DataFusionError if the native RuntimeEnv cannot be created
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html#method.build">Rust
   *     DataFusion: RuntimeEnvBuilder::build</a>
   */
  public RuntimeEnv build() {
    return new RuntimeEnv(new RuntimeEnvBridge(maxMemory, memoryFraction));
  }
}
