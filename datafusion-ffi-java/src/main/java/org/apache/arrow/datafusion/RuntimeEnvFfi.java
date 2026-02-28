package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for RuntimeEnv.
 *
 * <p>This class owns the native RuntimeEnv pointer (a boxed {@code Arc<RuntimeEnv>}) and contains
 * all native call logic. The public {@link RuntimeEnv} class delegates to this.
 */
final class RuntimeEnvFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeEnvFfi.class);

  private static final MethodHandle RUNTIME_ENV_CREATE =
      NativeUtil.downcall(
          "datafusion_runtime_env_create",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.JAVA_LONG.withName("max_memory"),
              ValueLayout.JAVA_DOUBLE.withName("memory_fraction"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle RUNTIME_ENV_DESTROY =
      NativeUtil.downcall(
          "datafusion_runtime_env_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private final MemorySegment runtimeEnv;
  private volatile boolean closed = false;

  /**
   * Creates a new RuntimeEnvFfi with optional memory limit.
   *
   * @param maxMemory maximum memory in bytes (0 for no limit)
   * @param memoryFraction fraction of max_memory for the greedy memory pool (0.0-1.0)
   * @throws DataFusionException if creation fails
   */
  RuntimeEnvFfi(long maxMemory, double memoryFraction) {
    try (Arena arena = Arena.ofConfined()) {
      runtimeEnv =
          NativeUtil.callForPointer(
              arena,
              "Create RuntimeEnv",
              errorOut ->
                  (MemorySegment)
                      RUNTIME_ENV_CREATE.invokeExact(maxMemory, memoryFraction, errorOut));
      logger.debug("Created RuntimeEnv: {}", runtimeEnv);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create RuntimeEnv", e);
    }
  }

  /** Returns the native pointer to the RuntimeEnv (for passing to other FFI functions). */
  MemorySegment nativePointer() {
    if (closed) {
      throw new IllegalStateException("RuntimeEnv has been closed");
    }
    return runtimeEnv;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        RUNTIME_ENV_DESTROY.invokeExact(runtimeEnv);
        logger.debug("Closed RuntimeEnv");
      } catch (Throwable e) {
        throw new DataFusionException("Error closing RuntimeEnv", e);
      }
    }
  }
}
