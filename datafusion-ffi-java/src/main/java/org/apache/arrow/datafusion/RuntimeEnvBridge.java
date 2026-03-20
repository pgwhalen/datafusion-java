package org.apache.arrow.datafusion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public RuntimeEnv API and Diplomat-generated DfRuntimeEnv.
 *
 * <p>This replaces RuntimeEnvFfi, delegating to the Diplomat-generated class for all native calls.
 */
final class RuntimeEnvBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeEnvBridge.class);

  private final DfRuntimeEnv dfRuntimeEnv;
  private volatile boolean closed = false;

  RuntimeEnvBridge(long maxMemory, double memoryFraction) {
    try {
      this.dfRuntimeEnv = DfRuntimeEnv.newWithMemoryLimit(maxMemory, memoryFraction);
      logger.debug("Created RuntimeEnv via Diplomat bridge");
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to create RuntimeEnv", e);
    }
  }

  /** Returns the underlying DfRuntimeEnv for passing to DfSessionContext. */
  DfRuntimeEnv dfRuntimeEnv() {
    if (closed) {
      throw new IllegalStateException("RuntimeEnv has been closed");
    }
    return dfRuntimeEnv;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dfRuntimeEnv.close();
      logger.debug("Closed RuntimeEnv");
    }
  }
}
