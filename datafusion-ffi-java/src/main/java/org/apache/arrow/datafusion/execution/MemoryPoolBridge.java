package org.apache.arrow.datafusion.execution;

import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.generated.DfMemoryPool;

/** Bridge between public {@link MemoryPool} API and Diplomat-generated DfMemoryPool. */
final class MemoryPoolBridge implements AutoCloseable {
  private final DfMemoryPool df;
  private volatile boolean closed = false;

  MemoryPoolBridge(DfMemoryPool df) {
    this.df = df;
  }

  long reserved() {
    if (closed) {
      throw new IllegalStateException("MemoryPool has been closed");
    }
    try {
      return df.reserved();
    } catch (Exception e) {
      throw new DataFusionError("Failed to read MemoryPool.reserved()", e);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      df.close();
    }
  }
}
