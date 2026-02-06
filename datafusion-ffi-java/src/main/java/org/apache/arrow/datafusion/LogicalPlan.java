package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.datafusion.ffi.DataFusionBindings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An opaque handle to a DataFusion logical plan.
 *
 * <p>A LogicalPlan is created by {@link SessionState#createLogicalPlan(String)} and represents a
 * parsed SQL query. It can be passed back to DataFusion for further processing. The plan is a pure
 * data structure with no runtime dependency, so it can outlive both the SessionState and
 * SessionContext that created it.
 */
public class LogicalPlan implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlan.class);

  private final MemorySegment plan;
  private volatile boolean closed = false;

  LogicalPlan(MemorySegment plan) {
    this.plan = plan;
  }

  /**
   * Returns the native handle for passing back to DataFusion FFI functions.
   *
   * @return the native memory segment
   */
  MemorySegment nativeHandle() {
    checkNotClosed();
    return plan;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("LogicalPlan has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        DataFusionBindings.LOGICAL_PLAN_DESTROY.invokeExact(plan);
        logger.debug("Closed LogicalPlan");
      } catch (Throwable e) {
        logger.error("Error closing LogicalPlan", e);
      }
    }
  }
}
