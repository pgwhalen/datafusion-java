package org.apache.arrow.datafusion;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for LogicalPlan.
 *
 * <p>This class manages the native pointer lifecycle for a logical plan. It exists in the ffi
 * package so that other FFI classes (e.g., {@link SessionStateFfi}) can create and manage logical
 * plans without exposing native pointers in the public API.
 */
final class LogicalPlanFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanFfi.class);

  private static final MethodHandle LOGICAL_PLAN_DESTROY =
      NativeUtil.downcall(
          "datafusion_logical_plan_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private final MemorySegment plan;
  private volatile boolean closed = false;

  LogicalPlanFfi(MemorySegment plan) {
    this.plan = plan;
  }

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
        LOGICAL_PLAN_DESTROY.invokeExact(plan);
        logger.debug("Closed LogicalPlan");
      } catch (Throwable e) {
        throw new DataFusionException("Error closing LogicalPlan", e);
      }
    }
  }
}
