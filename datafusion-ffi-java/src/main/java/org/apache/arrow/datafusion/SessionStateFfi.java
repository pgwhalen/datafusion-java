package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for SessionState.
 *
 * <p>This class manages the native pointer lifecycle for a session state and contains all native
 * call logic. It exists in the ffi package to keep {@code java.lang.foreign} and {@link NativeUtil}
 * usage out of the public API.
 */
final class SessionStateFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionStateFfi.class);

  private static final MethodHandle SESSION_STATE_DESTROY =
      NativeUtil.downcall(
          "datafusion_session_state_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle SESSION_STATE_CREATE_LOGICAL_PLAN =
      NativeUtil.downcall(
          "datafusion_session_state_create_logical_plan",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("state_with_rt"),
              ValueLayout.ADDRESS.withName("sql"),
              ValueLayout.ADDRESS.withName("error_out")));

  private final MemorySegment stateWithRuntime;
  private volatile boolean closed = false;

  SessionStateFfi(MemorySegment stateWithRuntime) {
    this.stateWithRuntime = stateWithRuntime;
  }

  /**
   * Creates a logical plan from a SQL string.
   *
   * @param sql the SQL query to parse into a logical plan
   * @return a LogicalPlanFfi wrapping the native plan pointer
   * @throws DataFusionException if the SQL is invalid or planning fails
   */
  LogicalPlanFfi createLogicalPlan(String sql) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment sqlSegment = arena.allocateFrom(sql);

      MemorySegment plan =
          NativeUtil.callForPointer(
              arena,
              "Create logical plan",
              errorOut ->
                  (MemorySegment)
                      SESSION_STATE_CREATE_LOGICAL_PLAN.invokeExact(
                          stateWithRuntime, sqlSegment, errorOut));

      logger.debug("Created LogicalPlan: {}", plan);
      return new LogicalPlanFfi(plan);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create logical plan", e);
    }
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("SessionState has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        SESSION_STATE_DESTROY.invokeExact(stateWithRuntime);
        logger.debug("Closed SessionState");
      } catch (Throwable e) {
        logger.error("Error closing SessionState", e);
      }
    }
  }
}
