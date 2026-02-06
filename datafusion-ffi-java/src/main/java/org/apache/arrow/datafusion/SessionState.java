package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.apache.arrow.datafusion.ffi.DataFusionBindings;
import org.apache.arrow.datafusion.ffi.NativeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A snapshot of a DataFusion session's state, capable of creating logical plans.
 *
 * <p>A SessionState is obtained from {@link SessionContext#state()} and bundles the session
 * configuration with its own Tokio runtime. This means it can outlive the SessionContext that
 * created it.
 */
public class SessionState implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionState.class);

  private final MemorySegment stateWithRuntime;
  private volatile boolean closed = false;

  SessionState(MemorySegment stateWithRuntime) {
    this.stateWithRuntime = stateWithRuntime;
  }

  /**
   * Creates a logical plan from a SQL string.
   *
   * @param sql the SQL query to parse into a logical plan
   * @return a LogicalPlan representing the parsed query
   * @throws DataFusionException if the SQL is invalid or planning fails
   */
  public LogicalPlan createLogicalPlan(String sql) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment sqlSegment = arena.allocateFrom(sql);

      MemorySegment plan =
          NativeUtil.callForPointer(
              arena,
              "Create logical plan",
              errorOut ->
                  (MemorySegment)
                      DataFusionBindings.SESSION_STATE_CREATE_LOGICAL_PLAN.invokeExact(
                          stateWithRuntime, sqlSegment, errorOut));

      logger.debug("Created LogicalPlan: {}", plan);
      return new LogicalPlan(plan);
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
        DataFusionBindings.SESSION_STATE_DESTROY.invokeExact(stateWithRuntime);
        logger.debug("Closed SessionState");
      } catch (Throwable e) {
        logger.error("Error closing SessionState", e);
      }
    }
  }
}
