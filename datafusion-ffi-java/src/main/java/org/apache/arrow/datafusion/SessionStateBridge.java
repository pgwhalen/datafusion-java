package org.apache.arrow.datafusion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public SessionState API and Diplomat-generated DfSessionState.
 *
 * <p>This replaces SessionStateFfi, delegating to the Diplomat-generated class for all native
 * calls.
 */
final class SessionStateBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionStateBridge.class);

  private final DfSessionState dfState;
  private volatile boolean closed = false;

  SessionStateBridge(DfSessionState dfState) {
    this.dfState = dfState;
  }

  LogicalPlanBridge createLogicalPlan(String sql) {
    checkNotClosed();
    try {
      DfLogicalPlan plan = dfState.createLogicalPlan(sql);
      return new LogicalPlanBridge(plan);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to create logical plan", e);
    }
  }

  /** Returns the underlying DfSessionState for use by SessionBridge. */
  DfSessionState dfState() {
    return dfState;
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
      dfState.close();
      logger.debug("Closed SessionState");
    }
  }
}
