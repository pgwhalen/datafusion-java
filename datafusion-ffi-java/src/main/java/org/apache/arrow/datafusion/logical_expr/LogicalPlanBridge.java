package org.apache.arrow.datafusion.logical_expr;

import org.apache.arrow.datafusion.generated.DfLogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public LogicalPlan API and Diplomat-generated DfLogicalPlan.
 *
 * <p>This replaces LogicalPlanFfi, delegating to the Diplomat-generated class for all native calls.
 */
public final class LogicalPlanBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanBridge.class);

  private final DfLogicalPlan dfPlan;
  private volatile boolean closed = false;

  public LogicalPlanBridge(DfLogicalPlan dfPlan) {
    this.dfPlan = dfPlan;
  }

  public DfLogicalPlan dfPlan() {
    checkNotClosed();
    return dfPlan;
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
      dfPlan.close();
      logger.debug("Closed LogicalPlan");
    }
  }
}
