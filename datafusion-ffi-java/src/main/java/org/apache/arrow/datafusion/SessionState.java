package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.ffi.SessionStateFfi;

/**
 * A snapshot of a DataFusion session's state, capable of creating logical plans.
 *
 * <p>A SessionState is obtained from {@link SessionContext#state()} and bundles the session
 * configuration with its own Tokio runtime. This means it can outlive the SessionContext that
 * created it.
 */
public class SessionState implements AutoCloseable {

  private final SessionStateFfi ffi;

  SessionState(SessionStateFfi ffi) {
    this.ffi = ffi;
  }

  /**
   * Creates a logical plan from a SQL string.
   *
   * @param sql the SQL query to parse into a logical plan
   * @return a LogicalPlan representing the parsed query
   * @throws DataFusionException if the SQL is invalid or planning fails
   */
  public LogicalPlan createLogicalPlan(String sql) {
    return new LogicalPlan(ffi.createLogicalPlan(sql));
  }

  @Override
  public void close() {
    ffi.close();
  }
}
