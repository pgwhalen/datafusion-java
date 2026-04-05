package org.apache.arrow.datafusion.execution;

import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.logical_expr.LogicalPlan;

/**
 * A snapshot of a DataFusion session's state, capable of creating logical plans.
 *
 * <p>A SessionState is obtained from {@link SessionContext#state()} and bundles the session
 * configuration with its own Tokio runtime. This means it can outlive the SessionContext that
 * created it.
 *
 * <p>Example:
 *
 * <p>{@snippet : try (SessionContext ctx = new SessionContext()) { try (SessionState state =
 * ctx.state()) { LogicalPlan plan = state.createLogicalPlan( "SELECT 1 + 1");
 * System.out.println(plan); } } }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/session_state/struct.SessionState.html">Rust
 *     DataFusion: SessionState</a>
 */
public class SessionState implements AutoCloseable {

  private final SessionStateBridge bridge;

  SessionState(SessionStateBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Creates a logical plan from a SQL string.
   *
   * <p>Example:
   *
   * <p>{@snippet : try (SessionState state = ctx.state()) { LogicalPlan plan =
   * state.createLogicalPlan( "SELECT id, name FROM users WHERE age > 30");
   * System.out.println(plan); } }
   *
   * @param sql the SQL query to parse into a logical plan
   * @return a LogicalPlan representing the parsed query
   * @throws DataFusionError if the SQL is invalid or planning fails
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/session_state/struct.SessionState.html#method.create_logical_plan">Rust
   *     DataFusion: SessionState::create_logical_plan</a>
   */
  public LogicalPlan createLogicalPlan(String sql) {
    return new LogicalPlan(bridge.createLogicalPlan(sql));
  }

  @Override
  public void close() {
    bridge.close();
  }
}
