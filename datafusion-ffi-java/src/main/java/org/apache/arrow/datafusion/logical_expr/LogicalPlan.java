package org.apache.arrow.datafusion.logical_expr;

import org.apache.arrow.datafusion.execution.SessionState;

/**
 * An opaque handle to a DataFusion logical plan.
 *
 * <p>A LogicalPlan is created by {@link SessionState#createLogicalPlan(String)} and represents a
 * parsed SQL query. It can be passed back to DataFusion for further processing. The plan is a pure
 * data structure with no runtime dependency, so it can outlive both the SessionState and
 * SessionContext that created it.
 *
 * <p>Example:
 *
 * <p>{@snippet : try (SessionState state = ctx.sessionState()) { LogicalPlan plan =
 * state.createLogicalPlan("SELECT * FROM t"); DataFrame df = ctx.executeLogicalPlan(plan); } }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/enum.LogicalPlan.html">Rust
 *     DataFusion: LogicalPlan</a>
 */
public class LogicalPlan implements AutoCloseable {

  private final LogicalPlanBridge bridge;

  public LogicalPlan(LogicalPlanBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Returns the underlying bridge. Internal use only.
   *
   * @return the bridge
   */
  public LogicalPlanBridge bridge() {
    return bridge;
  }

  @Override
  public void close() {
    bridge.close();
  }
}
