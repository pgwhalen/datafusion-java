package org.apache.arrow.datafusion;

/**
 * An opaque handle to a DataFusion logical plan.
 *
 * <p>A LogicalPlan is created by {@link SessionState#createLogicalPlan(String)} and represents a
 * parsed SQL query. It can be passed back to DataFusion for further processing. The plan is a pure
 * data structure with no runtime dependency, so it can outlive both the SessionState and
 * SessionContext that created it.
 */
public class LogicalPlan implements AutoCloseable {

  private final LogicalPlanBridge bridge;

  LogicalPlan(LogicalPlanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public void close() {
    bridge.close();
  }
}
