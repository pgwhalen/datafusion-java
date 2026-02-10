package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.ffi.LogicalPlanFfi;

/**
 * An opaque handle to a DataFusion logical plan.
 *
 * <p>A LogicalPlan is created by {@link SessionState#createLogicalPlan(String)} and represents a
 * parsed SQL query. It can be passed back to DataFusion for further processing. The plan is a pure
 * data structure with no runtime dependency, so it can outlive both the SessionState and
 * SessionContext that created it.
 */
public class LogicalPlan implements AutoCloseable {

  private final LogicalPlanFfi ffi;

  LogicalPlan(LogicalPlanFfi ffi) {
    this.ffi = ffi;
  }

  @Override
  public void close() {
    ffi.close();
  }
}
