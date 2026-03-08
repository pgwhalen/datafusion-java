package org.apache.arrow.datafusion;

/**
 * An opaque handle to a DataFusion physical expression.
 *
 * <p>A PhysicalExpr is created by {@link Session#createPhysicalExpr} and represents a compiled
 * expression that can be analyzed for literal guarantees. Unlike {@link Expr}, this is an owned
 * handle and must be closed when no longer needed.
 */
public class PhysicalExpr implements AutoCloseable {

  private final PhysicalExprBridge bridge;

  PhysicalExpr(PhysicalExprBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Returns the internal bridge for use by other bridge classes.
   *
   * @return the bridge
   */
  PhysicalExprBridge bridge() {
    return bridge;
  }

  @Override
  public void close() {
    bridge.close();
  }
}
