package org.apache.arrow.datafusion.physical_plan;

import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.logical_expr.Expr;

/**
 * An opaque handle to a DataFusion physical expression.
 *
 * <p>A PhysicalExpr is created by {@link Session#createPhysicalExpr} and represents a compiled
 * expression that can be analyzed for literal guarantees. Unlike {@link Expr}, this is an owned
 * handle and must be closed when no longer needed.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/trait.PhysicalExpr.html">Rust
 *     DataFusion: PhysicalExpr</a>
 */
public class PhysicalExpr implements AutoCloseable {

  private final PhysicalExprBridge bridge;

  /**
   * @apiNote This constructor is an implementation detail. The {@link PhysicalExprBridge} parameter
   *     type is package-private and not usable by external consumers.
   */
  public PhysicalExpr(PhysicalExprBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Returns the internal bridge for cross-package bridge access.
   *
   * @apiNote This method is an implementation detail. The return type {@link PhysicalExprBridge} is
   *     package-private and not usable by external consumers.
   * @return the bridge
   */
  public PhysicalExprBridge bridge() {
    return bridge;
  }

  @Override
  public void close() {
    bridge.close();
  }
}
