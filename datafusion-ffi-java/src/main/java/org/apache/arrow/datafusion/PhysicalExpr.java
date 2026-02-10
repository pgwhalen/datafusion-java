package org.apache.arrow.datafusion;

/**
 * An opaque handle to a DataFusion physical expression.
 *
 * <p>A PhysicalExpr is created by {@link Session#createPhysicalExpr} and represents a compiled
 * expression that can be analyzed for literal guarantees. Unlike {@link Expr}, this is an owned
 * handle and must be closed when no longer needed.
 */
public class PhysicalExpr implements AutoCloseable {

  private final PhysicalExprFfi ffi;

  PhysicalExpr(PhysicalExprFfi ffi) {
    this.ffi = ffi;
  }

  /**
   * Returns the internal FFI helper for use by other FFI classes.
   *
   * @return the FFI helper
   */
  PhysicalExprFfi ffi() {
    return ffi;
  }

  @Override
  public void close() {
    ffi.close();
  }
}
