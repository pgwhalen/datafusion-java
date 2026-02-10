package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.ffi.ExprFfi;

/**
 * An opaque handle to a DataFusion logical expression.
 *
 * <p>This is a borrowed pointer that is only valid during the {@link TableProvider#scan} callback.
 * It must NOT be stored beyond the lifetime of that callback. It is NOT {@link AutoCloseable}
 * because it does not own the underlying memory.
 */
public class Expr {
  private final ExprFfi ffi;

  /** Internal constructor. Users should not create Expr instances directly. */
  public Expr(ExprFfi ffi) {
    this.ffi = ffi;
  }

  /**
   * Returns the internal FFI helper for use by other FFI classes.
   *
   * @return the FFI helper
   */
  public ExprFfi ffi() {
    return ffi;
  }
}
