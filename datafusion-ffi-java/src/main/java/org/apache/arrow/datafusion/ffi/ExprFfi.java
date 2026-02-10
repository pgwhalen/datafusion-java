package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.MemorySegment;

/**
 * Internal FFI helper for Expr.
 *
 * <p>This class holds the native pointer for a borrowed logical expression. It exists in the ffi
 * package so that other FFI classes (e.g., {@link TableProviderHandle}) can access the native
 * handle without exposing it in the public API.
 *
 * <p>Unlike {@link PhysicalExprFfi}, this is a borrowed pointer that does not own the underlying
 * memory and has no lifecycle management.
 */
public final class ExprFfi {
  private final MemorySegment pointer;

  public ExprFfi(MemorySegment pointer) {
    this.pointer = pointer;
  }

  public MemorySegment nativeHandle() {
    return pointer;
  }
}
