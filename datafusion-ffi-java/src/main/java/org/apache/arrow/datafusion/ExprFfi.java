package org.apache.arrow.datafusion;

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
final class ExprFfi {
  private final MemorySegment pointer;

  ExprFfi(MemorySegment pointer) {
    this.pointer = pointer;
  }

  MemorySegment nativeHandle() {
    return pointer;
  }
}
