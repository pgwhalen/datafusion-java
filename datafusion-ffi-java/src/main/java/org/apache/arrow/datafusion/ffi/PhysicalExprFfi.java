package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.MemorySegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for PhysicalExpr.
 *
 * <p>This class manages the native pointer lifecycle for a physical expression. It exists in the
 * ffi package so that other FFI classes (e.g., {@link LiteralGuaranteeFfi}) can access the native
 * handle without exposing it in the public API.
 */
public final class PhysicalExprFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(PhysicalExprFfi.class);

  private final MemorySegment pointer;
  private volatile boolean closed = false;

  public PhysicalExprFfi(MemorySegment pointer) {
    this.pointer = pointer;
  }

  MemorySegment nativeHandle() {
    checkNotClosed();
    return pointer;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("PhysicalExpr has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        DataFusionBindings.PHYSICAL_EXPR_DESTROY.invokeExact(pointer);
        logger.debug("Closed PhysicalExpr");
      } catch (Throwable e) {
        logger.error("Error closing PhysicalExpr", e);
      }
    }
  }
}
