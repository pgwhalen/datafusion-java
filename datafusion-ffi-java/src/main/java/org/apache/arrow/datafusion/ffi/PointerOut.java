package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Wraps a MemorySegment used for returning a pointer value to the caller.
 *
 * <p>This record provides a type-safe wrapper for FFI output pointer parameters, centralizing the
 * reinterpret and set logic.
 *
 * @param segment the memory segment to write the pointer to
 */
record PointerOut(MemorySegment segment) {

  /**
   * Writes a pointer value to the output segment.
   *
   * @param value the pointer to write
   */
  public void set(MemorySegment value) {
    segment.reinterpret(8).set(ValueLayout.ADDRESS, 0, value);
  }

  /** Writes a null pointer to the output segment. */
  public void setNull() {
    set(MemorySegment.NULL);
  }
}
