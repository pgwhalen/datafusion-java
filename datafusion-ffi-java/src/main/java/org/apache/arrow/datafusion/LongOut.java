package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Wraps a MemorySegment used for returning a long value to the caller.
 *
 * <p>This record provides a type-safe wrapper for FFI output long parameters, centralizing the
 * reinterpret and set logic.
 *
 * @param segment the memory segment to write the long to
 */
record LongOut(MemorySegment segment) {

  /**
   * Writes a long value to the output segment.
   *
   * @param value the long value to write
   */
  void set(long value) {
    segment.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, value);
  }
}
