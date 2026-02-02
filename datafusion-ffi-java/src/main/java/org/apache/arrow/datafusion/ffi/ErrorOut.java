package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Wraps a MemorySegment used for returning an error message to the caller.
 *
 * <p>This record provides a type-safe wrapper for FFI error output parameters. It handles null
 * checks and string allocation, centralizing error reporting logic that was previously duplicated
 * across Handle classes.
 *
 * @param segment the memory segment to write the error pointer to
 */
record ErrorOut(MemorySegment segment) {

  /**
   * Writes an error message to the output segment.
   *
   * <p>If the segment is null, this method does nothing (best-effort error reporting).
   *
   * @param message the error message to write
   * @param arena the arena to allocate the string in
   */
  public void set(String message, Arena arena) {
    if (segment.equals(MemorySegment.NULL)) {
      return;
    }
    try {
      MemorySegment msgSegment = arena.allocateFrom(message);
      segment.reinterpret(8).set(ValueLayout.ADDRESS, 0, msgSegment);
    } catch (Exception ignored) {
      // Best effort error reporting
    }
  }
}
