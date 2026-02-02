package org.apache.arrow.datafusion.ffi;

import java.io.PrintWriter;
import java.io.StringWriter;
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
public record ErrorOut(MemorySegment segment) {

  /** The standard success return code for upcall callbacks. */
  public static final int SUCCESS = 0;

  /**
   * When true, {@link #fromException} includes the full stack trace in error messages. Controlled
   * by the FULL_JAVA_STACK_TRACE environment variable.
   */
  public static final boolean FULL_STACK_TRACE =
      System.getenv("FULL_JAVA_STACK_TRACE") != null
          && !System.getenv("FULL_JAVA_STACK_TRACE").isEmpty();

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

  /**
   * Sets the error from an exception and returns an error code.
   *
   * <p>This is a convenience method for the common callback error handling pattern:
   *
   * <pre>{@code
   * } catch (Exception e) {
   *   return ErrorOut.fromException(errorOut, e, arena);
   * }
   * }</pre>
   *
   * <p>When the FULL_JAVA_STACK_TRACE environment variable is set, includes the full stack trace in
   * the error message. Otherwise, only the exception message is included.
   *
   * @param errorOut the error output segment
   * @param e the exception to report
   * @param arena the arena to allocate the error string in
   * @return -1 (the standard error return code)
   */
  public static int fromException(MemorySegment errorOut, Exception e, Arena arena) {
    String message;
    if (FULL_STACK_TRACE) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      message = sw.toString();
    } else {
      message = e.getMessage();
    }
    new ErrorOut(errorOut).set(message, arena);
    return -1;
  }
}
