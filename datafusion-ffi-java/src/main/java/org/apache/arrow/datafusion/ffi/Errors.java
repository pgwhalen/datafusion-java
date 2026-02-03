package org.apache.arrow.datafusion.ffi;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Utility methods for error handling in FFI callbacks. */
public final class Errors {

  /** The standard success return code for upcall callbacks. */
  public static final int SUCCESS = 0;

  private Errors() {}

  /**
   * Sets the error from an exception and returns an error code.
   *
   * <p>This is a convenience method for the common callback error handling pattern:
   *
   * <pre>{@code
   * } catch (Exception e) {
   *   return Errors.fromException(errorOut, e, arena, fullStackTrace);
   * }
   * }</pre>
   *
   * @param errorOut the error output segment
   * @param e the exception to report
   * @param arena the arena to allocate the error string in
   * @param fullStackTrace when true, includes the full stack trace; otherwise just the message
   * @return -1 (the standard error return code)
   */
  public static int fromException(
      MemorySegment errorOut, Exception e, Arena arena, boolean fullStackTrace) {
    String message;
    if (fullStackTrace) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      message = sw.toString();
    } else {
      message = e.getMessage();
    }
    setError(errorOut, message, arena);
    return -1;
  }

  private static void setError(MemorySegment errorOut, String message, Arena arena) {
    if (errorOut.equals(MemorySegment.NULL)) {
      return;
    }
    try {
      MemorySegment msgSegment = arena.allocateFrom(message);
      errorOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, msgSegment);
    } catch (Exception ignored) {
      // Best effort error reporting
    }
  }
}
