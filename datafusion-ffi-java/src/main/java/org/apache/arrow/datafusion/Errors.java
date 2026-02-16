package org.apache.arrow.datafusion;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/** Utility methods for error handling in FFI callbacks. */
final class Errors {

  /** The standard success return code for upcall callbacks. */
  static final int SUCCESS = 0;

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
  static int fromException(
      MemorySegment errorOut, Exception e, Arena arena, boolean fullStackTrace) {
    setError(errorOut, getErrorMessage(e, fullStackTrace), arena);
    return -1;
  }

  /**
   * Returns an error message for the given throwable.
   *
   * <p>When {@code fullStackTrace} is true, includes the full stack trace. Otherwise returns just
   * the exception message, falling back to the class name if the message is null.
   *
   * @param t the throwable to get the message from
   * @param fullStackTrace when true, includes the full stack trace
   * @return the error message string
   */
  static String getErrorMessage(Throwable t, boolean fullStackTrace) {
    if (fullStackTrace) {
      StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      return sw.toString();
    }
    String msg = t.getMessage();
    return msg != null ? msg : t.getClass().getName();
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
