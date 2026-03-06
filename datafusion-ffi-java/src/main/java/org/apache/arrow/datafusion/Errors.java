package org.apache.arrow.datafusion;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

/** Utility methods for error handling in FFI callbacks. */
final class Errors {

  private Errors() {}

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

  static void writeException(long errorAddr, long errorCap, Throwable t, boolean fullStackTrace) {
    writeError(errorAddr, errorCap, getErrorMessage(t, fullStackTrace));
  }

  static void writeError(long errorAddr, long errorCap, String message) {
    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
    int len = (int) Math.min(bytes.length, errorCap);
    MemorySegment.ofAddress(errorAddr)
        .reinterpret(errorCap)
        .copyFrom(MemorySegment.ofArray(bytes).asSlice(0, len));
  }
}
