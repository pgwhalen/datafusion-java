package org.apache.arrow.datafusion;

/**
 * Exception thrown when a native DataFusion function returns an error.
 *
 * <p>This exception wraps error messages returned from the native Rust code via the FFI boundary.
 * The error message is extracted from the native error pointer and the native memory is properly
 * freed before this exception is constructed.
 */
public class NativeErrorException extends DataFusionException {
  private final String operation;
  private final String nativeMessage;

  /**
   * Creates a new NativeErrorException.
   *
   * @param operation The operation that failed (e.g., "SQL execution", "register table")
   * @param nativeMessage The error message from the native code, or null if unavailable
   */
  public NativeErrorException(String operation, String nativeMessage) {
    super(formatMessage(operation, nativeMessage));
    this.operation = operation;
    this.nativeMessage = nativeMessage;
  }

  /**
   * Gets the operation that failed.
   *
   * @return The operation name
   */
  public String getOperation() {
    return operation;
  }

  /**
   * Gets the raw error message from native code.
   *
   * @return The native error message, or null if no message was available
   */
  public String getNativeMessage() {
    return nativeMessage;
  }

  private static String formatMessage(String operation, String nativeMessage) {
    if (nativeMessage != null && !nativeMessage.isEmpty()) {
      return operation + " failed: " + nativeMessage;
    }
    return operation + " failed: unknown error";
  }
}
