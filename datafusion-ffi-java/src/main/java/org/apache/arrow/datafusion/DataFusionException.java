package org.apache.arrow.datafusion;

/**
 * Base exception class for DataFusion errors.
 *
 * <p>This is the parent class for all exceptions thrown by the DataFusion FFI bindings.
 */
public class DataFusionException extends RuntimeException {
  public DataFusionException(String message) {
    super(message);
  }

  public DataFusionException(String message, Throwable cause) {
    super(message, cause);
  }
}
