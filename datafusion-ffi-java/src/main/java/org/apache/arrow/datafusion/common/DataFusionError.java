package org.apache.arrow.datafusion.common;

/**
 * Base exception class for DataFusion errors.
 *
 * <p>This is the parent class for all exceptions thrown by the DataFusion FFI bindings.
 *
 * <p>Example:
 *
 * <p>{@snippet : try { DataFrame df = ctx.sql("INVALID SQL"); } catch (DataFusionError e) {
 * System.err.println("DataFusion error: " + e.getMessage()); } }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/error/enum.DataFusionError.html">Rust
 *     DataFusion: DataFusionError</a>
 */
public class DataFusionError extends RuntimeException {
  /**
   * Creates a new DataFusionError with the specified message.
   *
   * @param message the error message
   */
  public DataFusionError(String message) {
    super(message);
  }

  /**
   * Creates a new DataFusionError with the specified message and cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public DataFusionError(String message, Throwable cause) {
    super(message, cause);
  }
}
