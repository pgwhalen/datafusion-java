package org.apache.arrow.datafusion.common;

/**
 * Base exception class for DataFusion errors.
 *
 * <p>This is the parent class for all exceptions thrown by the DataFusion FFI bindings.
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/error/enum.DataFusionError.html">Rust
 *     DataFusion: DataFusionError</a>
 */
public class DataFusionError extends RuntimeException {
  public DataFusionError(String message) {
    super(message);
  }

  public DataFusionError(String message, Throwable cause) {
    super(message, cause);
  }
}
