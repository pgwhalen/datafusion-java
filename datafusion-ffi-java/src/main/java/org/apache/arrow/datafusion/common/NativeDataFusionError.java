package org.apache.arrow.datafusion.common;

import org.apache.arrow.datafusion.DfError;

/**
 * Exception indicating an error originating from the native DataFusion library.
 *
 * <p>This is package-private because users should catch the parent {@link DataFusionError}.
 */
public final class NativeDataFusionError extends DataFusionError {
  public NativeDataFusionError(DfError error) {
    super(extractMessage(error));
  }

  private static String extractMessage(DfError error) {
    try (error) {
      return error.toDisplay();
    }
  }
}
