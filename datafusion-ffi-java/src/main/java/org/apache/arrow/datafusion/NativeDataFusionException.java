package org.apache.arrow.datafusion;

/**
 * Exception indicating an error originating from the native DataFusion library.
 *
 * <p>This is package-private because users should catch the parent {@link DataFusionException}.
 */
final class NativeDataFusionException extends DataFusionException {
  NativeDataFusionException(DfError error) {
    super(extractMessage(error));
  }

  private static String extractMessage(DfError error) {
    try (error) {
      return error.toDisplay();
    }
  }
}
