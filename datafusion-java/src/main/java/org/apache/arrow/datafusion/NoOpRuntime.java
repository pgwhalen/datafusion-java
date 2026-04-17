package org.apache.arrow.datafusion;

/**
 * A no-op runtime implementation. The new FFI library manages its own Tokio runtime internally, so
 * this exists only for API compatibility.
 */
@SuppressWarnings("deprecation")
final class NoOpRuntime implements Runtime {

  static final NoOpRuntime INSTANCE = new NoOpRuntime();

  private NoOpRuntime() {}

  @Override
  public long getPointer() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
