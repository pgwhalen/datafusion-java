package org.apache.arrow.datafusion;

@SuppressWarnings("deprecation")
final class TokioRuntime extends AbstractProxy implements Runtime {

  TokioRuntime() {
    super();
  }

  @Override
  void doClose(long pointer) {
    // no-op: the FFI library manages its own runtime
  }

  static TokioRuntime create() {
    return new TokioRuntime();
  }
}
