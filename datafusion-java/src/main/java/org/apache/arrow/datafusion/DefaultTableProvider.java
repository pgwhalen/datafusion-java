package org.apache.arrow.datafusion;

@SuppressWarnings("deprecation")
class DefaultTableProvider extends AbstractProxy implements TableProvider {
  DefaultTableProvider() {
    super();
  }

  @Override
  void doClose(long pointer) throws Exception {
    // no-op
  }
}
