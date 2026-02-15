package org.apache.arrow.datafusion;

import org.junit.jupiter.api.Test;

/**
 * Validates that Java struct layouts match Rust struct sizes at build time. Each Handle class
 * exposes a package-private {@code validateSizes()} method that compares Java {@code StructLayout}
 * byte sizes against Rust-reported sizes via FFI downcalls.
 */
class FfiSizeValidationTest {
  @Test
  void executionPlanSizes() {
    ExecutionPlanHandle.validateSizes();
  }

  @Test
  void tableProviderSizes() {
    TableProviderHandle.validateSizes();
  }

  @Test
  void schemaProviderSizes() {
    SchemaProviderHandle.validateSizes();
  }

  @Test
  void recordBatchReaderSizes() {
    RecordBatchReaderHandle.validateSizes();
  }

  @Test
  void catalogProviderSizes() {
    CatalogProviderHandle.validateSizes();
  }

  @Test
  void fileFormatSizes() {
    FileFormatHandle.validateSizes();
  }

  @Test
  void fileSourceSizes() {
    FileSourceHandle.validateSizes();
  }

  @Test
  void fileOpenerSizes() {
    FileOpenerHandle.validateSizes();
  }

  @Test
  void scalarValueSizes() {
    ScalarValueFfi.validateSizes();
  }
}
