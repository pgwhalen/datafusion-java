package org.apache.arrow.datafusion;

/**
 * Common interface over Diplomat-generated stream types.
 *
 * <p>Allows RecordBatchStreamBridge to work with both DfRecordBatchStream (eager collect) and
 * DfLazyRecordBatchStream (true batch-at-a-time streaming) without code duplication.
 */
interface NativeRecordBatchStream extends AutoCloseable {
  int next(long arrayOutAddr, long schemaOutAddr);

  void schemaTo(long outAddr);

  @Override
  void close();
}
