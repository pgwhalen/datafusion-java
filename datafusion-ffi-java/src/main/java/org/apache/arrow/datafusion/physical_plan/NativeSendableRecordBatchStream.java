package org.apache.arrow.datafusion.physical_plan;

/**
 * Common interface over Diplomat-generated stream types.
 *
 * <p>Allows SendableRecordBatchStreamBridge to work with both DfRecordBatchStream (eager collect)
 * and DfLazyRecordBatchStream (true batch-at-a-time streaming) without code duplication.
 */
public interface NativeSendableRecordBatchStream extends AutoCloseable {
  int next(long arrayOutAddr, long schemaOutAddr);

  void schemaTo(long outAddr);

  @Override
  void close();
}
