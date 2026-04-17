package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * A TableProvider that holds eagerly-collected Arrow data. Created by {@link
 * DefaultDataFrame#intoView()} and consumed by {@link DefaultSessionContext#registerTable(String,
 * TableProvider)}.
 */
@SuppressWarnings("deprecation")
class CollectedViewTableProvider implements TableProvider {
  final VectorSchemaRoot root;
  final DictionaryProvider dictionaryProvider;
  final BufferAllocator allocator;
  private final SendableRecordBatchStream stream;

  CollectedViewTableProvider(
      VectorSchemaRoot root,
      SendableRecordBatchStream stream,
      DictionaryProvider dictionaryProvider,
      BufferAllocator allocator) {
    this.root = root;
    this.stream = stream;
    this.dictionaryProvider = dictionaryProvider;
    this.allocator = allocator;
  }

  /**
   * Release all data held by this provider. Closes the stream (which frees vectors and native
   * resources). The standalone allocator is not closed to avoid leak detection false positives from
   * dictionary metadata that the C Data Interface may hold.
   */
  void releaseData() {
    stream.close();
  }

  @Override
  public long getPointer() {
    return 0;
  }
}
