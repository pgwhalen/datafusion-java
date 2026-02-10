package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.ffi.DataFrameFfi;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A DataFrame representing the result of a DataFusion query.
 *
 * <p>This class wraps a native DataFusion DataFrame and provides methods for executing the query
 * and retrieving results.
 */
public class DataFrame implements AutoCloseable {
  private final DataFrameFfi ffi;

  DataFrame(DataFrameFfi ffi) {
    this.ffi = ffi;
  }

  /**
   * Executes the DataFrame and returns a stream of record batches.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A RecordBatchStream for iterating over results
   * @throws DataFusionException if execution fails
   */
  public RecordBatchStream executeStream(BufferAllocator allocator) {
    return ffi.executeStream(allocator);
  }

  @Override
  public void close() {
    ffi.close();
  }
}
