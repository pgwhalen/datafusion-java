package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.RecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for DataFrame.
 *
 * <p>This class owns the native DataFrame pointer and contains all native call logic. The public
 * {@code DataFrame} class delegates to this.
 */
public final class DataFrameFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrameFfi.class);

  private final MemorySegment runtime;
  private final MemorySegment dataframe;
  private volatile boolean closed = false;

  DataFrameFfi(MemorySegment runtime, MemorySegment dataframe) {
    this.runtime = runtime;
    this.dataframe = dataframe;
  }

  /**
   * Executes the DataFrame and returns a stream of record batches.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A RecordBatchStream for iterating over results
   * @throws DataFusionException if execution fails
   */
  public RecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment stream =
          NativeUtil.callForPointer(
              arena,
              "Execute stream",
              errorOut ->
                  (MemorySegment)
                      DataFusionBindings.DATAFRAME_EXECUTE_STREAM.invokeExact(
                          runtime, dataframe, errorOut));

      logger.debug("Created RecordBatchStream: {}", stream);
      return new RecordBatchStream(new RecordBatchStreamFfi(runtime, stream, allocator));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to execute stream", e);
    }
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("DataFrame has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        DataFusionBindings.DATAFRAME_DESTROY.invokeExact(dataframe);
        logger.debug("Closed DataFrame");
      } catch (Throwable e) {
        logger.error("Error closing DataFrame", e);
      }
    }
  }
}
