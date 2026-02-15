package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for DataFrame.
 *
 * <p>This class owns the native DataFrame pointer and contains all native call logic. The public
 * {@code DataFrame} class delegates to this.
 */
final class DataFrameFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrameFfi.class);

  private static final MethodHandle DATAFRAME_DESTROY =
      NativeUtil.downcall(
          "datafusion_dataframe_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle DATAFRAME_EXECUTE_STREAM =
      NativeUtil.downcall(
          "datafusion_dataframe_execute_stream",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // rt
              ValueLayout.ADDRESS, // df
              ValueLayout.ADDRESS // error_out
              ));

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
  RecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment stream =
          NativeUtil.callForPointer(
              arena,
              "Execute stream",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_EXECUTE_STREAM.invokeExact(runtime, dataframe, errorOut));

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
        DATAFRAME_DESTROY.invokeExact(dataframe);
        logger.debug("Closed DataFrame");
      } catch (Throwable e) {
        logger.error("Error closing DataFrame", e);
      }
    }
  }
}
