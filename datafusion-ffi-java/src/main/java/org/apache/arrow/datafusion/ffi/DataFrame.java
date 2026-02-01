package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DataFrame representing the result of a DataFusion query.
 *
 * <p>This class wraps a native DataFusion DataFrame and provides methods for executing the query
 * and retrieving results.
 */
public class DataFrame implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrame.class);

  private final MemorySegment runtime;
  private final MemorySegment dataframe;
  private volatile boolean closed = false;

  DataFrame(MemorySegment runtime, MemorySegment dataframe) {
    this.runtime = runtime;
    this.dataframe = dataframe;
  }

  /**
   * Executes the DataFrame and returns a stream of record batches.
   *
   * @param allocator The buffer allocator for Arrow data
   * @return A RecordBatchStream for iterating over results
   * @throws RuntimeException if execution fails
   */
  public RecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment errorOut = arena.allocate(ValueLayout.ADDRESS);
      errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

      MemorySegment stream =
          (MemorySegment)
              DataFusionBindings.DATAFRAME_EXECUTE_STREAM.invokeExact(runtime, dataframe, errorOut);

      if (stream.equals(MemorySegment.NULL)) {
        MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
        if (!errorPtr.equals(MemorySegment.NULL)) {
          String errorMessage = errorPtr.reinterpret(1024).getUtf8String(0);
          DataFusionBindings.FREE_STRING.invokeExact(errorPtr);
          throw new RuntimeException("Execute stream failed: " + errorMessage);
        }
        throw new RuntimeException("Execute stream failed: unknown error");
      }

      logger.debug("Created RecordBatchStream: {}", stream);
      return new RecordBatchStream(runtime, stream, allocator);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to execute stream", e);
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
