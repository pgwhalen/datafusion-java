package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for RecordBatchStream.
 *
 * <p>This class owns the native stream pointers and contains all native call logic. The public
 * {@code RecordBatchStream} class delegates to this.
 */
final class RecordBatchStreamFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RecordBatchStreamFfi.class);

  private static final MethodHandle STREAM_DESTROY =
      NativeUtil.downcall(
          "datafusion_stream_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle STREAM_SCHEMA =
      NativeUtil.downcall(
          "datafusion_stream_schema",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("stream"),
              ValueLayout.ADDRESS.withName("schema_out"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle STREAM_NEXT =
      NativeUtil.downcall(
          "datafusion_stream_next",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("stream"),
              ValueLayout.ADDRESS.withName("array_out"),
              ValueLayout.ADDRESS.withName("schema_out"),
              ValueLayout.ADDRESS.withName("error_out")));

  // Size of FFI_ArrowSchema and FFI_ArrowArray structures
  private static final long ARROW_SCHEMA_SIZE = 72; // sizeof(FFI_ArrowSchema)
  private static final long ARROW_ARRAY_SIZE = 80; // sizeof(FFI_ArrowArray)

  private final MemorySegment runtime;
  private final MemorySegment stream;
  private final BufferAllocator allocator;
  private final CDataDictionaryProvider dictionaryProvider;
  private VectorSchemaRoot vectorSchemaRoot;
  private boolean initialized = false;
  private volatile boolean closed = false;

  RecordBatchStreamFfi(MemorySegment runtime, MemorySegment stream, BufferAllocator allocator) {
    this.runtime = runtime;
    this.stream = stream;
    this.allocator = allocator;
    this.dictionaryProvider = new CDataDictionaryProvider();
  }

  /**
   * Gets the VectorSchemaRoot that will be populated with data as batches are loaded.
   *
   * @return The VectorSchemaRoot
   */
  VectorSchemaRoot getVectorSchemaRoot() {
    ensureInitialized();
    return vectorSchemaRoot;
  }

  /**
   * Loads the next batch of data into the VectorSchemaRoot.
   *
   * @return true if a batch was loaded, false if the stream is exhausted
   * @throws DataFusionException if loading fails
   */
  boolean loadNextBatch() {
    checkNotClosed();
    ensureInitialized();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment arrayOut = arena.allocate(ARROW_ARRAY_SIZE);
      MemorySegment schemaOut = arena.allocate(ARROW_SCHEMA_SIZE);

      int result =
          NativeUtil.callForStreamResult(
              arena,
              "Stream next",
              errorOut ->
                  (int) STREAM_NEXT.invokeExact(runtime, stream, arrayOut, schemaOut, errorOut));

      if (result == 0) {
        logger.debug("End of stream reached");
        return false;
      }

      ArrowArray arrowArray = ArrowArray.wrap(arrayOut.address());
      Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, dictionaryProvider);

      logger.debug("Loaded batch with {} rows", vectorSchemaRoot.getRowCount());
      return true;
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to load next batch", e);
    }
  }

  /**
   * Looks up a dictionary by its ID.
   *
   * @param id The dictionary ID
   * @return The Dictionary, or null if not found
   */
  Dictionary lookup(long id) {
    return dictionaryProvider.lookup(id);
  }

  /**
   * Gets the set of all dictionary IDs in this provider.
   *
   * @return Set of dictionary IDs
   */
  Set<Long> getDictionaryIds() {
    return dictionaryProvider.getDictionaryIds();
  }

  private void ensureInitialized() {
    if (!initialized) {
      Schema schema = getSchema();
      vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
      initialized = true;
    }
  }

  private Schema getSchema() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment schemaOut = arena.allocate(ARROW_SCHEMA_SIZE);

      NativeUtil.call(
          arena,
          "Get schema",
          errorOut -> (int) STREAM_SCHEMA.invokeExact(stream, schemaOut, errorOut));

      ArrowSchema arrowSchema = ArrowSchema.wrap(schemaOut.address());
      return Data.importSchema(allocator, arrowSchema, dictionaryProvider);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get schema", e);
    }
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("RecordBatchStream has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        STREAM_DESTROY.invokeExact(stream);
        dictionaryProvider.close();
        if (initialized && vectorSchemaRoot != null) {
          vectorSchemaRoot.close();
        }
        logger.debug("Closed RecordBatchStream");
      } catch (Throwable e) {
        logger.error("Error closing RecordBatchStream", e);
      }
    }
  }
}
