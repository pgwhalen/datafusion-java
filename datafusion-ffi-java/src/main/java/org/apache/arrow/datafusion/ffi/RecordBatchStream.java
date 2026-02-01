package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Set;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream of record batches from a DataFusion query execution.
 *
 * <p>This class provides zero-copy access to Arrow data returned from DataFusion through the Arrow
 * C Data Interface. Implements DictionaryProvider to allow decoding of dictionary-encoded columns.
 */
public class RecordBatchStream implements RecordBatchReader, DictionaryProvider {
  private static final Logger logger = LoggerFactory.getLogger(RecordBatchStream.class);

  // Size of FFI_ArrowSchema and FFI_ArrowArray structures
  // These are C structs with specific layouts
  private static final long ARROW_SCHEMA_SIZE = 72; // sizeof(FFI_ArrowSchema)
  private static final long ARROW_ARRAY_SIZE = 80; // sizeof(FFI_ArrowArray)

  private final MemorySegment runtime;
  private final MemorySegment stream;
  private final BufferAllocator allocator;
  private final CDataDictionaryProvider dictionaryProvider;
  private VectorSchemaRoot vectorSchemaRoot;
  private boolean initialized = false;
  private volatile boolean closed = false;

  RecordBatchStream(MemorySegment runtime, MemorySegment stream, BufferAllocator allocator) {
    this.runtime = runtime;
    this.stream = stream;
    this.allocator = allocator;
    this.dictionaryProvider = new CDataDictionaryProvider();
  }

  /**
   * Gets the VectorSchemaRoot that will be populated with data as the stream is iterated.
   *
   * @return The VectorSchemaRoot
   */
  public VectorSchemaRoot getVectorSchemaRoot() {
    ensureInitialized();
    return vectorSchemaRoot;
  }

  /**
   * Loads the next batch of data into the VectorSchemaRoot.
   *
   * @return true if a batch was loaded, false if the stream is exhausted
   * @throws DataFusionException if loading fails
   */
  public boolean loadNextBatch() {
    checkNotClosed();
    ensureInitialized();

    try (Arena arena = Arena.ofConfined()) {
      // Allocate space for FFI structs
      MemorySegment arrayOut = arena.allocate(ARROW_ARRAY_SIZE);
      MemorySegment schemaOut = arena.allocate(ARROW_SCHEMA_SIZE);
      MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

      int result =
          (int)
              DataFusionBindings.STREAM_NEXT.invokeExact(
                  runtime, stream, arrayOut, schemaOut, errorOut);

      NativeUtil.checkStreamResult(result, errorOut, "Stream next");

      if (result == 0) {
        // End of stream
        logger.debug("End of stream reached");
        return false;
      }

      // Import the batch data using Arrow C Data Interface
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
      MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

      int result = (int) DataFusionBindings.STREAM_SCHEMA.invokeExact(stream, schemaOut, errorOut);

      NativeUtil.checkResult(result, errorOut, "Get schema");

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

  /**
   * Looks up a dictionary by its ID.
   *
   * @param id The dictionary ID
   * @return The Dictionary, or null if not found
   */
  @Override
  public Dictionary lookup(long id) {
    return dictionaryProvider.lookup(id);
  }

  /**
   * Gets the set of all dictionary IDs in this provider.
   *
   * @return Set of dictionary IDs
   */
  @Override
  public Set<Long> getDictionaryIds() {
    return dictionaryProvider.getDictionaryIds();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        DataFusionBindings.STREAM_DESTROY.invokeExact(stream);
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
