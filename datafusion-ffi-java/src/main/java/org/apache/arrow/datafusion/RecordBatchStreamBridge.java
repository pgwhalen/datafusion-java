package org.apache.arrow.datafusion;

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
 * Bridge between public RecordBatchStream API and Diplomat-generated DfRecordBatchStream.
 *
 * <p>This replaces RecordBatchStreamFfi, delegating to the Diplomat-generated class for all native
 * calls.
 */
final class RecordBatchStreamBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RecordBatchStreamBridge.class);

  private final NativeRecordBatchStream dfStream;
  private final BufferAllocator allocator;
  private final CDataDictionaryProvider dictionaryProvider;
  private VectorSchemaRoot vectorSchemaRoot;
  private boolean initialized = false;
  private volatile boolean closed = false;

  RecordBatchStreamBridge(NativeRecordBatchStream dfStream, BufferAllocator allocator) {
    this.dfStream = dfStream;
    this.allocator = allocator;
    this.dictionaryProvider = new CDataDictionaryProvider();
  }

  VectorSchemaRoot getVectorSchemaRoot() {
    ensureInitialized();
    return vectorSchemaRoot;
  }

  boolean loadNextBatch() {
    checkNotClosed();
    ensureInitialized();

    try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {

      int result = dfStream.next(arrowArray.memoryAddress(), arrowSchema.memoryAddress());

      if (result == 0) {
        logger.debug("End of stream reached");
        return false;
      }

      Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, dictionaryProvider);

      logger.debug("Loaded batch with {} rows", vectorSchemaRoot.getRowCount());
      return true;
    } catch (DfError e) {
      throw new NativeDataFusionException(e);
    } catch (DataFusionException e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionException("Failed to load next batch", e);
    }
  }

  Dictionary lookup(long id) {
    return dictionaryProvider.lookup(id);
  }

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
    try (ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
      dfStream.schemaTo(arrowSchema.memoryAddress());
      return Data.importSchema(allocator, arrowSchema, dictionaryProvider);
    } catch (DfError e) {
      throw new NativeDataFusionException(e);
    } catch (Exception e) {
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
      Throwable firstError = null;

      try {
        dfStream.close();
      } catch (Throwable e) {
        firstError = e;
      }
      try {
        dictionaryProvider.close();
      } catch (Exception e) {
        if (firstError == null) firstError = e;
        else firstError.addSuppressed(e);
      }
      if (initialized && vectorSchemaRoot != null) {
        try {
          vectorSchemaRoot.close();
        } catch (Exception e) {
          if (firstError == null) firstError = e;
          else firstError.addSuppressed(e);
        }
      }

      if (firstError != null) {
        throw new DataFusionException("Error closing RecordBatchStream", firstError);
      }
      logger.debug("Closed RecordBatchStream");
    }
  }
}
