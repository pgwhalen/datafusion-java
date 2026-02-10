package org.apache.arrow.datafusion;

import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * A stream of record batches from a DataFusion query execution.
 *
 * <p>This class provides zero-copy access to Arrow data returned from DataFusion through the Arrow
 * C Data Interface. Implements DictionaryProvider to allow decoding of dictionary-encoded columns.
 */
public class RecordBatchStream implements RecordBatchReader, DictionaryProvider {
  private final RecordBatchStreamFfi ffi;

  /**
   * Creates a RecordBatchStream from an FFI wrapper.
   *
   * @param ffi the FFI wrapper holding native pointers and allocator
   */
  RecordBatchStream(RecordBatchStreamFfi ffi) {
    this.ffi = ffi;
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    return ffi.getVectorSchemaRoot();
  }

  @Override
  public boolean loadNextBatch() {
    return ffi.loadNextBatch();
  }

  @Override
  public Dictionary lookup(long id) {
    return ffi.lookup(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return ffi.getDictionaryIds();
  }

  @Override
  public void close() {
    ffi.close();
  }
}
