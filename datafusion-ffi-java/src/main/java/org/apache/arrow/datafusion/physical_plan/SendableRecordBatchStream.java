package org.apache.arrow.datafusion.physical_plan;

import java.util.Set;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * A stream of record batches from a DataFusion query execution.
 *
 * <p>This class provides zero-copy access to Arrow data returned from DataFusion through the Arrow
 * C Data Interface. Implements DictionaryProvider to allow decoding of dictionary-encoded columns.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/type.SendableRecordBatchStream.html">Rust
 *     DataFusion: SendableRecordBatchStream</a>
 */
public class SendableRecordBatchStream implements RecordBatchReader, DictionaryProvider {
  private final SendableRecordBatchStreamBridge bridge;

  /**
   * Creates a SendableRecordBatchStream from a bridge wrapper.
   *
   * @param bridge the bridge wrapper holding native stream and allocator
   */
  public SendableRecordBatchStream(SendableRecordBatchStreamBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    return bridge.getVectorSchemaRoot();
  }

  @Override
  public boolean loadNextBatch() {
    return bridge.loadNextBatch();
  }

  @Override
  public Dictionary lookup(long id) {
    return bridge.lookup(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return bridge.getDictionaryIds();
  }

  @Override
  public void close() {
    bridge.close();
  }
}
