package org.apache.arrow.datafusion;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;

@SuppressWarnings("deprecation")
class DefaultRecordBatchStream extends AbstractProxy implements RecordBatchStream {
  private final SendableRecordBatchStream ffiStream;

  DefaultRecordBatchStream(SendableRecordBatchStream ffiStream) {
    super();
    this.ffiStream = ffiStream;
  }

  @Override
  void doClose(long pointer) {
    ffiStream.close();
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() {
    return ffiStream.getVectorSchemaRoot();
  }

  @Override
  public CompletableFuture<Boolean> loadNextBatch() {
    CompletableFuture<Boolean> result = new CompletableFuture<>();
    try {
      boolean hasNext = ffiStream.loadNextBatch();
      result.complete(hasNext);
    } catch (Exception e) {
      result.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return result;
  }

  @Override
  public Dictionary lookup(long id) {
    return ffiStream.lookup(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return ffiStream.getDictionaryIds();
  }
}
