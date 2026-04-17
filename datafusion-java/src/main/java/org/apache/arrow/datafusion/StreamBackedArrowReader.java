package org.apache.arrow.datafusion;

import java.io.IOException;
import java.util.Set;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a {@link SendableRecordBatchStream} to the {@link ArrowReader} interface used by the old
 * API's collect() method.
 */
class StreamBackedArrowReader extends ArrowReader {
  private final SendableRecordBatchStream stream;
  private long totalBytesRead = 0;

  StreamBackedArrowReader(SendableRecordBatchStream stream, BufferAllocator allocator) {
    super(allocator);
    this.stream = stream;
  }

  @Override
  public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
    return stream.getVectorSchemaRoot();
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    boolean hasData = stream.loadNextBatch();
    if (hasData) {
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      for (FieldVector vector : root.getFieldVectors()) {
        totalBytesRead += vector.getBufferSize();
      }
    }
    return hasData;
  }

  @Override
  protected Schema readSchema() throws IOException {
    return stream.getVectorSchemaRoot().getSchema();
  }

  @Override
  public Dictionary lookup(long id) {
    return stream.lookup(id);
  }

  @Override
  public Set<Long> getDictionaryIds() {
    return stream.getDictionaryIds();
  }

  @Override
  protected void closeReadSource() throws IOException {
    stream.close();
  }

  @Override
  public long bytesRead() {
    return totalBytesRead;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
