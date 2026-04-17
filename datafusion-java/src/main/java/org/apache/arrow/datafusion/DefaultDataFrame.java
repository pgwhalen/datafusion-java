package org.apache.arrow.datafusion;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
class DefaultDataFrame extends AbstractProxy implements DataFrame {

  private static final Logger logger = LoggerFactory.getLogger(DefaultDataFrame.class);
  private final DefaultSessionContext context;
  private final org.apache.arrow.datafusion.dataframe.DataFrame ffiDf;

  DefaultDataFrame(
      DefaultSessionContext context, org.apache.arrow.datafusion.dataframe.DataFrame ffiDf) {
    super();
    this.context = context;
    this.ffiDf = ffiDf;
  }

  @Override
  public CompletableFuture<ArrowReader> collect(BufferAllocator allocator) {
    CompletableFuture<ArrowReader> result = new CompletableFuture<>();
    try {
      SendableRecordBatchStream stream = ffiDf.collect(allocator);
      result.complete(new StreamBackedArrowReader(stream, allocator));
    } catch (Exception e) {
      result.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return result;
  }

  @Override
  public CompletableFuture<RecordBatchStream> executeStream(BufferAllocator allocator) {
    CompletableFuture<RecordBatchStream> result = new CompletableFuture<>();
    try {
      SendableRecordBatchStream stream = ffiDf.executeStream(allocator);
      result.complete(new DefaultRecordBatchStream(stream));
    } catch (Exception e) {
      result.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return result;
  }

  @Override
  public CompletableFuture<Void> show() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      ffiDf.show();
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> writeParquet(Path path) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      ffiDf.writeParquet(path.toAbsolutePath().toString());
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> writeCsv(Path path) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      ffiDf.writeCsv(path.toAbsolutePath().toString());
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(new RuntimeException(e.getMessage(), e));
    }
    return future;
  }

  @Override
  public TableProvider intoView() {
    // Use a dedicated allocator for intoView data that won't cause leak detection
    // issues. The SendableRecordBatchStream.close() from collect() may leave dictionary
    // metadata allocated; using a standalone RootAllocator avoids polluting the
    // context's allocator with these leftovers.
    RootAllocator viewAllocator = new RootAllocator();
    SendableRecordBatchStream stream = ffiDf.collect(viewAllocator);
    VectorSchemaRoot root = stream.getVectorSchemaRoot();

    // Populate the root with data; without this the root has the schema but zero rows
    stream.loadNextBatch();

    return new CollectedViewTableProvider(root, stream, stream, viewAllocator);
  }

  @Override
  void doClose(long pointer) {
    ffiDf.close();
  }
}
