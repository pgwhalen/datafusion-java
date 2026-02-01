package org.apache.arrow.datafusion.ffi;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Common interface for reading record batches.
 *
 * <p>This interface is used in two directions:
 *
 * <ul>
 *   <li>Consuming data from Rust: {@link RecordBatchStream} implements this to iterate over query
 *       results
 *   <li>Providing data to Rust: User implementations provide data for custom {@link ExecutionPlan}s
 * </ul>
 *
 * <p>The pattern follows Arrow's ArrowReader approach: call {@link #getVectorSchemaRoot()} once to
 * get the schema and buffer container, then call {@link #loadNextBatch()} to populate it with each
 * batch of data.
 */
public interface RecordBatchReader extends AutoCloseable {
  /**
   * Gets the VectorSchemaRoot that will be populated with data as batches are loaded.
   *
   * <p>The returned VectorSchemaRoot is reused across batches. Each call to {@link
   * #loadNextBatch()} updates its contents.
   *
   * @return The VectorSchemaRoot containing the schema and current batch data
   */
  VectorSchemaRoot getVectorSchemaRoot();

  /**
   * Loads the next batch of data into the VectorSchemaRoot.
   *
   * <p>After this method returns true, the VectorSchemaRoot from {@link #getVectorSchemaRoot()}
   * will contain the new batch data. When this method returns false, the stream is exhausted and no
   * more data is available.
   *
   * @return true if a batch was loaded, false if no more batches are available
   * @throws DataFusionException if loading fails
   */
  boolean loadNextBatch();

  /**
   * Releases resources held by this reader.
   *
   * <p>After calling close, the reader should not be used.
   */
  @Override
  void close();
}
