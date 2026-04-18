package org.apache.arrow.datafusion.execution;

/**
 * An opaque handle to a DataFusion memory pool.
 *
 * <p>Obtained from {@link TaskContext#memoryPool()}. The only accessor currently exposed is
 * {@link #reserved()}. Additional methods from Rust's {@code MemoryPool} trait (e.g. {@code
 * pool_size}, {@code reserve}, {@code grow}) will be added in future changes.
 *
 * <p>Example:
 *
 * {@snippet :
 * try (MemoryPool pool = taskContext.memoryPool()) {
 *     long bytes = pool.reserved();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/memory_pool/trait.MemoryPool.html">Rust
 *     DataFusion: MemoryPool</a>
 */
public class MemoryPool implements AutoCloseable {
  private final MemoryPoolBridge bridge;

  /** Package-private constructor — obtain instances via {@link TaskContext#memoryPool()}. */
  MemoryPool(MemoryPoolBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Total bytes currently reserved across all consumers of this pool.
   *
   * <p>See {@link MemoryPool} class-level example.
   *
   * @return reserved bytes
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/memory_pool/trait.MemoryPool.html#tymethod.reserved">Rust
   *     DataFusion: MemoryPool::reserved</a>
   */
  public long reserved() {
    return bridge.reserved();
  }

  @Override
  public void close() {
    bridge.close();
  }
}
