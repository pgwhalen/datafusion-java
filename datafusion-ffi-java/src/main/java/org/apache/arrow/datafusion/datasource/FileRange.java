package org.apache.arrow.datafusion.datasource;

/**
 * A byte range within a file, specifying a contiguous segment to read.
 *
 * <p>Example:
 *
 * <p>{@snippet : FileRange range = new FileRange(0, 1024); long start = range.start(); // 0 long
 * end = range.end(); // 1024 long length = end - start; // 1024 bytes }
 *
 * @param start start byte offset (inclusive)
 * @param end end byte offset (exclusive)
 * @see <a
 *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.FileRange.html">Rust
 *     DataFusion: FileRange</a>
 */
public record FileRange(long start, long end) {

  /**
   * Returns the start byte offset (inclusive).
   *
   * @return start offset
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.FileRange.html#structfield.start">Rust
   *     DataFusion: FileRange::start</a>
   */
  public long start() {
    return start;
  }

  /**
   * Returns the end byte offset (exclusive).
   *
   * @return end offset
   * @see <a
   *     href="https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource/struct.FileRange.html#structfield.end">Rust
   *     DataFusion: FileRange::end</a>
   */
  public long end() {
    return end;
  }
}
