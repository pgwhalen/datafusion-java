package org.apache.arrow.datafusion;

/**
 * A source code location within an expression, consisting of a line and column.
 *
 * <p>Example:
 *
 * <p>{@snippet : Location loc = new Location(1, 10); long line = loc.line(); // 1 long column =
 * loc.column(); // 10 }
 *
 * @param line the line number
 * @param column the column number
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Location.html">Rust
 *     DataFusion: Location</a>
 */
public record Location(long line, long column) {

  /**
   * Returns the line number.
   *
   * @return the line number
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Location.html#structfield.line">Rust
   *     DataFusion: Location::line</a>
   */
  public long line() {
    return line;
  }

  /**
   * Returns the column number.
   *
   * @return the column number
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Location.html#structfield.column">Rust
   *     DataFusion: Location::column</a>
   */
  public long column() {
    return column;
  }
}
