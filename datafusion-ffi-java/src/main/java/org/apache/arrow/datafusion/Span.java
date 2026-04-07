package org.apache.arrow.datafusion;

/**
 * A span in source code, defined by a start and end location.
 *
 * <p>Example:
 *
 * {@snippet :
 * Location start = new Location(1, 5);
 * Location end = new Location(1, 20);
 * Span span = new Span(start, end);
 * }
 *
 * @param start the start location
 * @param end the end location
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Span.html">Rust
 *     DataFusion: Span</a>
 */
public record Span(Location start, Location end) {

  /**
   * Returns the start location of this span.
   *
   * @return the start location
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Span.html#structfield.start">Rust
   *     DataFusion: Span::start</a>
   */
  public Location start() {
    return start;
  }

  /**
   * Returns the end location of this span.
   *
   * @return the end location
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Span.html#structfield.end">Rust
   *     DataFusion: Span::end</a>
   */
  public Location end() {
    return end;
  }
}
