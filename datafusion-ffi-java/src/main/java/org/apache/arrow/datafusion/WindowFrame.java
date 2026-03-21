package org.apache.arrow.datafusion;

/**
 * A window frame definition, corresponding to DataFusion's {@code WindowFrame}.
 *
 * @param frameType the type of frame (ROWS, RANGE, or GROUPS)
 * @param startBound the start boundary of the window frame
 * @param endBound the end boundary of the window frame
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/window_frame/struct.WindowFrame.html">Rust
 *     DataFusion: WindowFrame</a>
 */
public record WindowFrame(
    WindowFrameUnits frameType, WindowFrameBound startBound, WindowFrameBound endBound) {

  /**
   * Returns the type of frame (ROWS, RANGE, or GROUPS).
   *
   * @return the frame type
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/window_frame/struct.WindowFrame.html#structfield.units">Rust
   *     DataFusion: WindowFrame::units</a>
   */
  public WindowFrameUnits frameType() {
    return frameType;
  }

  /**
   * Returns the start boundary of the window frame.
   *
   * @return the start bound
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/window_frame/struct.WindowFrame.html#structfield.start_bound">Rust
   *     DataFusion: WindowFrame::start_bound</a>
   */
  public WindowFrameBound startBound() {
    return startBound;
  }

  /**
   * Returns the end boundary of the window frame.
   *
   * @return the end bound
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/window_frame/struct.WindowFrame.html#structfield.end_bound">Rust
   *     DataFusion: WindowFrame::end_bound</a>
   */
  public WindowFrameBound endBound() {
    return endBound;
  }
}
