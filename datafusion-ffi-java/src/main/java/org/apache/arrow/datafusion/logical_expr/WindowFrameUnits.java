package org.apache.arrow.datafusion.logical_expr;

/**
 * The type of a window frame, corresponding to DataFusion's {@code WindowFrameUnits}.
 *
 * <p>Example:
 *
 * {@snippet :
 * WindowFrame rowFrame = new WindowFrame(
 *     WindowFrameUnits.ROWS,
 *     new WindowFrameBound.Preceding(null),
 *     new WindowFrameBound.CurrentRow()
 * );
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/logical_expr/window_frame/enum.WindowFrameUnits.html">Rust
 *     DataFusion: WindowFrameUnits</a>
 */
public enum WindowFrameUnits {
  ROWS,
  RANGE,
  GROUPS
}
