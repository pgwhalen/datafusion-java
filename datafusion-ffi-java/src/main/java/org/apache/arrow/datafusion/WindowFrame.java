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
    WindowFrameUnits frameType, WindowFrameBound startBound, WindowFrameBound endBound) {}
