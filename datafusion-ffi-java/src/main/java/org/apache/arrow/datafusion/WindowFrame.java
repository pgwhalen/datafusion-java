package org.apache.arrow.datafusion;

/**
 * A window frame definition, corresponding to DataFusion's {@code WindowFrame}.
 *
 * @param frameType the type of frame (ROWS, RANGE, or GROUPS)
 * @param startBound the start boundary of the window frame
 * @param endBound the end boundary of the window frame
 */
public record WindowFrame(
    WindowFrameType frameType, WindowFrameBound startBound, WindowFrameBound endBound) {}
