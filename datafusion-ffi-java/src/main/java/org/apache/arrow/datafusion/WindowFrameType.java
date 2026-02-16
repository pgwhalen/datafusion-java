package org.apache.arrow.datafusion;

/** The type of a window frame, corresponding to DataFusion's {@code WindowFrameUnits}. */
public enum WindowFrameType {
  ROWS,
  RANGE,
  GROUPS
}
