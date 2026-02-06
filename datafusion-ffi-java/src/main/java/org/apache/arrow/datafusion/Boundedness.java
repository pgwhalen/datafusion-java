package org.apache.arrow.datafusion;

/** The boundedness of an execution plan's input data. */
public enum Boundedness {
  /** The input data is finite (bounded). */
  BOUNDED,
  /** The input data is potentially infinite (unbounded). */
  UNBOUNDED;
}
