package org.apache.arrow.datafusion;

/** The emission type of an execution plan, describing how it produces output. */
public enum EmissionType {
  /** The plan produces output incrementally as input arrives. */
  INCREMENTAL,
  /** The plan produces output only after all input has been consumed. */
  FINAL,
  /** The plan may produce output both incrementally and after all input. */
  BOTH;
}
