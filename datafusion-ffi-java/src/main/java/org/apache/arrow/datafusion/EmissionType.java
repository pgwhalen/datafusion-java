package org.apache.arrow.datafusion;

/**
 * The emission type of an execution plan, describing how it produces output.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/execution_plan/enum.EmissionType.html">Rust
 *     DataFusion: EmissionType</a>
 */
public enum EmissionType {
  /** The plan produces output incrementally as input arrives. */
  INCREMENTAL,
  /** The plan produces output only after all input has been consumed. */
  FINAL,
  /** The plan may produce output both incrementally and after all input. */
  BOTH;
}
