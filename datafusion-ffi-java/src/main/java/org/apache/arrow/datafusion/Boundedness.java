package org.apache.arrow.datafusion;

/**
 * The boundedness of an execution plan's input data.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/physical_plan/execution_plan/enum.Boundedness.html">Rust
 *     DataFusion: Boundedness</a>
 */
public enum Boundedness {
  /** The input data is finite (bounded). */
  BOUNDED,
  /** The input data is potentially infinite (unbounded). */
  UNBOUNDED;
}
