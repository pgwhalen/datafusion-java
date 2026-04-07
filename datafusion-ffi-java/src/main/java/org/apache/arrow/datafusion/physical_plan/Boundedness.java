package org.apache.arrow.datafusion.physical_plan;

/**
 * The boundedness of an execution plan's input data.
 *
 * <p>Example:
 *
 * {@snippet :
 * // Used when implementing a custom ExecutionPlan
 * Boundedness boundedness = Boundedness.BOUNDED;
 * // BOUNDED for finite data, UNBOUNDED for streaming
 * }
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
