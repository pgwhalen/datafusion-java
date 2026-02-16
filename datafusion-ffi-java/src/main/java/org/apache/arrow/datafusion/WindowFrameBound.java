package org.apache.arrow.datafusion;

/**
 * A window frame boundary, corresponding to DataFusion's {@code WindowFrameBound}.
 *
 * <p>A null {@code value} in {@link Preceding} or {@link Following} represents an unbounded
 * boundary.
 */
public sealed interface WindowFrameBound {

  /** The current row boundary. */
  record CurrentRow() implements WindowFrameBound {}

  /**
   * A preceding boundary. A null value means unbounded preceding.
   *
   * @param value the offset value, or null for unbounded
   */
  record Preceding(ScalarValue value) implements WindowFrameBound {}

  /**
   * A following boundary. A null value means unbounded following.
   *
   * @param value the offset value, or null for unbounded
   */
  record Following(ScalarValue value) implements WindowFrameBound {}
}
