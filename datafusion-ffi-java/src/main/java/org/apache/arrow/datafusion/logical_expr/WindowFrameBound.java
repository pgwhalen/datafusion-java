package org.apache.arrow.datafusion.logical_expr;

import org.apache.arrow.datafusion.common.ScalarValue;

/**
 * A window frame boundary, corresponding to DataFusion's {@code WindowFrameBound}.
 *
 * <p>A null {@code value} in {@link Preceding} or {@link Following} represents an unbounded
 * boundary.
 *
 * <p>Example:
 *
 * <p>{@snippet : WindowFrameBound current = new WindowFrameBound.CurrentRow(); WindowFrameBound
 * unboundedPreceding = new WindowFrameBound.Preceding(null); WindowFrameBound fiveFollowing = new
 * WindowFrameBound.Following(ScalarValue.ofInt64(5L)); }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/window_frame/enum.WindowFrameBound.html">Rust
 *     DataFusion: WindowFrameBound</a>
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
