package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.vector.FieldVector;

/**
 * Tracks aggregation state for a single group during aggregate computation.
 *
 * <p>An accumulator is created per group by {@link AggregateUDF#createAccumulator()}. DataFusion
 * calls {@link #updateBatch} with input data, then {@link #evaluate} for the final result.
 *
 * <p>For distributed/partitioned execution, DataFusion uses {@link #state} and {@link #mergeBatch}
 * to combine partial results from multiple partitions.
 *
 * <p>Example:
 *
 * {@snippet :
 * class SumAccumulator implements Accumulator {
 *     private long sum = 0;
 *
 *     public void updateBatch(List<FieldVector> values) {
 *         BigIntVector col = (BigIntVector) values.get(0);
 *         for (int i = 0; i < col.getValueCount(); i++) {
 *             if (!col.isNull(i)) sum += col.get(i);
 *         }
 *     }
 *
 *     public ScalarValue evaluate() { return new ScalarValue.Int64(sum); }
 *     public List<ScalarValue> state() { return List.of(new ScalarValue.Int64(sum)); }
 *
 *     public void mergeBatch(List<FieldVector> states) {
 *         BigIntVector partialSums = (BigIntVector) states.get(0);
 *         for (int i = 0; i < partialSums.getValueCount(); i++) {
 *             if (!partialSums.isNull(i)) sum += partialSums.get(i);
 *         }
 *     }
 *
 *     public long size() { return Long.BYTES; }
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html">Rust
 *     DataFusion: Accumulator</a>
 */
public interface Accumulator {

  /**
   * Updates the accumulator state with a batch of input values.
   *
   * <p>Each element in {@code values} is a column vector for one argument of the aggregate
   * function. For a single-argument aggregate like SUM(x), {@code values} has one element.
   *
   * <p>See {@link Accumulator} for an example.
   *
   * @param values the input column vectors, one per argument
   * @see <a
   *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html#tymethod.update_batch">Rust
   *     DataFusion: Accumulator::update_batch</a>
   */
  void updateBatch(List<FieldVector> values);

  /**
   * Returns the final aggregate result as a scalar value.
   *
   * <p>See {@link Accumulator} for an example.
   *
   * @return the final aggregate result
   * @see <a
   *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html#tymethod.evaluate">Rust
   *     DataFusion: Accumulator::evaluate</a>
   */
  ScalarValue evaluate();

  /**
   * Returns the intermediate accumulation state as scalar values.
   *
   * <p>Used for distributed execution: partial accumulators serialize their state via this method,
   * then another accumulator merges them via {@link #mergeBatch}. The number and types of values
   * must match {@link AggregateUDF#stateFields}.
   *
   * <p>See {@link Accumulator} for an example.
   *
   * @return the intermediate state values
   * @see <a
   *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html#tymethod.state">Rust
   *     DataFusion: Accumulator::state</a>
   */
  List<ScalarValue> state();

  /**
   * Merges intermediate state from other accumulators into this one.
   *
   * <p>Each element in {@code states} is a column vector containing partial state values from
   * multiple accumulators. The vectors correspond to the fields returned by {@link
   * AggregateUDF#stateFields}.
   *
   * <p>See {@link Accumulator} for an example.
   *
   * @param states the partial state column vectors to merge
   * @see <a
   *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html#tymethod.merge_batch">Rust
   *     DataFusion: Accumulator::merge_batch</a>
   */
  void mergeBatch(List<FieldVector> states);

  /**
   * Returns the estimated memory size of this accumulator in bytes.
   *
   * <p>Used by DataFusion for memory management. Include the size of all internal state.
   *
   * <p>See {@link Accumulator} for an example.
   *
   * @return the estimated memory size in bytes
   * @see <a
   *     href="https://docs.rs/datafusion-expr-common/52.1.0/datafusion_expr_common/accumulator/trait.Accumulator.html#tymethod.size">Rust
   *     DataFusion: Accumulator::size</a>
   */
  long size();
}
