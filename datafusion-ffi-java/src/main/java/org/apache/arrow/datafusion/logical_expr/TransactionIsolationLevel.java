package org.apache.arrow.datafusion.logical_expr;

/**
 * Transaction isolation level.
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Statement.TransactionStart tx) {
 *     TransactionIsolationLevel level = tx.isolationLevel();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/statement/enum.TransactionIsolationLevel.html">Rust
 *     DataFusion: TransactionIsolationLevel</a>
 */
public enum TransactionIsolationLevel {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE,
  SNAPSHOT
}
