package org.apache.arrow.datafusion.logical_expr;

/**
 * Transaction access mode (READ ONLY or READ WRITE).
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Statement.TransactionStart tx) {
 *     TransactionAccessMode mode = tx.accessMode();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/statement/enum.TransactionAccessMode.html">Rust
 *     DataFusion: TransactionAccessMode</a>
 */
public enum TransactionAccessMode {
  READ_ONLY,
  READ_WRITE
}
