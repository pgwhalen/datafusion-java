package org.apache.arrow.datafusion.logical_expr;

/**
 * Transaction conclusion type (COMMIT or ROLLBACK).
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Statement.TransactionEnd tx) {
 *     TransactionConclusion conclusion = tx.conclusion();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/53.1.0/datafusion_expr/logical_plan/statement/enum.TransactionConclusion.html">Rust
 *     DataFusion: TransactionConclusion</a>
 */
public enum TransactionConclusion {
  COMMIT,
  ROLLBACK
}
