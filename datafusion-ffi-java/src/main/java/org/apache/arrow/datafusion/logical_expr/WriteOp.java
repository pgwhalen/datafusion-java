package org.apache.arrow.datafusion.logical_expr;

/**
 * The type of DML write operation.
 *
 * {@snippet :
 * if (plan instanceof LogicalPlan.Dml dml) {
 *     WriteOp op = dml.op();
 *     System.out.println("DML operation: " + op);
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-expr/52.1.0/datafusion_expr/logical_plan/dml/enum.WriteOp.html">Rust
 *     DataFusion: WriteOp</a>
 */
public enum WriteOp {
  INSERT_APPEND,
  INSERT_OVERWRITE,
  INSERT_REPLACE,
  DELETE,
  UPDATE,
  CTAS
}
