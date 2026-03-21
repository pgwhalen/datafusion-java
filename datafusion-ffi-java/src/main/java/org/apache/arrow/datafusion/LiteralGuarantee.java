package org.apache.arrow.datafusion;

import java.util.List;
import java.util.Set;

/**
 * A guarantee about the values of a column derived from analyzing filter expressions.
 *
 * <p>For example, the filter {@code WHERE col IN (1, 2, 3)} produces a guarantee that {@code col}
 * must be one of {@code {1, 2, 3}} (an {@link Guarantee#IN} guarantee).
 *
 * @param column the column this guarantee applies to
 * @param guarantee the type of guarantee (IN or NOT_IN)
 * @param literals the set of literal values in the guarantee
 * @see <a
 *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/struct.LiteralGuarantee.html">Rust
 *     DataFusion: LiteralGuarantee</a>
 */
public record LiteralGuarantee(Column column, Guarantee guarantee, Set<ScalarValue> literals) {

  public LiteralGuarantee {
    literals = Set.copyOf(literals);
  }

  /**
   * Returns the column this guarantee applies to.
   *
   * @return the column
   * @see <a
   *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/struct.LiteralGuarantee.html#structfield.column">Rust
   *     DataFusion: LiteralGuarantee::column</a>
   */
  public Column column() {
    return column;
  }

  /**
   * Returns the type of guarantee (IN or NOT_IN).
   *
   * @return the guarantee type
   * @see <a
   *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/struct.LiteralGuarantee.html#structfield.guarantee">Rust
   *     DataFusion: LiteralGuarantee::guarantee</a>
   */
  public Guarantee guarantee() {
    return guarantee;
  }

  /**
   * Returns the set of literal values in the guarantee.
   *
   * @return the literal values
   * @see <a
   *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/struct.LiteralGuarantee.html#structfield.literals">Rust
   *     DataFusion: LiteralGuarantee::literals</a>
   */
  public Set<ScalarValue> literals() {
    return literals;
  }

  /**
   * Analyzes a physical expression to extract literal guarantees.
   *
   * <p>This mirrors DataFusion's {@code LiteralGuarantee::analyze()} function.
   *
   * @param expr the physical expression to analyze
   * @return the list of literal guarantees extracted from the expression
   * @throws DataFusionError if analysis fails
   * @see <a
   *     href="https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr/utils/struct.LiteralGuarantee.html#method.analyze">Rust
   *     DataFusion: LiteralGuarantee::analyze</a>
   */
  public static List<LiteralGuarantee> analyze(PhysicalExpr expr) {
    return LiteralGuaranteeBridge.analyze(expr.bridge());
  }
}
