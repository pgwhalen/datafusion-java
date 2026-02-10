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
 */
public record LiteralGuarantee(Column column, Guarantee guarantee, Set<ScalarValue> literals) {

  public LiteralGuarantee {
    literals = Set.copyOf(literals);
  }

  /**
   * Analyzes a physical expression to extract literal guarantees.
   *
   * <p>This mirrors DataFusion's {@code LiteralGuarantee::analyze()} function.
   *
   * @param expr the physical expression to analyze
   * @return the list of literal guarantees extracted from the expression
   * @throws DataFusionException if analysis fails
   */
  public static List<LiteralGuarantee> analyze(PhysicalExpr expr) {
    return LiteralGuaranteeFfi.analyze(expr);
  }
}
