package org.apache.arrow.datafusion;

/** The volatility of a scalar UDF, indicating how its result depends on its inputs. */
public enum Volatility {
  /** The function always returns the same result for the same input. */
  IMMUTABLE,
  /** The function returns the same result within a single query but may differ across queries. */
  STABLE,
  /** The function may return different results each time it is called. */
  VOLATILE
}
