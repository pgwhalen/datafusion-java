package org.apache.arrow.datafusion;

/**
 * How nulls are treated in aggregate and window functions, corresponding to DataFusion's {@code
 * NullTreatment}.
 */
public enum NullTreatment {
  RESPECT_NULLS,
  IGNORE_NULLS
}
