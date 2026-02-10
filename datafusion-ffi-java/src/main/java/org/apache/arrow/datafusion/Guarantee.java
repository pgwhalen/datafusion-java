package org.apache.arrow.datafusion;

/** The type of guarantee from a literal guarantee analysis. */
public enum Guarantee {
  /** The column's values must be one of the specified literals. */
  IN,
  /** The column's values must not be any of the specified literals. */
  NOT_IN;
}
