package org.apache.arrow.datafusion;

/** Insert operation mode for DataFrame write operations. */
public enum InsertOp {
  APPEND,
  OVERWRITE,
  REPLACE
}
