package org.apache.arrow.datafusion;

/** The type of a table in a DataFusion catalog. */
public enum TableType {
  /** A base table (physical storage). */
  BASE,
  /** A view (virtual table defined by a query). */
  VIEW,
  /** A temporary table that exists only for the session. */
  TEMPORARY;
}
