package org.apache.arrow.datafusion;

/**
 * A reference to a table, which may be unqualified (bare), partially qualified (schema + table), or
 * fully qualified (catalog + schema + table).
 */
public sealed interface TableReference {

  /** An unqualified table reference consisting of only a table name. */
  record Bare(String table) implements TableReference {}

  /** A partially qualified table reference consisting of a schema and table name. */
  record Partial(String schema, String table) implements TableReference {}

  /** A fully qualified table reference consisting of a catalog, schema, and table name. */
  record Full(String catalog, String schema, String table) implements TableReference {}
}
