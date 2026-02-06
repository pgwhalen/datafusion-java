package org.apache.arrow.datafusion;

import java.util.List;
import java.util.Optional;

/**
 * A provider of tables within a catalog schema.
 *
 * <p>In DataFusion's catalog hierarchy, a SchemaProvider represents a database schema (namespace)
 * that contains tables. Tables are referenced in SQL as {@code catalog.schema.table}.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * public class MySchemaProvider implements SchemaProvider {
 *     private final Map<String, TableProvider> tables;
 *
 *     @Override
 *     public List<String> tableNames() {
 *         return new ArrayList<>(tables.keySet());
 *     }
 *
 *     @Override
 *     public TableProvider table(String name) {
 *         return tables.get(name);
 *     }
 * }
 * }</pre>
 */
public interface SchemaProvider {
  /**
   * Returns the names of all tables in this schema.
   *
   * @return List of table names
   */
  List<String> tableNames();

  /**
   * Returns the table provider for the given table name.
   *
   * @param name The table name
   * @return The table provider, or null if the table doesn't exist
   */
  Optional<TableProvider> table(String name);

  /**
   * If supported by the implementation, adds a new table to this schema.
   *
   * <p>If a table of the same name was already registered, returns the previously registered table.
   *
   * <p>Default implementation throws {@link UnsupportedOperationException}.
   *
   * @param name The table name
   * @param table The table provider to register
   * @return The previously registered table with the same name, if any
   * @throws UnsupportedOperationException if this schema does not support registering tables
   */
  default Optional<TableProvider> registerTable(String name, TableProvider table) {
    throw new UnsupportedOperationException("schema provider does not support registering tables");
  }

  /**
   * If supported by the implementation, removes the named table from this schema and returns the
   * previously registered {@link TableProvider}, if any.
   *
   * <p>If no table with the given name exists, returns empty.
   *
   * <p>Default implementation throws {@link UnsupportedOperationException}.
   *
   * @param name The table name
   * @return The previously registered table, if any
   * @throws UnsupportedOperationException if this schema does not support deregistering tables
   */
  default Optional<TableProvider> deregisterTable(String name) {
    throw new UnsupportedOperationException(
        "schema provider does not support deregistering tables");
  }

  /**
   * Checks if a table exists in this schema.
   *
   * <p>Default implementation checks if {@link #table(String)} returns non-null.
   *
   * @param name The table name
   * @return true if the table exists
   */
  default boolean tableExists(String name) {
    return table(name).isPresent();
  }
}
