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
