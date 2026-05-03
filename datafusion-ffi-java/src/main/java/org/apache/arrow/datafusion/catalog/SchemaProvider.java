package org.apache.arrow.datafusion.catalog;

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
 * {@snippet :
 * public class MySchemaProvider implements SchemaProvider {
 *     private final Map<String, TableProvider> tables;
 *
 *     @Override
 *     public List<String> tableNames() {
 *         return new ArrayList<>(tables.keySet());
 *     }
 *
 *     @Override
 *     public Optional<TableProvider> table(String name) {
 *         return Optional.ofNullable(tables.get(name));
 *     }
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html">Rust
 *     DataFusion: SchemaProvider</a>
 */
public interface SchemaProvider {
  /**
   * Returns the names of all tables in this schema.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public List<String> tableNames() {
   *     return new ArrayList<>(tables.keySet());
   * }
   * }
   *
   * @return List of table names
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html#method.table_names">Rust
   *     DataFusion: SchemaProvider::table_names</a>
   */
  List<String> tableNames();

  /**
   * Returns the table provider for the given table name.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Optional<TableProvider> table(String name) {
   *     return Optional.ofNullable(tables.get(name));
   * }
   * }
   *
   * @param name The table name
   * @return The table provider, or null if the table doesn't exist
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html#method.table">Rust
   *     DataFusion: SchemaProvider::table</a>
   */
  Optional<TableProvider> table(String name);

  /**
   * If supported by the implementation, adds a new table to this schema.
   *
   * <p>If a table of the same name was already registered, returns the previously registered table.
   *
   * <p>Default implementation throws {@link UnsupportedOperationException}.
   *
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Optional<TableProvider> registerTable(String name, TableProvider table) {
   *     return Optional.ofNullable(tables.put(name, table));
   * }
   * }
   *
   * @param name The table name
   * @param table The table provider to register
   * @return The previously registered table with the same name, if any
   * @throws UnsupportedOperationException if this schema does not support registering tables
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html#method.register_table">Rust
   *     DataFusion: SchemaProvider::register_table</a>
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
   * <p>Example:
   *
   * {@snippet :
   * @Override
   * public Optional<TableProvider> deregisterTable(String name) {
   *     return Optional.ofNullable(tables.remove(name));
   * }
   * }
   *
   * @param name The table name
   * @return The previously registered table, if any
   * @throws UnsupportedOperationException if this schema does not support deregistering tables
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html#method.deregister_table">Rust
   *     DataFusion: SchemaProvider::deregister_table</a>
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
   * <p>Example:
   *
   * {@snippet :
   * boolean exists = schemaProvider.tableExists("my_table");
   * }
   *
   * @param name The table name
   * @return true if the table exists
   * @see <a
   *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.SchemaProvider.html#method.table_exist">Rust
   *     DataFusion: SchemaProvider::table_exist</a>
   */
  default boolean tableExists(String name) {
    return table(name).isPresent();
  }
}
