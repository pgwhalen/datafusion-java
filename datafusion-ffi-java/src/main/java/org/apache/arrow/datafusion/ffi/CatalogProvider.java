package org.apache.arrow.datafusion.ffi;

import java.util.List;

/**
 * A provider of schemas within a catalog.
 *
 * <p>In DataFusion's catalog hierarchy, a CatalogProvider represents a catalog (database) that
 * contains schemas. Tables are referenced in SQL as {@code catalog.schema.table}.
 *
 * <p>Example implementation:
 *
 * <pre>{@code
 * public class MyCatalogProvider implements CatalogProvider {
 *     private final Map<String, SchemaProvider> schemas;
 *
 *     @Override
 *     public List<String> schemaNames() {
 *         return new ArrayList<>(schemas.keySet());
 *     }
 *
 *     @Override
 *     public SchemaProvider schema(String name) {
 *         return schemas.get(name);
 *     }
 * }
 * }</pre>
 */
public interface CatalogProvider {
  /**
   * Returns the names of all schemas in this catalog.
   *
   * @return List of schema names
   */
  List<String> schemaNames();

  /**
   * Returns the schema provider for the given schema name.
   *
   * @param name The schema name
   * @return The schema provider, or null if the schema doesn't exist
   */
  SchemaProvider schema(String name);
}
