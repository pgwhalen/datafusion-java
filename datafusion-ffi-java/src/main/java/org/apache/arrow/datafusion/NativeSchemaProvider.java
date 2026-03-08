package org.apache.arrow.datafusion;

import java.util.List;
import java.util.Optional;

/**
 * A schema provider backed by a native DataFusion schema.
 *
 * <p>Supports introspection (listing tables, checking existence) but not retrieving native {@link
 * TableProvider} instances. Use {@code ctx.table("catalog.schema.table")} to query native tables.
 */
final class NativeSchemaProvider implements SchemaProvider {
  private final SessionContextBridge bridge;
  private final String catalogName;
  private final String schemaName;

  NativeSchemaProvider(SessionContextBridge bridge, String catalogName, String schemaName) {
    this.bridge = bridge;
    this.catalogName = catalogName;
    this.schemaName = schemaName;
  }

  @Override
  public List<String> tableNames() {
    return bridge.catalogTableNames(catalogName, schemaName);
  }

  @Override
  public Optional<TableProvider> table(String name) {
    throw new UnsupportedOperationException(
        "Cannot retrieve native TableProvider. Use ctx.table(\""
            + catalogName
            + "."
            + schemaName
            + "."
            + name
            + "\") instead.");
  }

  @Override
  public boolean tableExists(String name) {
    return bridge.catalogTableExists(catalogName, schemaName, name);
  }
}
