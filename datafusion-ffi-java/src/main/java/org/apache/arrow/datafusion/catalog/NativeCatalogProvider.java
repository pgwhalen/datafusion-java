package org.apache.arrow.datafusion.catalog;

import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.SessionContextBridge;

/**
 * A catalog provider backed by a native DataFusion catalog.
 *
 * <p>This is used to introspect catalogs registered in the session context. It does not support
 * registering Java-implemented schemas — use {@link SessionContext#registerCatalog} for that.
 */
public final class NativeCatalogProvider implements CatalogProvider {
  private final SessionContextBridge bridge;
  private final String catalogName;

  public NativeCatalogProvider(SessionContextBridge bridge, String catalogName) {
    this.bridge = bridge;
    this.catalogName = catalogName;
  }

  @Override
  public List<String> schemaNames() {
    return bridge.catalogSchemaNames(catalogName);
  }

  @Override
  public Optional<SchemaProvider> schema(String name) {
    List<String> schemas = schemaNames();
    if (schemas.contains(name)) {
      return Optional.of(new NativeSchemaProvider(bridge, catalogName, name));
    }
    return Optional.empty();
  }
}
