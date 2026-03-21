package org.apache.arrow.datafusion;

import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link CatalogProvider} to the Diplomat-generated {@link
 * DfCatalogTrait} interface for FFI callbacks.
 */
public final class DfCatalogAdapter implements DfCatalogTrait {
  private final CatalogProvider catalog;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  public DfCatalogAdapter(
      CatalogProvider catalog, BufferAllocator allocator, boolean fullStackTrace) {
    this.catalog = catalog;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long schemaNamesTo(long bufAddr, long bufCap) {
    try {
      List<String> names = catalog.schemaNames();
      if (names.isEmpty()) {
        return 0;
      }
      return NativeUtil.writeStrings(bufAddr, bufCap, names);
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public long schema(long nameAddr, long nameLen) {
    try {
      Optional<SchemaProvider> sp = catalog.schema(NativeUtil.readString(nameAddr, nameLen));
      if (sp.isEmpty()) {
        return 0;
      }
      return DfSchemaProvider.createRaw(new DfSchemaAdapter(sp.get(), allocator, fullStackTrace));
    } catch (Exception e) {
      return 0;
    }
  }
}
