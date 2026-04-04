package org.apache.arrow.datafusion;

import java.util.Optional;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.generated.DfCatalogTrait;
import org.apache.arrow.datafusion.generated.DfSchemaProvider;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link CatalogProvider} to the Diplomat-generated {@link
 * DfCatalogTrait} interface for FFI callbacks.
 */
public final class DfCatalogAdapter implements DfCatalogTrait {
  private final CatalogProvider catalog;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  /**
   * Creates a new adapter wrapping the given catalog provider.
   *
   * @param catalog the catalog provider to adapt
   * @param allocator the buffer allocator
   * @param fullStackTrace whether to include full stack traces in errors
   */
  public DfCatalogAdapter(
      CatalogProvider catalog, BufferAllocator allocator, boolean fullStackTrace) {
    this.catalog = catalog;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long schemaNamesRaw() {
    try {
      return NativeUtil.toRawStringArray(catalog.schemaNames());
    } catch (Exception e) {
      return 0;
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
