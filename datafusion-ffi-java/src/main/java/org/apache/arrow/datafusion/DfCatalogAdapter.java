package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link CatalogProvider} to the Diplomat-generated {@link
 * DfCatalogTrait} interface for FFI callbacks.
 */
final class DfCatalogAdapter implements DfCatalogTrait {
  private final CatalogProvider catalog;
  private final BufferAllocator allocator;

  DfCatalogAdapter(CatalogProvider catalog, BufferAllocator allocator) {
    this.catalog = catalog;
    this.allocator = allocator;
  }

  @Override
  public long schemaNamesTo(long bufAddr, long bufCap) {
    try {
      List<String> names = catalog.schemaNames();
      if (names.isEmpty()) {
        return 0;
      }
      byte[] bytes = String.join("\0", names).getBytes(StandardCharsets.UTF_8);
      int len = (int) Math.min(bytes.length, bufCap);
      MemorySegment.ofAddress(bufAddr).reinterpret(bufCap).copyFrom(MemorySegment.ofArray(bytes));
      return len;
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public long schema(long nameAddr, long nameLen) {
    try {
      String name = readString(nameAddr, nameLen);
      Optional<SchemaProvider> sp = catalog.schema(name);
      if (sp.isEmpty()) {
        return 0;
      }
      return DfSchemaProvider.createRaw(new DfSchemaAdapter(sp.get(), allocator));
    } catch (Exception e) {
      return 0;
    }
  }

  static String readString(long addr, long len) {
    byte[] bytes = MemorySegment.ofAddress(addr).reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  static void writeError(long errorAddr, long errorCap, String message) {
    byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
    int len = (int) Math.min(bytes.length, errorCap);
    MemorySegment.ofAddress(errorAddr)
        .reinterpret(errorCap)
        .copyFrom(MemorySegment.ofArray(bytes).asSlice(0, len));
  }
}
