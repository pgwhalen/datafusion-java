package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link SchemaProvider} to the Diplomat-generated {@link DfSchemaTrait}
 * interface for FFI callbacks.
 */
final class DfSchemaAdapter implements DfSchemaTrait {
  private final SchemaProvider schema;
  private final BufferAllocator allocator;

  DfSchemaAdapter(SchemaProvider schema, BufferAllocator allocator) {
    this.schema = schema;
    this.allocator = allocator;
  }

  @Override
  public long tableNamesTo(long bufAddr, long bufCap) {
    try {
      List<String> names = schema.tableNames();
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
  public boolean tableExists(long nameAddr, long nameLen) {
    try {
      String name = DfCatalogAdapter.readString(nameAddr, nameLen);
      return schema.tableExists(name);
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public long table(long nameAddr, long nameLen) {
    try {
      String name = DfCatalogAdapter.readString(nameAddr, nameLen);
      Optional<TableProvider> tp = schema.table(name);
      if (tp.isEmpty()) {
        return 0;
      }
      DfTableAdapter adapter = new DfTableAdapter(tp.get(), allocator);
      long ptr = DfTableProvider.createRaw(adapter);
      // Rust has imported the schema during createRaw, safe to release Java-side FFI resources
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      return 0;
    }
  }
}
