package org.apache.arrow.datafusion;

import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link SchemaProvider} to the Diplomat-generated {@link DfSchemaTrait}
 * interface for FFI callbacks.
 */
final class DfSchemaAdapter implements DfSchemaTrait {
  private final SchemaProvider schema;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  DfSchemaAdapter(SchemaProvider schema, BufferAllocator allocator, boolean fullStackTrace) {
    this.schema = schema;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long tableNamesTo(long bufAddr, long bufCap) {
    try {
      List<String> names = schema.tableNames();
      if (names.isEmpty()) {
        return 0;
      }
      return NativeUtil.writeStrings(bufAddr, bufCap, names);
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public boolean tableExists(long nameAddr, long nameLen) {
    try {
      return schema.tableExists(NativeUtil.readString(nameAddr, nameLen));
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public long table(long nameAddr, long nameLen, long errorAddr, long errorCap) {
    try {
      Optional<TableProvider> tp = schema.table(NativeUtil.readString(nameAddr, nameLen));
      if (tp.isEmpty()) {
        return 0;
      }
      DfTableAdapter adapter = new DfTableAdapter(tp.get(), allocator, fullStackTrace);
      long ptr = DfTableProvider.createRaw(adapter);
      // Rust has imported the schema during createRaw, safe to release Java-side FFI resources
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
