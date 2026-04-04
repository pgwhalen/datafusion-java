package org.apache.arrow.datafusion;

import java.util.Optional;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.generated.DfSchemaTrait;
import org.apache.arrow.datafusion.generated.DfTableProvider;
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
  public long tableNamesRaw() {
    try {
      return NativeUtil.toRawStringArray(schema.tableNames());
    } catch (Exception e) {
      return 0;
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
