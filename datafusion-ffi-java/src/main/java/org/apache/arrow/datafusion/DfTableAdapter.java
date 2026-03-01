package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link TableProvider} to the Diplomat-generated {@link DfTableTrait}
 * interface for FFI callbacks.
 */
final class DfTableAdapter implements DfTableTrait {
  private final TableProvider provider;
  private final BufferAllocator allocator;

  // Cached FFI schema - kept alive for the lifetime of this adapter
  private final ArrowSchema ffiSchema;
  private final long schemaAddr;

  DfTableAdapter(TableProvider provider, BufferAllocator allocator) {
    this.provider = provider;
    this.allocator = allocator;
    // Export schema once and cache the FFI address
    this.ffiSchema = ArrowSchema.allocateNew(allocator);
    Data.exportSchema(allocator, provider.schema(), null, ffiSchema);
    this.schemaAddr = ffiSchema.memoryAddress();
  }

  /** Close the cached FFI schema after Rust has imported it (call after createRaw). */
  void closeFfiSchema() {
    ffiSchema.release(); // Free exported schema data (format strings, children, etc.)
    ffiSchema.close(); // Free the 72-byte struct allocation
  }

  @Override
  public long schemaAddress() {
    return schemaAddr;
  }

  @Override
  public int tableType() {
    return switch (provider.tableType()) {
      case BASE -> 0;
      case VIEW -> 1;
      case TEMPORARY -> 2;
    };
  }

  @Override
  public long scan(
      long sessionAddr,
      long filterAddr,
      long filterLen,
      long projectionAddr,
      long projectionLen,
      long limit,
      long errorAddr,
      long errorCap) {
    try {
      // Deserialize filters from protobuf bytes
      List<Expr> filters;
      if (filterAddr == 0 || filterLen == 0) {
        filters = Collections.emptyList();
      } else {
        byte[] filterBytes =
            MemorySegment.ofAddress(filterAddr)
                .reinterpret(filterLen)
                .toArray(ValueLayout.JAVA_BYTE);
        filters = ExprProtoConverter.fromProtoBytes(filterBytes);
      }

      // Read projection indices
      List<Integer> projection;
      if (projectionAddr == 0 || projectionLen == 0) {
        projection = Collections.emptyList();
      } else {
        projection = new ArrayList<>((int) projectionLen);
        MemorySegment projMem =
            MemorySegment.ofAddress(projectionAddr).reinterpret(projectionLen * 8);
        for (int i = 0; i < projectionLen; i++) {
          projection.add((int) projMem.getAtIndex(ValueLayout.JAVA_LONG, i));
        }
      }

      // Convert limit
      Long limitValue = limit >= 0 ? limit : null;

      // Create a Session from the session address - borrowed, valid only during this call
      Session session =
          new Session(new SessionFfi(MemorySegment.ofAddress(sessionAddr)), allocator);

      // Call the provider
      ExecutionPlan plan = provider.scan(session, filters, projection, limitValue);

      // Wrap and return as raw pointer
      DfExecutionPlanAdapter adapter = new DfExecutionPlanAdapter(plan, allocator);
      long ptr = DfExecutionPlan.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      DfCatalogAdapter.writeError(errorAddr, errorCap, e.getMessage());
      return 0;
    }
  }

  @Override
  public int supportsFiltersPushdown(
      long filterAddr,
      long filterLen,
      long resultAddr,
      long resultCap,
      long errorAddr,
      long errorCap) {
    try {
      // Deserialize filters
      List<Expr> filters;
      if (filterAddr == 0 || filterLen == 0) {
        filters = Collections.emptyList();
      } else {
        byte[] filterBytes =
            MemorySegment.ofAddress(filterAddr)
                .reinterpret(filterLen)
                .toArray(ValueLayout.JAVA_BYTE);
        filters = ExprProtoConverter.fromProtoBytes(filterBytes);
      }

      // Call provider
      List<FilterPushDown> results = provider.supportsFiltersPushdown(filters);

      // Write i32 discriminants to result buffer
      int count = (int) Math.min(results.size(), resultCap);
      MemorySegment resultMem = MemorySegment.ofAddress(resultAddr).reinterpret((long) count * 4);
      for (int i = 0; i < count; i++) {
        int disc =
            switch (results.get(i)) {
              case UNSUPPORTED -> 0;
              case INEXACT -> 1;
              case EXACT -> 2;
            };
        resultMem.setAtIndex(ValueLayout.JAVA_INT, i, disc);
      }
      return count;
    } catch (Exception e) {
      DfCatalogAdapter.writeError(errorAddr, errorCap, e.getMessage());
      return -1;
    }
  }
}
