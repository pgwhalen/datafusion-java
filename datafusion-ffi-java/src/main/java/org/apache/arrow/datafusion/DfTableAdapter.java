package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.generated.DfExecutionPlan;
import org.apache.arrow.datafusion.generated.DfTableTrait;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link TableProvider} to the Diplomat-generated {@link DfTableTrait}
 * interface for FFI callbacks.
 */
public final class DfTableAdapter implements DfTableTrait {
  private final TableProvider provider;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  // Cached FFI schema - kept alive for the lifetime of this adapter
  private final ArrowSchema ffiSchema;
  private final long schemaAddr;

  public DfTableAdapter(TableProvider provider, BufferAllocator allocator, boolean fullStackTrace) {
    this.provider = provider;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
    // Export schema once and cache the FFI address
    this.ffiSchema = ArrowSchema.allocateNew(allocator);
    try {
      Data.exportSchema(allocator, provider.schema(), null, ffiSchema);
    } catch (Exception e) {
      ffiSchema.close();
      throw e;
    }
    this.schemaAddr = ffiSchema.memoryAddress();
  }

  /** Close the cached FFI schema after Rust has imported it (call after createRaw). */
  public void closeFfiSchema() {
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
      long filtersAddr,
      long filtersLen,
      long projectionAddr,
      long projectionLen,
      long limit,
      long errorAddr,
      long errorCap) {
    try {
      // Deserialize filters from raw byte buffer
      byte[] filterBytes = NativeUtil.readBytes(filtersAddr, filtersLen);
      List<Expr> filterExprs =
          filterBytes.length == 0
              ? Collections.emptyList()
              : ExprProtoConverter.fromProtoBytes(filterBytes);

      // Convert projection from raw u32 buffer.
      // projectionAddr == 0 means Rust None (no projection constraint, all columns) -> null.
      // projectionAddr != 0 with projectionLen == 0 means Rust Some([]) (zero columns
      // explicitly requested, e.g. for count(*)) -> empty list.
      // projectionAddr != 0 with projectionLen > 0 means Rust Some([...]) -> list of indices.
      List<Integer> projectionList;
      if (projectionAddr == 0) {
        projectionList = null;
      } else if (projectionLen == 0) {
        projectionList = Collections.emptyList();
      } else {
        int[] projectionArr = NativeUtil.readU32s(projectionAddr, projectionLen);
        projectionList = new ArrayList<>(projectionArr.length);
        for (int idx : projectionArr) {
          projectionList.add(idx);
        }
      }

      // Convert limit
      Long limitValue = limit >= 0 ? limit : null;

      // Create a Session - the session address from Rust is unused since Session creates
      // its own default SessionState for physical expression compilation
      Session session = new Session(allocator);

      // Call the provider
      ExecutionPlan plan = provider.scan(session, filterExprs, projectionList, limitValue);

      // Wrap and return as raw pointer
      DfExecutionPlanAdapter adapter = new DfExecutionPlanAdapter(plan, allocator, fullStackTrace);
      long ptr = DfExecutionPlan.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }

  @Override
  public int supportsFiltersPushdown(
      long filtersAddr,
      long filtersLen,
      long resultAddr,
      long resultCap,
      long errorAddr,
      long errorCap) {
    try {
      // Deserialize filters from raw byte buffer
      byte[] filterBytes = NativeUtil.readBytes(filtersAddr, filtersLen);
      List<Expr> filterExprs =
          filterBytes.length == 0
              ? Collections.emptyList()
              : ExprProtoConverter.fromProtoBytes(filterBytes);

      // Call provider
      List<TableProviderFilterPushDown> results = provider.supportsFiltersPushdown(filterExprs);

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
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return -1;
    }
  }
}
