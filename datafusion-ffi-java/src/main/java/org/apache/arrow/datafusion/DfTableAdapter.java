package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.catalog.ScanArgs;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.generated.DfExecutionPlan;
import org.apache.arrow.datafusion.generated.DfTableTrait;
import org.apache.arrow.datafusion.logical_expr.ColumnAssignment;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.InsertOp;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.NativeSendableRecordBatchStream;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStreamBridge;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link TableProvider} to the Diplomat-generated {@link DfTableTrait}
 * interface for FFI callbacks.
 */
public final class DfTableAdapter implements DfTableTrait {
  // Native method handles for DfLazyRecordBatchStream operations (used in insert_into callback).
  // We resolve these directly rather than going through the generated class whose constructor
  // is package-private to the generated package.
  private static final StructLayout LAZY_STREAM_RESULT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("union_val"),
          ValueLayout.JAVA_BOOLEAN.withName("is_ok"),
          MemoryLayout.paddingLayout(7));

  private static final MethodHandle LAZY_STREAM_DESTROY;
  private static final MethodHandle LAZY_STREAM_SCHEMA_TO;
  private static final MethodHandle LAZY_STREAM_NEXT;

  static {
    var lib = NativeLoader.get();
    var linker = Linker.nativeLinker();
    LAZY_STREAM_DESTROY =
        linker.downcallHandle(
            lib.find("datafusion_DfLazyRecordBatchStream_destroy").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    LAZY_STREAM_SCHEMA_TO =
        linker.downcallHandle(
            lib.find("datafusion_DfLazyRecordBatchStream_schema_to").orElseThrow(),
            FunctionDescriptor.of(LAZY_STREAM_RESULT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    LAZY_STREAM_NEXT =
        linker.downcallHandle(
            lib.find("datafusion_DfLazyRecordBatchStream_next").orElseThrow(),
            FunctionDescriptor.of(
                LAZY_STREAM_RESULT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG));
  }

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
      ScanArgs args = new ScanArgs(filterExprs, projectionList, limitValue);
      ExecutionPlan plan = provider.scanWithArgs(session, args);

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

  @Override
  public long insertInto(
      long sessionAddr, long inputStreamPtr, int insertOp, long errorAddr, long errorCap) {
    MemorySegment streamHandle = MemorySegment.ofAddress(inputStreamPtr);
    boolean streamClosed = false;
    try {
      // Wrap the raw DfLazyRecordBatchStream pointer as a NativeSendableRecordBatchStream
      // using native method handles directly (the generated class constructor is package-private)
      NativeSendableRecordBatchStream adapter =
          new NativeSendableRecordBatchStream() {
            public int next(long arrayOutAddr, long schemaOutAddr) {
              try (var arena = Arena.ofConfined()) {
                var result =
                    (MemorySegment)
                        LAZY_STREAM_NEXT.invokeExact(
                            (SegmentAllocator) arena, streamHandle, arrayOutAddr, schemaOutAddr);
                boolean isOk = result.get(ValueLayout.JAVA_BOOLEAN, 8L);
                if (isOk) {
                  return result.get(ValueLayout.JAVA_INT, 0L);
                } else {
                  throw new RuntimeException("Lazy stream next() failed");
                }
              } catch (RuntimeException ex) {
                throw ex;
              } catch (Throwable ex) {
                throw new RuntimeException(ex);
              }
            }

            public void schemaTo(long outAddr) {
              try (var arena = Arena.ofConfined()) {
                var result =
                    (MemorySegment)
                        LAZY_STREAM_SCHEMA_TO.invokeExact(
                            (SegmentAllocator) arena, streamHandle, outAddr);
                boolean isOk = result.get(ValueLayout.JAVA_BOOLEAN, 8L);
                if (!isOk) {
                  throw new RuntimeException("Lazy stream schemaTo() failed");
                }
              } catch (RuntimeException ex) {
                throw ex;
              } catch (Throwable ex) {
                throw new RuntimeException(ex);
              }
            }

            public void close() {
              try {
                LAZY_STREAM_DESTROY.invokeExact(streamHandle);
              } catch (Throwable ex) {
                throw new RuntimeException(ex);
              }
            }
          };
      SendableRecordBatchStreamBridge bridge =
          new SendableRecordBatchStreamBridge(adapter, allocator);
      SendableRecordBatchStream stream = new SendableRecordBatchStream(bridge);

      // Convert insert op discriminant
      InsertOp op =
          switch (insertOp) {
            case 1 -> InsertOp.OVERWRITE;
            case 2 -> InsertOp.REPLACE;
            default -> InsertOp.APPEND;
          };

      Session session = new Session(allocator);
      ExecutionPlan plan = provider.insertInto(session, stream, op);

      // Wrap and return as raw pointer
      DfExecutionPlanAdapter planAdapter =
          new DfExecutionPlanAdapter(plan, allocator, fullStackTrace);
      long ptr = DfExecutionPlan.createRaw(planAdapter);
      planAdapter.closeFfiSchema();

      // Close the stream (drops the Rust lazy stream)
      stream.close();
      streamClosed = true;

      return ptr;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    } finally {
      if (!streamClosed) {
        try {
          LAZY_STREAM_DESTROY.invokeExact(streamHandle);
        } catch (Throwable ignored) {
        }
      }
    }
  }

  @Override
  public long deleteFrom(
      long sessionAddr, long filtersAddr, long filtersLen, long errorAddr, long errorCap) {
    try {
      // Deserialize filters from raw byte buffer
      byte[] filterBytes = NativeUtil.readBytes(filtersAddr, filtersLen);
      List<Expr> filterExprs =
          filterBytes.length == 0
              ? Collections.emptyList()
              : ExprProtoConverter.fromProtoBytes(filterBytes);

      Session session = new Session(allocator);
      ExecutionPlan plan = provider.deleteFrom(session, filterExprs);

      // Wrap and return as raw pointer
      DfExecutionPlanAdapter adapter = new DfExecutionPlanAdapter(plan, allocator, fullStackTrace);
      long ptr = DfExecutionPlan.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (UnsupportedOperationException e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }

  @Override
  public long update(
      long sessionAddr,
      long colNamesPtr,
      long assignExprsAddr,
      long assignExprsLen,
      long filtersAddr,
      long filtersLen,
      long errorAddr,
      long errorCap) {
    try {
      // Read column names from DfStringArray raw pointer (takes ownership and destroys)
      List<String> colNamesList = NativeUtil.fromRawStringArray(colNamesPtr);

      // Deserialize assignment expressions from protobuf bytes
      byte[] assignBytes = NativeUtil.readBytes(assignExprsAddr, assignExprsLen);
      List<Expr> assignExprs =
          assignBytes.length == 0
              ? Collections.emptyList()
              : ExprProtoConverter.fromProtoBytes(assignBytes);

      // Deserialize filter expressions from protobuf bytes
      byte[] filterBytes = NativeUtil.readBytes(filtersAddr, filtersLen);
      List<Expr> filterExprs =
          filterBytes.length == 0
              ? Collections.emptyList()
              : ExprProtoConverter.fromProtoBytes(filterBytes);

      // Pair column names with expressions into ColumnAssignment list
      List<ColumnAssignment> assignments = new ArrayList<>(colNamesList.size());
      for (int i = 0; i < colNamesList.size(); i++) {
        assignments.add(new ColumnAssignment(colNamesList.get(i), assignExprs.get(i)));
      }

      Session session = new Session(allocator);
      ExecutionPlan plan = provider.update(session, assignments, filterExprs);

      // Wrap and return as raw pointer
      DfExecutionPlanAdapter adapter = new DfExecutionPlanAdapter(plan, allocator, fullStackTrace);
      long ptr = DfExecutionPlan.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (UnsupportedOperationException e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
