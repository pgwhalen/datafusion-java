package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.proto.SortExprNodeCollection;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public DataFrame API and Diplomat-generated DfDataFrame.
 *
 * <p>This replaces DataFrameFfi, delegating to the Diplomat-generated class for all native calls.
 */
final class DataFrameBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrameBridge.class);

  private final DfDataFrame dfDf;
  private volatile boolean closed = false;

  DataFrameBridge(DfDataFrame dfDf) {
    this.dfDf = dfDf;
  }

  RecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();
    try {
      DfLazyRecordBatchStream lazyStream = dfDf.executeStream();
      NativeRecordBatchStream adapter =
          new NativeRecordBatchStream() {
            public int next(long a, long s) {
              return lazyStream.next(a, s);
            }

            public void schemaTo(long s) {
              lazyStream.schemaTo(s);
            }

            public void close() {
              lazyStream.close();
            }
          };
      return new RecordBatchStream(new RecordBatchStreamBridge(adapter, allocator));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to execute stream", e);
    }
  }

  RecordBatchStream collect(BufferAllocator allocator) {
    checkNotClosed();
    try {
      DfRecordBatchStream dfStream = dfDf.collectStream();
      NativeRecordBatchStream adapter =
          new NativeRecordBatchStream() {
            public int next(long a, long s) {
              return dfStream.next(a, s);
            }

            public void schemaTo(long s) {
              dfStream.schemaTo(s);
            }

            public void close() {
              dfStream.close();
            }
          };
      return new RecordBatchStream(new RecordBatchStreamBridge(adapter, allocator));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to collect", e);
    }
  }

  DataFrameBridge filter(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.filterBytes(seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to filter", e);
    }
  }

  DataFrameBridge select(List<Expr> exprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(exprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.selectBytes(seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to select", e);
    }
  }

  DataFrameBridge aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    checkNotClosed();
    byte[] groupBytes = ExprProtoConverter.toProtoBytes(groupExprs);
    byte[] aggrBytes = ExprProtoConverter.toProtoBytes(aggrExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment groupSeg = allocBytes(arena, groupBytes);
      MemorySegment aggrSeg = allocBytes(arena, aggrBytes);
      DfDataFrame result =
          dfDf.aggregateBytes(groupSeg.address(), groupBytes.length, aggrSeg.address(), aggrBytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to aggregate", e);
    }
  }

  DataFrameBridge sort(List<SortExpr> sortExprs) {
    checkNotClosed();
    byte[] bytes = sortExprToProtoBytes(sortExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.sortBytes(seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to sort", e);
    }
  }

  DataFrameBridge limit(long skip, long fetch) {
    checkNotClosed();
    try {
      DfDataFrame result = dfDf.limit(skip, fetch);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to limit", e);
    }
  }

  DataFrameBridge join(
      DataFrameBridge right,
      JoinType joinType,
      List<String> leftCols,
      List<String> rightCols,
      Expr filter) {
    checkNotClosed();
    byte[] leftBytes = encodeNullSeparated(leftCols.toArray(new String[0]));
    byte[] rightBytes = encodeNullSeparated(rightCols.toArray(new String[0]));
    byte[] filterBytes = filter != null ? ExprProtoConverter.toProtoBytes(List.of(filter)) : new byte[0];
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment leftSeg = allocBytes(arena, leftBytes);
      MemorySegment rightSeg = allocBytes(arena, rightBytes);
      MemorySegment filterSeg = allocBytes(arena, filterBytes);
      DfDataFrame result =
          dfDf.join(
              right.dfDf,
              joinTypeToDfJoinType(joinType),
              leftSeg.address(), leftBytes.length,
              rightSeg.address(), rightBytes.length,
              filterSeg.address(), filterBytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to join", e);
    }
  }

  DataFrameBridge joinOn(DataFrameBridge right, JoinType joinType, List<Expr> onExprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(onExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.joinOnBytes(right.dfDf, joinTypeToDfJoinType(joinType), seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to join_on", e);
    }
  }

  DataFrameBridge union(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.union(other.dfDf));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to union", e);
    }
  }

  DataFrameBridge unionDistinct(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.unionDistinct(other.dfDf));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to union distinct", e);
    }
  }

  DataFrameBridge intersect(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.intersect(other.dfDf));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to intersect", e);
    }
  }

  DataFrameBridge except(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.exceptAll(other.dfDf));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to except", e);
    }
  }

  DataFrameBridge distinct() {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.distinct());
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to distinct", e);
    }
  }

  DataFrameBridge withColumn(String name, Expr expr) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(expr));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.withColumn(name, seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to with_column", e);
    }
  }

  DataFrameBridge withColumnRenamed(String oldName, String newName) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.withColumnRenamed(oldName, newName));
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to with_column_renamed", e);
    }
  }

  DataFrameBridge dropColumns(List<String> columns) {
    checkNotClosed();
    byte[] bytes = encodeNullSeparated(columns.toArray(new String[0]));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment seg = allocBytes(arena, bytes);
      DfDataFrame result = dfDf.dropColumns(seg.address(), bytes.length);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to drop_columns", e);
    }
  }

  void writeParquet(String path) {
    checkNotClosed();
    try {
      dfDf.writeParquet(path);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to write parquet", e);
    }
  }

  void writeCsv(String path) {
    checkNotClosed();
    try {
      dfDf.writeCsv(path);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to write csv", e);
    }
  }

  void writeJson(String path) {
    checkNotClosed();
    try {
      dfDf.writeJson(path);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to write json", e);
    }
  }

  void show() {
    checkNotClosed();
    try {
      dfDf.show();
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to show", e);
    }
  }

  long count() {
    checkNotClosed();
    try {
      return dfDf.count();
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to count", e);
    }
  }

  Schema schema() {
    checkNotClosed();
    try (RootAllocator tempAllocator = new RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator)) {
      dfDf.schemaTo(ffiSchema.memoryAddress());
      return Data.importSchema(tempAllocator, ffiSchema, null);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to get schema", e);
    }
  }

  // ── Private helpers ──

  private static DfJoinType joinTypeToDfJoinType(JoinType joinType) {
    return switch (joinType) {
      case INNER -> DfJoinType.INNER;
      case LEFT -> DfJoinType.LEFT;
      case RIGHT -> DfJoinType.RIGHT;
      case FULL -> DfJoinType.FULL;
      case LEFT_SEMI -> DfJoinType.LEFT_SEMI;
      case LEFT_ANTI -> DfJoinType.LEFT_ANTI;
      case RIGHT_SEMI -> DfJoinType.RIGHT_SEMI;
      case RIGHT_ANTI -> DfJoinType.RIGHT_ANTI;
    };
  }

  private static MemorySegment allocBytes(Arena arena, byte[] bytes) {
    if (bytes.length == 0) return MemorySegment.NULL;
    MemorySegment seg = arena.allocate(bytes.length);
    seg.copyFrom(MemorySegment.ofArray(bytes));
    return seg;
  }

  private static byte[] encodeNullSeparated(String[] strs) {
    if (strs.length == 0) return new byte[0];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strs.length; i++) {
      if (i > 0) sb.append('\0');
      sb.append(strs[i]);
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] sortExprToProtoBytes(List<SortExpr> sortExprs) {
    SortExprNodeCollection.Builder builder = SortExprNodeCollection.newBuilder();
    for (SortExpr sort : sortExprs) {
      builder.addSortExprNodes(
          org.apache.arrow.datafusion.proto.SortExprNode.newBuilder()
              .setExpr(ExprProtoConverter.toProto(sort.expr()))
              .setAsc(sort.asc())
              .setNullsFirst(sort.nullsFirst()));
    }
    return builder.build().toByteArray();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("DataFrame has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dfDf.close();
      logger.debug("Closed DataFrame");
    }
  }

  private static String dfErrorMessage(DfError e) {
    try (e) {
      return e.toDisplay();
    }
  }
}
