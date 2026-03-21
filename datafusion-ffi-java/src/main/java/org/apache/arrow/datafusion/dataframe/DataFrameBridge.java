package org.apache.arrow.datafusion.dataframe;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.CsvOptions;
import org.apache.arrow.datafusion.DfDataFrame;
import org.apache.arrow.datafusion.DfError;
import org.apache.arrow.datafusion.DfInsertOp;
import org.apache.arrow.datafusion.DfJoinType;
import org.apache.arrow.datafusion.DfLazyRecordBatchStream;
import org.apache.arrow.datafusion.DfRecordBatchStream;
import org.apache.arrow.datafusion.DfWriteOptions;
import org.apache.arrow.datafusion.ExprProtoConverter;
import org.apache.arrow.datafusion.JsonOptions;
import org.apache.arrow.datafusion.ParquetOptions;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.InsertOp;
import org.apache.arrow.datafusion.logical_expr.JoinType;
import org.apache.arrow.datafusion.logical_expr.SortExpr;
import org.apache.arrow.datafusion.physical_plan.NativeSendableRecordBatchStream;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStreamBridge;
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
public final class DataFrameBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrameBridge.class);

  private final DfDataFrame dfDf;
  private volatile boolean closed = false;

  public DataFrameBridge(DfDataFrame dfDf) {
    this.dfDf = dfDf;
  }

  SendableRecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();
    try {
      DfLazyRecordBatchStream lazyStream = dfDf.executeStream();
      NativeSendableRecordBatchStream adapter =
          new NativeSendableRecordBatchStream() {
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
      return new SendableRecordBatchStream(new SendableRecordBatchStreamBridge(adapter, allocator));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to execute stream", e);
    }
  }

  SendableRecordBatchStream collect(BufferAllocator allocator) {
    checkNotClosed();
    try {
      DfRecordBatchStream dfStream = dfDf.collectStream();
      NativeSendableRecordBatchStream adapter =
          new NativeSendableRecordBatchStream() {
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
      return new SendableRecordBatchStream(new SendableRecordBatchStreamBridge(adapter, allocator));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to collect", e);
    }
  }

  DataFrameBridge filter(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    try {
      DfDataFrame result = dfDf.filterBytes(bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to filter", e);
    }
  }

  DataFrameBridge select(List<Expr> exprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(exprs);
    try {
      DfDataFrame result = dfDf.selectBytes(bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to select", e);
    }
  }

  DataFrameBridge aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    checkNotClosed();
    byte[] groupBytes = ExprProtoConverter.toProtoBytes(groupExprs);
    byte[] aggrBytes = ExprProtoConverter.toProtoBytes(aggrExprs);
    try {
      DfDataFrame result = dfDf.aggregateBytes(groupBytes, aggrBytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to aggregate", e);
    }
  }

  DataFrameBridge sort(List<SortExpr> sortExprs) {
    checkNotClosed();
    byte[] bytes = sortExprToProtoBytes(sortExprs);
    try {
      DfDataFrame result = dfDf.sortBytes(bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to sort", e);
    }
  }

  DataFrameBridge limit(long skip, long fetch) {
    checkNotClosed();
    try {
      DfDataFrame result = dfDf.limit(skip, fetch);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to limit", e);
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
    byte[] filterBytes =
        filter != null ? ExprProtoConverter.toProtoBytes(List.of(filter)) : new byte[0];
    try {
      DfDataFrame result =
          dfDf.join(right.dfDf, joinTypeToDfJoinType(joinType), leftBytes, rightBytes, filterBytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to join", e);
    }
  }

  DataFrameBridge joinOn(DataFrameBridge right, JoinType joinType, List<Expr> onExprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(onExprs);
    try {
      DfDataFrame result = dfDf.joinOnBytes(right.dfDf, joinTypeToDfJoinType(joinType), bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to join_on", e);
    }
  }

  DataFrameBridge union(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.union(other.dfDf));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to union", e);
    }
  }

  DataFrameBridge unionDistinct(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.unionDistinct(other.dfDf));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to union distinct", e);
    }
  }

  DataFrameBridge intersect(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.intersect(other.dfDf));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to intersect", e);
    }
  }

  DataFrameBridge except(DataFrameBridge other) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.exceptAll(other.dfDf));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to except", e);
    }
  }

  DataFrameBridge distinct() {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.distinct());
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to distinct", e);
    }
  }

  DataFrameBridge withColumn(String name, Expr expr) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(expr));
    try {
      DfDataFrame result = dfDf.withColumn(name, bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to with_column", e);
    }
  }

  DataFrameBridge withColumnRenamed(String oldName, String newName) {
    checkNotClosed();
    try {
      return new DataFrameBridge(dfDf.withColumnRenamed(oldName, newName));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to with_column_renamed", e);
    }
  }

  DataFrameBridge dropColumns(List<String> columns) {
    checkNotClosed();
    byte[] bytes = encodeNullSeparated(columns.toArray(new String[0]));
    try {
      DfDataFrame result = dfDf.dropColumns(bytes);
      return new DataFrameBridge(result);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to drop_columns", e);
    }
  }

  void writeParquet(String path) {
    checkNotClosed();
    try {
      dfDf.writeParquet(path);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write parquet", e);
    }
  }

  void writeCsv(String path) {
    checkNotClosed();
    try {
      dfDf.writeCsv(path);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write csv", e);
    }
  }

  void writeJson(String path) {
    checkNotClosed();
    try {
      dfDf.writeJson(path);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write json", e);
    }
  }

  void writeParquet(String path, DataFrameWriteOptions writeOpts, ParquetOptions formatOpts) {
    checkNotClosed();
    try {
      DfWriteOptions dfOpts = toDfWriteOptions(writeOpts);
      byte[] partitionBytes = encodeNullSeparated(writeOpts.partitionBy().toArray(new String[0]));
      byte[] formatBytes = formatOpts != null ? formatOpts.encodeOptions() : new byte[0];
      dfDf.writeParquetWithOptions(path, dfOpts, partitionBytes, formatBytes);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write parquet", e);
    }
  }

  void writeCsv(String path, DataFrameWriteOptions writeOpts, CsvOptions formatOpts) {
    checkNotClosed();
    try {
      DfWriteOptions dfOpts = toDfWriteOptions(writeOpts);
      byte[] partitionBytes = encodeNullSeparated(writeOpts.partitionBy().toArray(new String[0]));
      byte[] formatBytes = formatOpts != null ? formatOpts.encodeOptions() : new byte[0];
      dfDf.writeCsvWithOptions(path, dfOpts, partitionBytes, formatBytes);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write csv", e);
    }
  }

  void writeJson(String path, DataFrameWriteOptions writeOpts, JsonOptions formatOpts) {
    checkNotClosed();
    try {
      DfWriteOptions dfOpts = toDfWriteOptions(writeOpts);
      byte[] partitionBytes = encodeNullSeparated(writeOpts.partitionBy().toArray(new String[0]));
      byte[] formatBytes = formatOpts != null ? formatOpts.encodeOptions() : new byte[0];
      dfDf.writeJsonWithOptions(path, dfOpts, partitionBytes, formatBytes);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to write json", e);
    }
  }

  void show() {
    checkNotClosed();
    try {
      dfDf.show();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to show", e);
    }
  }

  long count() {
    checkNotClosed();
    try {
      return dfDf.count();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to count", e);
    }
  }

  Schema schema() {
    checkNotClosed();
    try (RootAllocator tempAllocator = new RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator)) {
      dfDf.schemaTo(ffiSchema.memoryAddress());
      return Data.importSchema(tempAllocator, ffiSchema, null);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get schema", e);
    }
  }

  // ── Private helpers ──

  private static DfWriteOptions toDfWriteOptions(DataFrameWriteOptions writeOpts) {
    DfWriteOptions dfOpts = new DfWriteOptions();
    dfOpts.singleFileOutput = writeOpts.singleFileOutput();
    dfOpts.insertOp = insertOpToDfInsertOp(writeOpts.insertOp());
    return dfOpts;
  }

  private static DfInsertOp insertOpToDfInsertOp(InsertOp insertOp) {
    return switch (insertOp) {
      case APPEND -> DfInsertOp.APPEND;
      case OVERWRITE -> DfInsertOp.OVERWRITE;
      case REPLACE -> DfInsertOp.REPLACE;
    };
  }

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
}
