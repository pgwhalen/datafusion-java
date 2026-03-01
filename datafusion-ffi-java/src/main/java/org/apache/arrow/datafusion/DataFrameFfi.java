package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
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
 * Internal FFI helper for DataFrame.
 *
 * <p>This class owns the native DataFrame pointer and contains all native call logic. The public
 * {@code DataFrame} class delegates to this.
 */
final class DataFrameFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DataFrameFfi.class);

  private static final MethodHandle DATAFRAME_DESTROY =
      NativeUtil.downcall(
          "datafusion_dataframe_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle DATAFRAME_EXECUTE_STREAM =
      NativeUtil.downcall(
          "datafusion_dataframe_execute_stream",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("error_out")));

  // ── Phase 2: Transformation FFI methods ──

  private static final MethodHandle DATAFRAME_FILTER =
      NativeUtil.downcall(
          "datafusion_dataframe_filter",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("predicate_bytes"),
              ValueLayout.JAVA_LONG.withName("predicate_len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_SELECT =
      NativeUtil.downcall(
          "datafusion_dataframe_select",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("expr_bytes"),
              ValueLayout.JAVA_LONG.withName("expr_len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_AGGREGATE =
      NativeUtil.downcall(
          "datafusion_dataframe_aggregate",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("group_bytes"),
              ValueLayout.JAVA_LONG.withName("group_len"),
              ValueLayout.ADDRESS.withName("aggr_bytes"),
              ValueLayout.JAVA_LONG.withName("aggr_len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_SORT =
      NativeUtil.downcall(
          "datafusion_dataframe_sort",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("sort_bytes"),
              ValueLayout.JAVA_LONG.withName("sort_len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_LIMIT =
      NativeUtil.downcall(
          "datafusion_dataframe_limit",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.JAVA_LONG.withName("skip"),
              ValueLayout.JAVA_LONG.withName("fetch"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_COLLECT =
      NativeUtil.downcall(
          "datafusion_dataframe_collect",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_SHOW =
      NativeUtil.downcall(
          "datafusion_dataframe_show",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_COUNT =
      NativeUtil.downcall(
          "datafusion_dataframe_count",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("count_out"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_SCHEMA =
      NativeUtil.downcall(
          "datafusion_dataframe_schema",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("schema_out"),
              ValueLayout.ADDRESS.withName("error_out")));

  // ── Phase 3: Joins and set operations ──

  private static final MethodHandle DATAFRAME_JOIN =
      NativeUtil.downcall(
          "datafusion_dataframe_join",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("left"),
              ValueLayout.ADDRESS.withName("right"),
              ValueLayout.JAVA_INT.withName("join_type"),
              ValueLayout.ADDRESS.withName("left_cols"),
              ValueLayout.ADDRESS.withName("right_cols"),
              ValueLayout.JAVA_LONG.withName("num_cols"),
              ValueLayout.ADDRESS.withName("filter_bytes"),
              ValueLayout.JAVA_LONG.withName("filter_len"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_JOIN_ON =
      NativeUtil.downcall(
          "datafusion_dataframe_join_on",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("left"),
              ValueLayout.ADDRESS.withName("right"),
              ValueLayout.JAVA_INT.withName("join_type"),
              ValueLayout.ADDRESS.withName("on_bytes"),
              ValueLayout.JAVA_LONG.withName("on_len"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_UNION =
      NativeUtil.downcall(
          "datafusion_dataframe_union",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("other"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_UNION_DISTINCT =
      NativeUtil.downcall(
          "datafusion_dataframe_union_distinct",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("other"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_INTERSECT =
      NativeUtil.downcall(
          "datafusion_dataframe_intersect",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("other"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_EXCEPT =
      NativeUtil.downcall(
          "datafusion_dataframe_except",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("other"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_DISTINCT =
      NativeUtil.downcall(
          "datafusion_dataframe_distinct",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("error_out")));

  // ── Phase 5: Advanced features ──

  private static final MethodHandle DATAFRAME_WITH_COLUMN =
      NativeUtil.downcall(
          "datafusion_dataframe_with_column",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("name"),
              ValueLayout.ADDRESS.withName("expr_bytes"),
              ValueLayout.JAVA_LONG.withName("expr_len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_WITH_COLUMN_RENAMED =
      NativeUtil.downcall(
          "datafusion_dataframe_with_column_renamed",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("old_name"),
              ValueLayout.ADDRESS.withName("new_name"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_DROP_COLUMNS =
      NativeUtil.downcall(
          "datafusion_dataframe_drop_columns",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("col_names"),
              ValueLayout.JAVA_LONG.withName("num_cols"),
              ValueLayout.ADDRESS.withName("error_out")));

  // ── Write operations ──

  private static final MethodHandle DATAFRAME_WRITE_PARQUET =
      NativeUtil.downcall(
          "datafusion_dataframe_write_parquet",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("path"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_WRITE_CSV =
      NativeUtil.downcall(
          "datafusion_dataframe_write_csv",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("path"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle DATAFRAME_WRITE_JSON =
      NativeUtil.downcall(
          "datafusion_dataframe_write_json",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("df"),
              ValueLayout.ADDRESS.withName("path"),
              ValueLayout.ADDRESS.withName("error_out")));

  private final MemorySegment runtime;
  private final MemorySegment dataframe;
  private final MemorySegment context;
  private volatile boolean closed = false;

  DataFrameFfi(MemorySegment runtime, MemorySegment dataframe) {
    this(runtime, dataframe, MemorySegment.NULL);
  }

  DataFrameFfi(MemorySegment runtime, MemorySegment dataframe, MemorySegment context) {
    this.runtime = runtime;
    this.dataframe = dataframe;
    this.context = context;
  }

  RecordBatchStream executeStream(BufferAllocator allocator) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment stream =
          NativeUtil.callForPointer(
              arena,
              "Execute stream",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_EXECUTE_STREAM.invokeExact(runtime, dataframe, errorOut));
      logger.debug("Created RecordBatchStream: {}", stream);
      return new RecordBatchStream(new RecordBatchStreamFfi(runtime, stream, allocator));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to execute stream", e);
    }
  }

  // ── Phase 2: Transformations ──

  DataFrameFfi filter(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment bytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame filter",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_FILTER.invokeExact(
                          runtime,
                          dataframe,
                          context,
                          bytesSegment,
                          (long) bytes.length,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to filter", e);
    }
  }

  DataFrameFfi select(List<Expr> exprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(exprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment bytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame select",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_SELECT.invokeExact(
                          runtime,
                          dataframe,
                          context,
                          bytesSegment,
                          (long) bytes.length,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to select", e);
    }
  }

  DataFrameFfi aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    checkNotClosed();
    byte[] groupBytes = ExprProtoConverter.toProtoBytes(groupExprs);
    byte[] aggrBytes = ExprProtoConverter.toProtoBytes(aggrExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment groupSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, groupBytes);
      MemorySegment aggrSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, aggrBytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame aggregate",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_AGGREGATE.invokeExact(
                          runtime,
                          dataframe,
                          context,
                          groupSegment,
                          (long) groupBytes.length,
                          aggrSegment,
                          (long) aggrBytes.length,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to aggregate", e);
    }
  }

  DataFrameFfi sort(List<SortExpr> sortExprs) {
    checkNotClosed();
    byte[] bytes = sortExprToProtoBytes(sortExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment bytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame sort",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_SORT.invokeExact(
                          runtime,
                          dataframe,
                          context,
                          bytesSegment,
                          (long) bytes.length,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to sort", e);
    }
  }

  DataFrameFfi limit(long skip, long fetch) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame limit",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_LIMIT.invokeExact(runtime, dataframe, skip, fetch, errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to limit", e);
    }
  }

  RecordBatchStream collect(BufferAllocator allocator) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment stream =
          NativeUtil.callForPointer(
              arena,
              "DataFrame collect",
              errorOut ->
                  (MemorySegment) DATAFRAME_COLLECT.invokeExact(runtime, dataframe, errorOut));
      return new RecordBatchStream(new RecordBatchStreamFfi(runtime, stream, allocator));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to collect", e);
    }
  }

  void show() {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      NativeUtil.call(
          arena,
          "DataFrame show",
          errorOut -> (int) DATAFRAME_SHOW.invokeExact(runtime, dataframe, errorOut));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to show", e);
    }
  }

  long count() {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment countOut = arena.allocate(ValueLayout.JAVA_LONG);
      NativeUtil.call(
          arena,
          "DataFrame count",
          errorOut -> (int) DATAFRAME_COUNT.invokeExact(runtime, dataframe, countOut, errorOut));
      return countOut.get(ValueLayout.JAVA_LONG, 0);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to count", e);
    }
  }

  Schema schema() {
    checkNotClosed();
    try (RootAllocator tempAllocator = new RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator);
        Arena arena = Arena.ofConfined()) {
      MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());
      NativeUtil.call(
          arena,
          "DataFrame schema",
          errorOut -> (int) DATAFRAME_SCHEMA.invokeExact(dataframe, schemaAddr, errorOut));
      return Data.importSchema(tempAllocator, ffiSchema, null);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get schema", e);
    }
  }

  // ── Phase 3: Joins and set operations ──

  DataFrameFfi join(
      DataFrameFfi right,
      JoinType joinType,
      List<String> leftCols,
      List<String> rightCols,
      Expr filter) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment leftColsArray = allocateStringArray(arena, leftCols);
      MemorySegment rightColsArray = allocateStringArray(arena, rightCols);

      MemorySegment filterBytesSegment;
      long filterLen;
      if (filter != null) {
        byte[] filterBytes = ExprProtoConverter.toProtoBytes(List.of(filter));
        filterBytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, filterBytes);
        filterLen = filterBytes.length;
      } else {
        filterBytesSegment = MemorySegment.NULL;
        filterLen = 0;
      }

      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame join",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_JOIN.invokeExact(
                          runtime,
                          dataframe,
                          right.dataframe,
                          joinTypeToInt(joinType),
                          leftColsArray,
                          rightColsArray,
                          (long) leftCols.size(),
                          filterBytesSegment,
                          filterLen,
                          context,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to join", e);
    }
  }

  DataFrameFfi joinOn(DataFrameFfi right, JoinType joinType, List<Expr> onExprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(onExprs);
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment bytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame join_on",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_JOIN_ON.invokeExact(
                          runtime,
                          dataframe,
                          right.dataframe,
                          joinTypeToInt(joinType),
                          bytesSegment,
                          (long) bytes.length,
                          context,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to join_on", e);
    }
  }

  DataFrameFfi union(DataFrameFfi other) {
    return setOperation(DATAFRAME_UNION, other, "union");
  }

  DataFrameFfi unionDistinct(DataFrameFfi other) {
    return setOperation(DATAFRAME_UNION_DISTINCT, other, "union distinct");
  }

  DataFrameFfi intersect(DataFrameFfi other) {
    return setOperation(DATAFRAME_INTERSECT, other, "intersect");
  }

  DataFrameFfi except(DataFrameFfi other) {
    return setOperation(DATAFRAME_EXCEPT, other, "except");
  }

  DataFrameFfi distinct() {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame distinct",
              errorOut ->
                  (MemorySegment) DATAFRAME_DISTINCT.invokeExact(runtime, dataframe, errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to distinct", e);
    }
  }

  // ── Phase 5: Advanced features ──

  DataFrameFfi withColumn(String name, Expr expr) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(expr));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment nameSegment = arena.allocateFrom(name);
      MemorySegment bytesSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame with_column",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_WITH_COLUMN.invokeExact(
                          runtime,
                          dataframe,
                          context,
                          nameSegment,
                          bytesSegment,
                          (long) bytes.length,
                          errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to with_column", e);
    }
  }

  DataFrameFfi withColumnRenamed(String oldName, String newName) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment oldSegment = arena.allocateFrom(oldName);
      MemorySegment newSegment = arena.allocateFrom(newName);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame with_column_renamed",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_WITH_COLUMN_RENAMED.invokeExact(
                          runtime, dataframe, oldSegment, newSegment, errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to with_column_renamed", e);
    }
  }

  DataFrameFfi dropColumns(List<String> columns) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment colsArray = allocateStringArray(arena, columns);
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame drop_columns",
              errorOut ->
                  (MemorySegment)
                      DATAFRAME_DROP_COLUMNS.invokeExact(
                          runtime, dataframe, colsArray, (long) columns.size(), errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to drop_columns", e);
    }
  }

  // ── Write operations ──

  void writeParquet(String path) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pathSegment = arena.allocateFrom(path);
      NativeUtil.call(
          arena,
          "DataFrame write_parquet",
          errorOut ->
              (int) DATAFRAME_WRITE_PARQUET.invokeExact(runtime, dataframe, pathSegment, errorOut));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to write parquet", e);
    }
  }

  void writeCsv(String path) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pathSegment = arena.allocateFrom(path);
      NativeUtil.call(
          arena,
          "DataFrame write_csv",
          errorOut ->
              (int) DATAFRAME_WRITE_CSV.invokeExact(runtime, dataframe, pathSegment, errorOut));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to write csv", e);
    }
  }

  void writeJson(String path) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment pathSegment = arena.allocateFrom(path);
      NativeUtil.call(
          arena,
          "DataFrame write_json",
          errorOut ->
              (int) DATAFRAME_WRITE_JSON.invokeExact(runtime, dataframe, pathSegment, errorOut));
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to write json", e);
    }
  }

  // ── Private helpers ──

  private DataFrameFfi setOperation(MethodHandle handle, DataFrameFfi other, String opName) {
    checkNotClosed();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment result =
          NativeUtil.callForPointer(
              arena,
              "DataFrame " + opName,
              errorOut ->
                  (MemorySegment) handle.invoke(runtime, dataframe, other.dataframe, errorOut));
      return new DataFrameFfi(runtime, result, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to " + opName, e);
    }
  }

  private static MemorySegment allocateStringArray(Arena arena, List<String> strings) {
    MemorySegment array = arena.allocate(ValueLayout.ADDRESS, Math.max(strings.size(), 1));
    for (int i = 0; i < strings.size(); i++) {
      array.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(strings.get(i)));
    }
    return array;
  }

  private static int joinTypeToInt(JoinType joinType) {
    return switch (joinType) {
      case INNER -> 0;
      case LEFT -> 1;
      case RIGHT -> 2;
      case FULL -> 3;
      case LEFT_SEMI -> 4;
      case LEFT_ANTI -> 5;
      case RIGHT_SEMI -> 6;
      case RIGHT_ANTI -> 7;
    };
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
      try {
        DATAFRAME_DESTROY.invokeExact(dataframe);
        logger.debug("Closed DataFrame");
      } catch (Throwable e) {
        throw new DataFusionException("Error closing DataFrame", e);
      }
    }
  }
}
