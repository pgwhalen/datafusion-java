package org.apache.arrow.datafusion.logical_expr;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.ExprProtoConverter;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.generated.DfDdlKind;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfExprBytes;
import org.apache.arrow.datafusion.generated.DfLogicalPlan;
import org.apache.arrow.datafusion.generated.DfLogicalPlanKind;
import org.apache.arrow.datafusion.generated.DfStatementKind;
import org.apache.arrow.datafusion.generated.DfTableRefType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public LogicalPlan API and Diplomat-generated DfLogicalPlan.
 *
 * <p>This replaces LogicalPlanFfi, delegating to the Diplomat-generated class for all native calls.
 */
public final class LogicalPlanBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanBridge.class);

  private final DfLogicalPlan dfPlan;
  private volatile boolean closed = false;

  public LogicalPlanBridge(DfLogicalPlan dfPlan) {
    this.dfPlan = dfPlan;
  }

  public DfLogicalPlan dfPlan() {
    checkNotClosed();
    return dfPlan;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("LogicalPlan has been closed");
    }
  }

  // ── Common methods ──

  DfLogicalPlanKind kind() {
    checkNotClosed();
    return dfPlan.kind();
  }

  Schema schema() {
    checkNotClosed();
    try (RootAllocator tempAllocator = new RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator)) {
      dfPlan.schemaTo(ffiSchema.memoryAddress());
      return Data.importSchema(tempAllocator, ffiSchema, null);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get schema", e);
    }
  }

  int inputsCount() {
    checkNotClosed();
    return (int) dfPlan.inputsCount();
  }

  LogicalPlanBridge inputAt(int index) {
    checkNotClosed();
    try {
      return new LogicalPlanBridge(dfPlan.inputAt(index));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  List<Expr> expressions() {
    checkNotClosed();
    return readExprs(dfPlan.expressionsProto());
  }

  String displayIndent() {
    checkNotClosed();
    return dfPlan.toDisplay();
  }

  String displaySingle() {
    checkNotClosed();
    return dfPlan.displaySingle();
  }

  String displayIndentSchema() {
    checkNotClosed();
    return dfPlan.displayIndentSchema();
  }

  String displayGraphviz() {
    checkNotClosed();
    return dfPlan.displayGraphviz();
  }

  String displayPgJson() {
    checkNotClosed();
    return dfPlan.displayPgJson();
  }

  OptionalLong maxRows() {
    checkNotClosed();
    long val = dfPlan.maxRows();
    return val < 0 ? OptionalLong.empty() : OptionalLong.of(val);
  }

  boolean containsOuterReference() {
    checkNotClosed();
    return dfPlan.containsOuterReference();
  }

  // ── Variant-specific accessors ──

  // -- Filter --
  Expr filterPredicate() {
    return readExprs(dfPlan.filterPredicateProto()).get(0);
  }

  // -- Sort --
  List<SortExpr> sortExprs() {
    return readSortExprs(dfPlan.sortExprsProto());
  }

  OptionalLong sortFetch() {
    long val = dfPlan.sortFetch();
    return val < 0 ? OptionalLong.empty() : OptionalLong.of(val);
  }

  // -- Join --
  JoinType joinType() {
    try {
      return switch (dfPlan.joinType()) {
        case INNER -> JoinType.INNER;
        case LEFT -> JoinType.LEFT;
        case RIGHT -> JoinType.RIGHT;
        case FULL -> JoinType.FULL;
        case LEFT_SEMI -> JoinType.LEFT_SEMI;
        case LEFT_ANTI -> JoinType.LEFT_ANTI;
        case RIGHT_SEMI -> JoinType.RIGHT_SEMI;
        case RIGHT_ANTI -> JoinType.RIGHT_ANTI;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  JoinConstraint joinConstraint() {
    try {
      return switch (dfPlan.joinConstraint()) {
        case ON -> JoinConstraint.ON;
        case USING -> JoinConstraint.USING;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  List<Expr> joinOnLeftKeys() {
    return readExprs(dfPlan.joinOnLeftProto());
  }

  List<Expr> joinOnRightKeys() {
    return readExprs(dfPlan.joinOnRightProto());
  }

  Optional<Expr> joinFilter() {
    List<Expr> exprs = readExprs(dfPlan.joinFilterProto());
    return exprs.isEmpty() ? Optional.empty() : Optional.of(exprs.get(0));
  }

  NullEquality joinNullEquality() {
    try {
      return switch (dfPlan.joinNullEquality()) {
        case NULL_EQUALS_NOTHING -> NullEquality.NULL_EQUALS_NOTHING;
        case NULL_EQUALS_NULL -> NullEquality.NULL_EQUALS_NULL;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  // -- Aggregate --
  List<Expr> aggregateGroupExprs() {
    return readExprs(dfPlan.aggregateGroupExprsProto());
  }

  List<Expr> aggregateAggrExprs() {
    return readExprs(dfPlan.aggregateAggrExprsProto());
  }

  // -- Limit --
  Optional<Expr> limitSkip() {
    List<Expr> exprs = readExprs(dfPlan.limitSkipProto());
    return exprs.isEmpty() ? Optional.empty() : Optional.of(exprs.get(0));
  }

  Optional<Expr> limitFetch() {
    List<Expr> exprs = readExprs(dfPlan.limitFetchProto());
    return exprs.isEmpty() ? Optional.empty() : Optional.of(exprs.get(0));
  }

  // -- TableScan --
  TableReference tableScanTableName() {
    String table = dfPlan.tableScanName();
    DfTableRefType refType = dfPlan.tableScanRefType();
    return switch (refType) {
      case BARE -> new TableReference.Bare(table);
      case PARTIAL -> new TableReference.Partial(dfPlan.tableScanSchemaName(), table);
      case FULL ->
          new TableReference.Full(
              dfPlan.tableScanCatalogName(), dfPlan.tableScanSchemaName(), table);
      case NONE -> new TableReference.Bare(table);
    };
  }

  OptionalLong tableScanFetch() {
    long val = dfPlan.tableScanFetch();
    return val < 0 ? OptionalLong.empty() : OptionalLong.of(val);
  }

  Optional<List<Integer>> tableScanProjection() {
    if (!dfPlan.tableScanHasProjection()) {
      return Optional.empty();
    }
    try (DfExprBytes projBytes = dfPlan.tableScanProjectionBytes()) {
      byte[] raw = readRawBytes(projBytes);
      if (raw.length == 0) {
        return Optional.of(List.of());
      }
      List<Integer> result = new ArrayList<>(raw.length / 4);
      for (int i = 0; i < raw.length; i += 4) {
        int val =
            (raw[i] & 0xFF)
                | ((raw[i + 1] & 0xFF) << 8)
                | ((raw[i + 2] & 0xFF) << 16)
                | ((raw[i + 3] & 0xFF) << 24);
        result.add(val);
      }
      return Optional.of(List.copyOf(result));
    }
  }

  List<Expr> tableScanFilters() {
    return readExprs(dfPlan.tableScanFiltersProto());
  }

  // -- EmptyRelation --
  boolean emptyRelationProduceOneRow() {
    return dfPlan.emptyRelationProduceOneRow();
  }

  // -- SubqueryAlias --
  String subqueryAliasName() {
    return dfPlan.subqueryAliasName();
  }

  // -- Explain --
  boolean explainVerbose() {
    return dfPlan.explainVerbose();
  }

  // -- Analyze --
  boolean analyzeVerbose() {
    return dfPlan.analyzeVerbose();
  }

  // -- RecursiveQuery --
  String recursiveQueryName() {
    return dfPlan.recursiveQueryName();
  }

  boolean recursiveQueryIsDistinct() {
    return dfPlan.recursiveQueryIsDistinct();
  }

  // -- Distinct --
  boolean distinctIsOn() {
    return dfPlan.distinctIsOn();
  }

  List<Expr> distinctOnOnExprs() {
    return readExprs(dfPlan.distinctOnOnExprsProto());
  }

  List<Expr> distinctOnSelectExprs() {
    return readExprs(dfPlan.distinctOnSelectExprsProto());
  }

  Optional<List<SortExpr>> distinctOnSortExprs() {
    List<SortExpr> exprs = readSortExprs(dfPlan.distinctOnSortExprsProto());
    return exprs.isEmpty() ? Optional.empty() : Optional.of(exprs);
  }

  // -- Values --
  int valuesRowCount() {
    return (int) dfPlan.valuesRowCount();
  }

  int valuesColCount() {
    return (int) dfPlan.valuesColCount();
  }

  List<List<Expr>> valuesExprs() {
    List<Expr> flat = readExprs(dfPlan.valuesAllExprsProto());
    int rows = valuesRowCount();
    int cols = valuesColCount();
    if (rows == 0 || cols == 0) {
      return List.of();
    }
    List<List<Expr>> result = new ArrayList<>(rows);
    for (int r = 0; r < rows; r++) {
      List<Expr> row = new ArrayList<>(cols);
      for (int c = 0; c < cols; c++) {
        row.add(flat.get(r * cols + c));
      }
      result.add(List.copyOf(row));
    }
    return List.copyOf(result);
  }

  // -- Window --
  List<Expr> windowExprs() {
    return readExprs(dfPlan.windowExprsProto());
  }

  // -- Statement --
  DfStatementKind statementKind() {
    try {
      return dfPlan.statementKind();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TransactionAccessMode statementTxStartAccessMode() {
    try {
      return switch (dfPlan.statementTxStartAccessMode()) {
        case READ_ONLY -> TransactionAccessMode.READ_ONLY;
        case READ_WRITE -> TransactionAccessMode.READ_WRITE;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TransactionIsolationLevel statementTxStartIsolationLevel() {
    try {
      return switch (dfPlan.statementTxStartIsolationLevel()) {
        case READ_UNCOMMITTED -> TransactionIsolationLevel.READ_UNCOMMITTED;
        case READ_COMMITTED -> TransactionIsolationLevel.READ_COMMITTED;
        case REPEATABLE_READ -> TransactionIsolationLevel.REPEATABLE_READ;
        case SERIALIZABLE -> TransactionIsolationLevel.SERIALIZABLE;
        case SNAPSHOT -> TransactionIsolationLevel.SNAPSHOT;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TransactionConclusion statementTxEndConclusion() {
    try {
      return switch (dfPlan.statementTxEndConclusion()) {
        case COMMIT -> TransactionConclusion.COMMIT;
        case ROLLBACK -> TransactionConclusion.ROLLBACK;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean statementTxEndChain() {
    try {
      return dfPlan.statementTxEndChain();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String statementSetVariableName() {
    try {
      return dfPlan.statementSetVariableName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String statementSetVariableValue() {
    try {
      return dfPlan.statementSetVariableValue();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String statementResetVariableName() {
    try {
      return dfPlan.statementResetVariableName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String statementPrepareName() {
    try {
      return dfPlan.statementPrepareName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  Schema statementPrepareFields() {
    try (RootAllocator tempAllocator = new RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator)) {
      dfPlan.statementPrepareSchemaTo(ffiSchema.memoryAddress());
      return Data.importSchema(tempAllocator, ffiSchema, null);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get prepare fields", e);
    }
  }

  String statementExecuteName() {
    try {
      return dfPlan.statementExecuteName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  List<Expr> statementExecuteParams() {
    return readExprs(dfPlan.statementExecuteParamsProto());
  }

  String statementDeallocateName() {
    try {
      return dfPlan.statementDeallocateName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  // -- Dml --
  WriteOp dmlWriteOp() {
    try {
      return switch (dfPlan.dmlWriteOp()) {
        case INSERT_APPEND -> WriteOp.INSERT_APPEND;
        case INSERT_OVERWRITE -> WriteOp.INSERT_OVERWRITE;
        case INSERT_REPLACE -> WriteOp.INSERT_REPLACE;
        case DELETE -> WriteOp.DELETE;
        case UPDATE -> WriteOp.UPDATE;
        case CTAS -> WriteOp.CTAS;
      };
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TableReference dmlTableName() {
    return buildTableRef(
        dfPlan.dmlTableRefType(),
        dfPlan.dmlTableName(),
        dfPlan.dmlTableSchemaName(),
        dfPlan.dmlTableCatalogName());
  }

  // -- Ddl --
  DfDdlKind ddlKind() {
    try {
      return dfPlan.ddlKind();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String ddlName() {
    try {
      return dfPlan.ddlName();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TableReference ddlTableName() {
    return buildTableRef(
        dfPlan.ddlTableRefType(),
        dfPlan.ddlName(),
        dfPlan.ddlTableSchemaName(),
        dfPlan.ddlTableCatalogName());
  }

  boolean ddlIfNotExists() {
    try {
      return dfPlan.ddlIfNotExists();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean ddlIfExists() {
    try {
      return dfPlan.ddlIfExists();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean ddlOrReplace() {
    try {
      return dfPlan.ddlOrReplace();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean ddlTemporary() {
    try {
      return dfPlan.ddlTemporary();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean ddlCascade() {
    try {
      return dfPlan.ddlCascade();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String ddlLocation() {
    try {
      return dfPlan.ddlLocation();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  String ddlFileType() {
    try {
      return dfPlan.ddlFileType();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  Optional<String> ddlViewDefinition() {
    if (!dfPlan.ddlViewHasDefinition()) {
      return Optional.empty();
    }
    try {
      return Optional.of(dfPlan.ddlViewDefinition());
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  boolean ddlIndexHasName() {
    return dfPlan.ddlIndexHasName();
  }

  boolean ddlIndexUnique() {
    try {
      return dfPlan.ddlIndexUnique();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    }
  }

  TableReference ddlIndexTable() {
    return buildTableRef(
        dfPlan.ddlIndexTableRefType(),
        dfPlan.ddlIndexTableName(),
        dfPlan.ddlIndexTableSchemaName(),
        dfPlan.ddlIndexTableCatalogName());
  }

  // ── Helper methods ──

  private static TableReference buildTableRef(
      DfTableRefType refType, String table, String schema, String catalog) {
    return switch (refType) {
      case BARE, NONE -> new TableReference.Bare(table);
      case PARTIAL -> new TableReference.Partial(schema, table);
      case FULL -> new TableReference.Full(catalog, schema, table);
    };
  }

  private static List<Expr> readExprs(DfExprBytes exprBytes) {
    try (exprBytes) {
      byte[] raw = readRawBytes(exprBytes);
      if (raw.length == 0) {
        return List.of();
      }
      return ExprProtoConverter.fromProtoBytes(raw);
    }
  }

  private static List<SortExpr> readSortExprs(DfExprBytes exprBytes) {
    try (exprBytes) {
      byte[] raw = readRawBytes(exprBytes);
      if (raw.length == 0) {
        return List.of();
      }
      return ExprProtoConverter.sortExprsFromProtoBytes(raw);
    }
  }

  private static byte[] readRawBytes(DfExprBytes exprBytes) {
    long len = exprBytes.len();
    if (len == 0) {
      return new byte[0];
    }
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment buf = arena.allocate(len);
      exprBytes.copyTo(buf.address(), len);
      return buf.toArray(ValueLayout.JAVA_BYTE);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dfPlan.close();
      logger.debug("Closed LogicalPlan");
    }
  }
}
