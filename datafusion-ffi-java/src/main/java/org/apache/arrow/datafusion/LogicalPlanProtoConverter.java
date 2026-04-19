package org.apache.arrow.datafusion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.JoinConstraint;
import org.apache.arrow.datafusion.logical_expr.JoinType;
import org.apache.arrow.datafusion.logical_expr.LogicalPlan;
import org.apache.arrow.datafusion.logical_expr.LogicalPlanBridge;
import org.apache.arrow.datafusion.logical_expr.NullEquality;
import org.apache.arrow.datafusion.logical_expr.SortExpr;
import org.apache.arrow.datafusion.logical_expr.WriteOp;
import org.apache.arrow.datafusion.proto.AggregateNode;
import org.apache.arrow.datafusion.proto.AnalyzeNode;
import org.apache.arrow.datafusion.proto.CreateCatalogNode;
import org.apache.arrow.datafusion.proto.CreateCatalogSchemaNode;
import org.apache.arrow.datafusion.proto.CreateExternalTableNode;
import org.apache.arrow.datafusion.proto.CreateViewNode;
import org.apache.arrow.datafusion.proto.DistinctOnNode;
import org.apache.arrow.datafusion.proto.DmlNode;
import org.apache.arrow.datafusion.proto.DropViewNode;
import org.apache.arrow.datafusion.proto.EmptyRelationNode;
import org.apache.arrow.datafusion.proto.ExplainNode;
import org.apache.arrow.datafusion.proto.JoinNode;
import org.apache.arrow.datafusion.proto.LimitNode;
import org.apache.arrow.datafusion.proto.LogicalExprNode;
import org.apache.arrow.datafusion.proto.LogicalPlanNode;
import org.apache.arrow.datafusion.proto.ProjectionNode;
import org.apache.arrow.datafusion.proto.RecursiveQueryNode;
import org.apache.arrow.datafusion.proto.SelectionNode;
import org.apache.arrow.datafusion.proto.SortExprNode;
import org.apache.arrow.datafusion.proto.SortNode;
import org.apache.arrow.datafusion.proto.SubqueryAliasNode;
import org.apache.arrow.datafusion.proto.ValuesNode;
import org.apache.arrow.datafusion.proto.WindowNode;

/**
 * Converts proto-encoded {@code LogicalPlanNode} messages into the Java {@link LogicalPlan} sealed
 * interface.
 *
 * <p>This is the plan-level analogue of {@link ExprProtoConverter}. A single FFI call to {@code
 * LogicalPlanBridge#toProtoBytesOrNull()} returns the current plan node (its child subtrees are
 * materialized recursively by re-entering {@link LogicalPlan#fromBridge}). Variants that {@code
 * datafusion-proto} cannot encode (Statement, CreateMemoryTable, CreateIndex, DropTable,
 * DropCatalogSchema, CreateFunction, DropFunction, Subquery, DescribeTable, Extension) fall back
 * to the variant-specific bridge accessors via {@link LogicalPlan#fromBridgeUnsupported}.
 * TableScan also falls back because the proto representation is lossy (stores column names rather
 * than indices and omits {@code fetch}).
 */
public final class LogicalPlanProtoConverter {
  private LogicalPlanProtoConverter() {}

  /** Materializes a {@link LogicalPlan} from a native bridge. */
  public static LogicalPlan fromBridge(LogicalPlanBridge bridge) {
    byte[] bytes = bridge.toProtoBytesOrNull();
    if (bytes == null) {
      return LogicalPlan.fromBridgeUnsupported(bridge);
    }
    LogicalPlanNode proto;
    try {
      proto = LogicalPlanNode.parseFrom(bytes);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new DataFusionError("Failed to decode LogicalPlan protobuf", e);
    }
    return fromProto(proto, bridge);
  }

  private static LogicalPlan fromProto(LogicalPlanNode proto, LogicalPlanBridge bridge) {
    return switch (proto.getLogicalPlanTypeCase()) {
      case PROJECTION -> {
        ProjectionNode n = proto.getProjection();
        yield new LogicalPlan.Projection(
            exprsFromProto(n.getExprList()), child(bridge, 0), bridge.schema(), bridge);
      }
      case SELECTION -> {
        SelectionNode n = proto.getSelection();
        Expr predicate = ExprProtoConverter.fromProto(n.getExpr());
        yield new LogicalPlan.Filter(predicate, child(bridge, 0), bridge.schema(), bridge);
      }
      case SORT -> {
        SortNode n = proto.getSort();
        List<SortExpr> sortExprs = new ArrayList<>(n.getExprCount());
        for (SortExprNode s : n.getExprList()) {
          sortExprs.add(sortExprFromProto(s));
        }
        OptionalLong fetch =
            n.getFetch() < 0 ? OptionalLong.empty() : OptionalLong.of(n.getFetch());
        yield new LogicalPlan.Sort(sortExprs, child(bridge, 0), fetch, bridge.schema(), bridge);
      }
      case JOIN -> {
        JoinNode n = proto.getJoin();
        JoinType joinType =
            switch (n.getJoinType()) {
              case INNER -> JoinType.INNER;
              case LEFT -> JoinType.LEFT;
              case RIGHT -> JoinType.RIGHT;
              case FULL -> JoinType.FULL;
              case LEFTSEMI -> JoinType.LEFT_SEMI;
              case LEFTANTI -> JoinType.LEFT_ANTI;
              case RIGHTSEMI -> JoinType.RIGHT_SEMI;
              case RIGHTANTI -> JoinType.RIGHT_ANTI;
              default -> JoinType.INNER;
            };
        JoinConstraint joinConstraint =
            switch (n.getJoinConstraint()) {
              case ON -> JoinConstraint.ON;
              case USING -> JoinConstraint.USING;
              default -> JoinConstraint.ON;
            };
        Optional<Expr> filter =
            n.hasFilter()
                ? Optional.of(ExprProtoConverter.fromProto(n.getFilter()))
                : Optional.empty();
        NullEquality nullEquality =
            switch (n.getNullEquality()) {
              case NULL_EQUALS_NOTHING -> NullEquality.NULL_EQUALS_NOTHING;
              case NULL_EQUALS_NULL -> NullEquality.NULL_EQUALS_NULL;
              default -> NullEquality.NULL_EQUALS_NOTHING;
            };
        yield new LogicalPlan.Join(
            child(bridge, 0),
            child(bridge, 1),
            joinType,
            joinConstraint,
            exprsFromProto(n.getLeftJoinKeyList()),
            exprsFromProto(n.getRightJoinKeyList()),
            filter,
            nullEquality,
            bridge.schema(),
            bridge);
      }
      case AGGREGATE -> {
        AggregateNode n = proto.getAggregate();
        yield new LogicalPlan.Aggregate(
            child(bridge, 0),
            exprsFromProto(n.getGroupExprList()),
            exprsFromProto(n.getAggrExprList()),
            bridge.schema(),
            bridge);
      }
      case WINDOW -> {
        WindowNode n = proto.getWindow();
        yield new LogicalPlan.Window(
            child(bridge, 0), exprsFromProto(n.getWindowExprList()), bridge.schema(), bridge);
      }
      case LIMIT -> {
        LimitNode n = proto.getLimit();
        Optional<Expr> skipExpr =
            n.getSkip() > 0 ? Optional.of(literalInt64(n.getSkip())) : Optional.empty();
        Optional<Expr> fetchExpr =
            n.getFetch() < 0 ? Optional.empty() : Optional.of(literalInt64(n.getFetch()));
        yield new LogicalPlan.Limit(child(bridge, 0), skipExpr, fetchExpr, bridge.schema(), bridge);
      }
      case DISTINCT -> new LogicalPlan.Distinct.All(child(bridge, 0), bridge.schema(), bridge);
      case DISTINCT_ON -> {
        DistinctOnNode n = proto.getDistinctOn();
        List<SortExpr> sorts = new ArrayList<>(n.getSortExprCount());
        for (SortExprNode s : n.getSortExprList()) {
          sorts.add(sortExprFromProto(s));
        }
        Optional<List<SortExpr>> sortOpt = sorts.isEmpty() ? Optional.empty() : Optional.of(sorts);
        yield new LogicalPlan.Distinct.On(
            exprsFromProto(n.getOnExprList()),
            exprsFromProto(n.getSelectExprList()),
            sortOpt,
            child(bridge, 0),
            bridge.schema(),
            bridge);
      }
      case EMPTY_RELATION -> {
        EmptyRelationNode n = proto.getEmptyRelation();
        yield new LogicalPlan.EmptyRelation(n.getProduceOneRow(), bridge.schema(), bridge);
      }
      case SUBQUERY_ALIAS -> {
        SubqueryAliasNode n = proto.getSubqueryAlias();
        yield new LogicalPlan.SubqueryAlias(
            child(bridge, 0), aliasTableName(n), bridge.schema(), bridge);
      }
      case EXPLAIN -> {
        ExplainNode n = proto.getExplain();
        yield new LogicalPlan.Explain(child(bridge, 0), n.getVerbose(), bridge.schema(), bridge);
      }
      case ANALYZE -> {
        AnalyzeNode n = proto.getAnalyze();
        yield new LogicalPlan.Analyze(child(bridge, 0), n.getVerbose(), bridge.schema(), bridge);
      }
      case VALUES -> {
        ValuesNode n = proto.getValues();
        int cols = (int) n.getNCols();
        List<LogicalExprNode> flat = n.getValuesListList();
        List<List<Expr>> rows = new ArrayList<>();
        if (cols > 0 && !flat.isEmpty()) {
          int rowCount = flat.size() / cols;
          for (int r = 0; r < rowCount; r++) {
            List<Expr> row = new ArrayList<>(cols);
            for (int c = 0; c < cols; c++) {
              row.add(ExprProtoConverter.fromProto(flat.get(r * cols + c)));
            }
            rows.add(List.copyOf(row));
          }
        }
        yield new LogicalPlan.Values(List.copyOf(rows), bridge.schema(), bridge);
      }
      case UNION -> {
        int count = bridge.inputsCount();
        List<LogicalPlan> inputs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
          inputs.add(child(bridge, i));
        }
        yield new LogicalPlan.Union(List.copyOf(inputs), bridge.schema(), bridge);
      }
      case RECURSIVE_QUERY -> {
        RecursiveQueryNode n = proto.getRecursiveQuery();
        yield new LogicalPlan.RecursiveQuery(
            n.getName(),
            child(bridge, 0),
            child(bridge, 1),
            n.getIsDistinct(),
            bridge.schema(),
            bridge);
      }
      case REPARTITION -> new LogicalPlan.Repartition(child(bridge, 0), bridge.schema(), bridge);
      case COPY_TO -> new LogicalPlan.Copy(child(bridge, 0), bridge.schema(), bridge);
      case UNNEST -> new LogicalPlan.Unnest(child(bridge, 0), bridge.schema(), bridge);
      case DML -> {
        DmlNode n = proto.getDml();
        TableReference tableName = TableReferenceConverter.convertTableReference(n.getTableName());
        WriteOp op =
            switch (n.getDmlType()) {
              case INSERT_APPEND -> WriteOp.INSERT_APPEND;
              case INSERT_OVERWRITE -> WriteOp.INSERT_OVERWRITE;
              case INSERT_REPLACE -> WriteOp.INSERT_REPLACE;
              case DELETE -> WriteOp.DELETE;
              case UPDATE -> WriteOp.UPDATE;
              case CTAS -> WriteOp.CTAS;
              default -> WriteOp.INSERT_APPEND;
            };
        yield new LogicalPlan.Dml(tableName, op, child(bridge, 0), bridge.schema(), bridge);
      }
      case CREATE_EXTERNAL_TABLE -> {
        CreateExternalTableNode n = proto.getCreateExternalTable();
        yield new LogicalPlan.Ddl.CreateExternalTable(
            TableReferenceConverter.convertTableReference(n.getName()),
            n.getLocation(),
            n.getFileType(),
            n.getIfNotExists(),
            n.getOrReplace(),
            n.getTemporary(),
            bridge.schema(),
            bridge);
      }
      case CREATE_VIEW -> {
        CreateViewNode n = proto.getCreateView();
        Optional<String> definition =
            n.getDefinition().isEmpty() ? Optional.empty() : Optional.of(n.getDefinition());
        yield new LogicalPlan.Ddl.CreateView(
            TableReferenceConverter.convertTableReference(n.getName()),
            n.getOrReplace(),
            definition,
            n.getTemporary(),
            child(bridge, 0),
            bridge.schema(),
            bridge);
      }
      case CREATE_CATALOG_SCHEMA -> {
        CreateCatalogSchemaNode n = proto.getCreateCatalogSchema();
        yield new LogicalPlan.Ddl.CreateCatalogSchema(
            n.getSchemaName(), n.getIfNotExists(), bridge.schema(), bridge);
      }
      case CREATE_CATALOG -> {
        CreateCatalogNode n = proto.getCreateCatalog();
        yield new LogicalPlan.Ddl.CreateCatalog(
            n.getCatalogName(), n.getIfNotExists(), bridge.schema(), bridge);
      }
      case DROP_VIEW -> {
        DropViewNode n = proto.getDropView();
        yield new LogicalPlan.Ddl.DropView(
            TableReferenceConverter.convertTableReference(n.getName()),
            n.getIfExists(),
            bridge.schema(),
            bridge);
      }
        // TableScan variants: the proto is lossy (column names vs indices, no fetch) — keep
        // using the variant-specific bridge accessors.
      case LISTING_SCAN, VIEW_SCAN, CUSTOM_SCAN, CTE_WORK_TABLE_SCAN ->
          LogicalPlan.fromBridgeTableScan(bridge);
      default -> LogicalPlan.fromBridgeUnsupported(bridge);
    };
  }

  private static LogicalPlan child(LogicalPlanBridge bridge, int idx) {
    return LogicalPlan.fromBridge(bridge.inputAt(idx));
  }

  private static List<Expr> exprsFromProto(List<LogicalExprNode> list) {
    List<Expr> out = new ArrayList<>(list.size());
    for (LogicalExprNode n : list) {
      out.add(ExprProtoConverter.fromProto(n));
    }
    return out;
  }

  private static SortExpr sortExprFromProto(SortExprNode proto) {
    return new SortExpr(
        ExprProtoConverter.fromProto(proto.getExpr()), proto.getAsc(), proto.getNullsFirst());
  }

  private static String aliasTableName(SubqueryAliasNode n) {
    var alias = n.getAlias();
    return switch (alias.getTableReferenceEnumCase()) {
      case BARE -> alias.getBare().getTable();
      case PARTIAL -> alias.getPartial().getTable();
      case FULL -> alias.getFull().getTable();
      default -> "";
    };
  }

  private static Expr literalInt64(long value) {
    return new Expr.LiteralExpr(new ScalarValue.Int64(value));
  }
}
