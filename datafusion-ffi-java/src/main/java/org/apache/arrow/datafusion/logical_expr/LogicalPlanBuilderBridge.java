package org.apache.arrow.datafusion.logical_expr;

import java.util.List;
import org.apache.arrow.datafusion.ExprProtoConverter;
import org.apache.arrow.datafusion.common.BridgeUtil;
import org.apache.arrow.datafusion.generated.DfJoinType;
import org.apache.arrow.datafusion.generated.DfLogicalPlanBuilder;
import org.apache.arrow.datafusion.generated.DfSessionContext;
import org.apache.arrow.datafusion.proto.SortExprNodeCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public LogicalPlanBuilder API and Diplomat-generated DfLogicalPlanBuilder.
 *
 * <p>Delegates to the Diplomat-generated class for all native calls.
 */
public final class LogicalPlanBuilderBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(LogicalPlanBuilderBridge.class);

  private final DfLogicalPlanBuilder dfBuilder;
  private volatile boolean closed = false;

  public LogicalPlanBuilderBridge(DfLogicalPlanBuilder dfBuilder) {
    this.dfBuilder = dfBuilder;
  }

  static LogicalPlanBuilderBridge createEmpty(DfSessionContext dfCtx, boolean produceOneRow) {
    return BridgeUtil.unwrap(
        "Failed to create empty plan builder",
        () -> new LogicalPlanBuilderBridge(DfLogicalPlanBuilder.empty(dfCtx, produceOneRow)));
  }

  static LogicalPlanBuilderBridge createFromPlan(DfSessionContext dfCtx, LogicalPlanBridge plan) {
    return BridgeUtil.unwrap(
        "Failed to create plan builder from plan",
        () -> new LogicalPlanBuilderBridge(DfLogicalPlanBuilder.fromPlan(dfCtx, plan.dfPlan())));
  }

  LogicalPlanBuilderBridge project(List<Expr> exprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(exprs);
    return BridgeUtil.unwrap(
        "Failed to project", () -> new LogicalPlanBuilderBridge(dfBuilder.project(bytes)));
  }

  LogicalPlanBuilderBridge filter(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    return BridgeUtil.unwrap(
        "Failed to filter", () -> new LogicalPlanBuilderBridge(dfBuilder.filter(bytes)));
  }

  LogicalPlanBuilderBridge sort(List<SortExpr> sortExprs) {
    checkNotClosed();
    byte[] bytes = sortExprToProtoBytes(sortExprs);
    return BridgeUtil.unwrap(
        "Failed to sort", () -> new LogicalPlanBuilderBridge(dfBuilder.sort(bytes)));
  }

  LogicalPlanBuilderBridge limit(long skip, long fetch) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to limit", () -> new LogicalPlanBuilderBridge(dfBuilder.limit(skip, fetch)));
  }

  LogicalPlanBuilderBridge aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    checkNotClosed();
    byte[] groupBytes = ExprProtoConverter.toProtoBytes(groupExprs);
    byte[] aggrBytes = ExprProtoConverter.toProtoBytes(aggrExprs);
    return BridgeUtil.unwrap(
        "Failed to aggregate",
        () -> new LogicalPlanBuilderBridge(dfBuilder.aggregate(groupBytes, aggrBytes)));
  }

  LogicalPlanBuilderBridge distinct() {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to distinct", () -> new LogicalPlanBuilderBridge(dfBuilder.distinct()));
  }

  LogicalPlanBuilderBridge having(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    return BridgeUtil.unwrap(
        "Failed to apply having", () -> new LogicalPlanBuilderBridge(dfBuilder.having(bytes)));
  }

  LogicalPlanBuilderBridge window(List<Expr> windowExprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(windowExprs);
    return BridgeUtil.unwrap(
        "Failed to apply window", () -> new LogicalPlanBuilderBridge(dfBuilder.window(bytes)));
  }

  LogicalPlanBuilderBridge alias(String name) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to apply alias", () -> new LogicalPlanBuilderBridge(dfBuilder.alias(name)));
  }

  LogicalPlanBuilderBridge explain(boolean verbose, boolean analyze) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to explain",
        () -> new LogicalPlanBuilderBridge(dfBuilder.explain(verbose, analyze)));
  }

  LogicalPlanBuilderBridge join(
      LogicalPlanBridge right, JoinType joinType, List<String> leftCols, List<String> rightCols) {
    checkNotClosed();
    String[] leftArr = leftCols.toArray(new String[0]);
    String[] rightArr = rightCols.toArray(new String[0]);
    return BridgeUtil.unwrap(
        "Failed to join",
        () ->
            new LogicalPlanBuilderBridge(
                dfBuilder.join(right.dfPlan(), joinTypeToDfJoinType(joinType), leftArr, rightArr)));
  }

  LogicalPlanBuilderBridge crossJoin(LogicalPlanBridge right) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to cross join",
        () -> new LogicalPlanBuilderBridge(dfBuilder.crossJoin(right.dfPlan())));
  }

  LogicalPlanBuilderBridge union(LogicalPlanBridge other) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to union", () -> new LogicalPlanBuilderBridge(dfBuilder.union(other.dfPlan())));
  }

  LogicalPlanBuilderBridge unionDistinct(LogicalPlanBridge other) {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to union distinct",
        () -> new LogicalPlanBuilderBridge(dfBuilder.unionDistinct(other.dfPlan())));
  }

  LogicalPlanBridge build() {
    checkNotClosed();
    return BridgeUtil.unwrap(
        "Failed to build logical plan", () -> new LogicalPlanBridge(dfBuilder.build()));
  }

  static LogicalPlanBridge intersect(
      LogicalPlanBridge left, LogicalPlanBridge right, boolean isAll) {
    return BridgeUtil.unwrap(
        "Failed to intersect",
        () ->
            new LogicalPlanBridge(
                DfLogicalPlanBuilder.intersect(left.dfPlan(), right.dfPlan(), isAll)));
  }

  static LogicalPlanBridge except(LogicalPlanBridge left, LogicalPlanBridge right, boolean isAll) {
    return BridgeUtil.unwrap(
        "Failed to except",
        () ->
            new LogicalPlanBridge(
                DfLogicalPlanBuilder.except(left.dfPlan(), right.dfPlan(), isAll)));
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
      throw new IllegalStateException("LogicalPlanBuilder has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      dfBuilder.close();
      logger.debug("Closed LogicalPlanBuilder");
    }
  }
}
