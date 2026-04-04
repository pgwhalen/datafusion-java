package org.apache.arrow.datafusion.logical_expr;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.datafusion.ExprProtoConverter;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfJoinType;
import org.apache.arrow.datafusion.generated.DfLogicalPlan;
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
    try {
      return new LogicalPlanBuilderBridge(DfLogicalPlanBuilder.empty(dfCtx, produceOneRow));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to create empty plan builder", e);
    }
  }

  static LogicalPlanBuilderBridge createFromPlan(DfSessionContext dfCtx, LogicalPlanBridge plan) {
    try {
      return new LogicalPlanBuilderBridge(DfLogicalPlanBuilder.fromPlan(dfCtx, plan.dfPlan()));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to create plan builder from plan", e);
    }
  }

  LogicalPlanBuilderBridge project(List<Expr> exprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(exprs);
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.project(bytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to project", e);
    }
  }

  LogicalPlanBuilderBridge filter(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.filter(bytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to filter", e);
    }
  }

  LogicalPlanBuilderBridge sort(List<SortExpr> sortExprs) {
    checkNotClosed();
    byte[] bytes = sortExprToProtoBytes(sortExprs);
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.sort(bytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to sort", e);
    }
  }

  LogicalPlanBuilderBridge limit(long skip, long fetch) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.limit(skip, fetch));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to limit", e);
    }
  }

  LogicalPlanBuilderBridge aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
    checkNotClosed();
    byte[] groupBytes = ExprProtoConverter.toProtoBytes(groupExprs);
    byte[] aggrBytes = ExprProtoConverter.toProtoBytes(aggrExprs);
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.aggregate(groupBytes, aggrBytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to aggregate", e);
    }
  }

  LogicalPlanBuilderBridge distinct() {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.distinct());
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to distinct", e);
    }
  }

  LogicalPlanBuilderBridge having(Expr predicate) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(List.of(predicate));
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.having(bytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to apply having", e);
    }
  }

  LogicalPlanBuilderBridge window(List<Expr> windowExprs) {
    checkNotClosed();
    byte[] bytes = ExprProtoConverter.toProtoBytes(windowExprs);
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.window(bytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to apply window", e);
    }
  }

  LogicalPlanBuilderBridge alias(String name) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.alias(name));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to apply alias", e);
    }
  }

  LogicalPlanBuilderBridge explain(boolean verbose, boolean analyze) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.explain(verbose, analyze));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to explain", e);
    }
  }

  LogicalPlanBuilderBridge join(
      LogicalPlanBridge right, JoinType joinType, List<String> leftCols, List<String> rightCols) {
    checkNotClosed();
    byte[] leftBytes = encodeNullSeparated(leftCols.toArray(new String[0]));
    byte[] rightBytes = encodeNullSeparated(rightCols.toArray(new String[0]));
    try {
      return new LogicalPlanBuilderBridge(
          dfBuilder.join(right.dfPlan(), joinTypeToDfJoinType(joinType), leftBytes, rightBytes));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to join", e);
    }
  }

  LogicalPlanBuilderBridge crossJoin(LogicalPlanBridge right) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.crossJoin(right.dfPlan()));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to cross join", e);
    }
  }

  LogicalPlanBuilderBridge union(LogicalPlanBridge other) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.union(other.dfPlan()));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to union", e);
    }
  }

  LogicalPlanBuilderBridge unionDistinct(LogicalPlanBridge other) {
    checkNotClosed();
    try {
      return new LogicalPlanBuilderBridge(dfBuilder.unionDistinct(other.dfPlan()));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to union distinct", e);
    }
  }

  LogicalPlanBridge build() {
    checkNotClosed();
    try {
      DfLogicalPlan plan = dfBuilder.build();
      return new LogicalPlanBridge(plan);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to build logical plan", e);
    }
  }

  static LogicalPlanBridge intersect(
      LogicalPlanBridge left, LogicalPlanBridge right, boolean isAll) {
    try {
      DfLogicalPlan plan = DfLogicalPlanBuilder.intersect(left.dfPlan(), right.dfPlan(), isAll);
      return new LogicalPlanBridge(plan);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to intersect", e);
    }
  }

  static LogicalPlanBridge except(LogicalPlanBridge left, LogicalPlanBridge right, boolean isAll) {
    try {
      DfLogicalPlan plan = DfLogicalPlanBuilder.except(left.dfPlan(), right.dfPlan(), isAll);
      return new LogicalPlanBridge(plan);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to except", e);
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
