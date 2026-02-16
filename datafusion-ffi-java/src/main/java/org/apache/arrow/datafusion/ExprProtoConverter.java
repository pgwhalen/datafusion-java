package org.apache.arrow.datafusion;

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.datafusion.proto.AggregateUDFExprNode;
import org.apache.arrow.datafusion.proto.AliasNode;
import org.apache.arrow.datafusion.proto.BetweenNode;
import org.apache.arrow.datafusion.proto.BinaryExprNode;
import org.apache.arrow.datafusion.proto.CaseNode;
import org.apache.arrow.datafusion.proto.CastNode;
import org.apache.arrow.datafusion.proto.CubeNode;
import org.apache.arrow.datafusion.proto.GroupingSetNode;
import org.apache.arrow.datafusion.proto.ILikeNode;
import org.apache.arrow.datafusion.proto.InListNode;
import org.apache.arrow.datafusion.proto.LikeNode;
import org.apache.arrow.datafusion.proto.LogicalExprList;
import org.apache.arrow.datafusion.proto.LogicalExprNode;
import org.apache.arrow.datafusion.proto.NegativeNode;
import org.apache.arrow.datafusion.proto.PlaceholderNode;
import org.apache.arrow.datafusion.proto.RollupNode;
import org.apache.arrow.datafusion.proto.ScalarUDFExprNode;
import org.apache.arrow.datafusion.proto.SimilarToNode;
import org.apache.arrow.datafusion.proto.SortExprNode;
import org.apache.arrow.datafusion.proto.TryCastNode;
import org.apache.arrow.datafusion.proto.WindowExprNode;

/**
 * Converts between proto {@code LogicalExprNode}/{@code LogicalExprList} messages and rich Java
 * {@link Expr} records.
 *
 * <p>The proto wire format matches what {@code datafusion-proto} produces, so these bytes can be
 * decoded directly in Java without a Rust round-trip.
 */
final class ExprProtoConverter {
  private ExprProtoConverter() {}

  /** Decode a proto-encoded {@code LogicalExprList} into a list of {@link Expr}. */
  static List<Expr> fromProtoBytes(byte[] bytes) {
    try {
      LogicalExprList list = LogicalExprList.parseFrom(bytes);
      List<Expr> result = new ArrayList<>(list.getExprCount());
      for (LogicalExprNode node : list.getExprList()) {
        result.add(fromProto(node));
      }
      return result;
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new DataFusionException("Failed to decode expression protobuf", e);
    }
  }

  /** Encode a list of {@link Expr} into proto bytes ({@code LogicalExprList}). */
  static byte[] toProtoBytes(List<Expr> exprs) {
    LogicalExprList.Builder builder = LogicalExprList.newBuilder();
    for (Expr expr : exprs) {
      builder.addExpr(toProto(expr));
    }
    return builder.build().toByteArray();
  }

  /** Convert a single proto {@code LogicalExprNode} to a Java {@link Expr}. */
  static Expr fromProto(LogicalExprNode proto) {
    if (proto == null || proto.getExprTypeCase() == LogicalExprNode.ExprTypeCase.EXPRTYPE_NOT_SET) {
      return new Expr.UnresolvedExpr(proto != null ? proto.toByteArray() : new byte[0]);
    }
    return switch (proto.getExprTypeCase()) {
      case COLUMN -> {
        var col = proto.getColumn();
        TableReference relation = null;
        if (col.hasRelation()) {
          relation = new TableReference.Bare(col.getRelation().getRelation());
        }
        yield new Expr.ColumnExpr(new Column(col.getName(), relation, null));
      }
      case ALIAS -> {
        AliasNode alias = proto.getAlias();
        List<TableReference> relations = new ArrayList<>();
        for (var tr : alias.getRelationList()) {
          relations.add(convertTableReference(tr));
        }
        yield new Expr.AliasExpr(fromProto(alias.getExpr()), alias.getAlias(), relations);
      }
      case LITERAL -> new Expr.LiteralExpr(ScalarValueProtoConverter.fromProto(proto.getLiteral()));
      case BINARY_EXPR -> {
        BinaryExprNode bin = proto.getBinaryExpr();
        Operator op = operatorFromString(bin.getOp());
        List<LogicalExprNode> operands = bin.getOperandsList();
        if (operands.size() == 2) {
          yield new Expr.BinaryExpr(fromProto(operands.get(0)), op, fromProto(operands.get(1)));
        } else if (operands.size() > 2) {
          // Fold right-associatively
          Expr result = fromProto(operands.get(operands.size() - 1));
          for (int i = operands.size() - 2; i >= 0; i--) {
            result = new Expr.BinaryExpr(fromProto(operands.get(i)), op, result);
          }
          yield result;
        } else {
          yield new Expr.UnresolvedExpr(proto.toByteArray());
        }
      }
      case IS_NULL_EXPR -> new Expr.IsNullExpr(fromProto(proto.getIsNullExpr().getExpr()));
      case IS_NOT_NULL_EXPR ->
          new Expr.IsNotNullExpr(fromProto(proto.getIsNotNullExpr().getExpr()));
      case NOT_EXPR -> new Expr.NotExpr(fromProto(proto.getNotExpr().getExpr()));
      case IS_TRUE -> new Expr.IsTrueExpr(fromProto(proto.getIsTrue().getExpr()));
      case IS_FALSE -> new Expr.IsFalseExpr(fromProto(proto.getIsFalse().getExpr()));
      case IS_UNKNOWN -> new Expr.IsUnknownExpr(fromProto(proto.getIsUnknown().getExpr()));
      case IS_NOT_TRUE -> new Expr.IsNotTrueExpr(fromProto(proto.getIsNotTrue().getExpr()));
      case IS_NOT_FALSE -> new Expr.IsNotFalseExpr(fromProto(proto.getIsNotFalse().getExpr()));
      case IS_NOT_UNKNOWN ->
          new Expr.IsNotUnknownExpr(fromProto(proto.getIsNotUnknown().getExpr()));
      case BETWEEN -> {
        BetweenNode b = proto.getBetween();
        yield new Expr.BetweenExpr(
            fromProto(b.getExpr()), b.getNegated(), fromProto(b.getLow()), fromProto(b.getHigh()));
      }
      case CASE_ -> {
        CaseNode c = proto.getCase();
        Expr matchExpr = c.hasExpr() ? fromProto(c.getExpr()) : null;
        List<WhenThen> whenThens = new ArrayList<>();
        for (var wt : c.getWhenThenExprList()) {
          whenThens.add(new WhenThen(fromProto(wt.getWhenExpr()), fromProto(wt.getThenExpr())));
        }
        Expr elseExpr = c.hasElseExpr() ? fromProto(c.getElseExpr()) : null;
        yield new Expr.CaseExpr(matchExpr, whenThens, elseExpr);
      }
      case CAST -> {
        CastNode cast = proto.getCast();
        yield new Expr.CastExpr(
            fromProto(cast.getExpr()), ArrowTypeProtoConverter.fromProto(cast.getArrowType()));
      }
      case TRY_CAST -> {
        TryCastNode tc = proto.getTryCast();
        yield new Expr.TryCastExpr(
            fromProto(tc.getExpr()), ArrowTypeProtoConverter.fromProto(tc.getArrowType()));
      }
      case NEGATIVE -> {
        NegativeNode neg = proto.getNegative();
        yield new Expr.NegativeExpr(fromProto(neg.getExpr()));
      }
      case IN_LIST -> {
        InListNode inList = proto.getInList();
        List<Expr> list = new ArrayList<>();
        for (var e : inList.getListList()) {
          list.add(fromProto(e));
        }
        yield new Expr.InListExpr(fromProto(inList.getExpr()), list, inList.getNegated());
      }
      case LIKE -> {
        LikeNode like = proto.getLike();
        Character esc = like.getEscapeChar().isEmpty() ? null : like.getEscapeChar().charAt(0);
        yield new Expr.LikeExpr(
            like.getNegated(), fromProto(like.getExpr()), fromProto(like.getPattern()), esc, false);
      }
      case ILIKE -> {
        ILikeNode ilike = proto.getIlike();
        Character esc = ilike.getEscapeChar().isEmpty() ? null : ilike.getEscapeChar().charAt(0);
        yield new Expr.LikeExpr(
            ilike.getNegated(),
            fromProto(ilike.getExpr()),
            fromProto(ilike.getPattern()),
            esc,
            true);
      }
      case SIMILAR_TO -> {
        SimilarToNode st = proto.getSimilarTo();
        Character esc = st.getEscapeChar().isEmpty() ? null : st.getEscapeChar().charAt(0);
        yield new Expr.SimilarToExpr(
            st.getNegated(), fromProto(st.getExpr()), fromProto(st.getPattern()), esc);
      }
      case SCALAR_UDF_EXPR -> {
        ScalarUDFExprNode udf = proto.getScalarUdfExpr();
        List<Expr> args = new ArrayList<>();
        for (var a : udf.getArgsList()) {
          args.add(fromProto(a));
        }
        yield new Expr.ScalarFunctionExpr(udf.getFunName(), args);
      }
      case AGGREGATE_UDF_EXPR -> {
        AggregateUDFExprNode agg = proto.getAggregateUdfExpr();
        List<Expr> args = new ArrayList<>();
        for (var a : agg.getArgsList()) {
          args.add(fromProto(a));
        }
        Expr filter = agg.hasFilter() ? fromProto(agg.getFilter()) : null;
        List<SortExpr> orderBy = new ArrayList<>();
        for (var s : agg.getOrderByList()) {
          orderBy.add(convertSortExpr(s));
        }
        NullTreatment nt =
            agg.hasNullTreatment() ? convertNullTreatment(agg.getNullTreatment()) : null;
        yield new Expr.AggregateFunctionExpr(
            agg.getFunName(), args, agg.getDistinct(), filter, orderBy, nt);
      }
      case WINDOW_EXPR -> {
        WindowExprNode win = proto.getWindowExpr();
        String funcName = win.hasUdaf() ? win.getUdaf() : win.hasUdwf() ? win.getUdwf() : "unknown";
        List<Expr> args = new ArrayList<>();
        for (var a : win.getExprsList()) {
          args.add(fromProto(a));
        }
        List<Expr> partitionBy = new ArrayList<>();
        for (var p : win.getPartitionByList()) {
          partitionBy.add(fromProto(p));
        }
        List<SortExpr> orderBy = new ArrayList<>();
        for (var s : win.getOrderByList()) {
          orderBy.add(convertSortExpr(s));
        }
        WindowFrame frame = win.hasWindowFrame() ? convertWindowFrame(win.getWindowFrame()) : null;
        NullTreatment nt =
            win.hasNullTreatment() ? convertNullTreatment(win.getNullTreatment()) : null;
        yield new Expr.WindowFunctionExpr(funcName, args, partitionBy, orderBy, frame, nt);
      }
      case WILDCARD -> {
        var w = proto.getWildcard();
        TableReference qualifier =
            w.hasQualifier() ? convertTableReference(w.getQualifier()) : null;
        yield new Expr.WildcardExpr(qualifier);
      }
      case GROUPING_SET -> {
        GroupingSetNode gs = proto.getGroupingSet();
        List<List<Expr>> groups = new ArrayList<>();
        for (var exprList : gs.getExprList()) {
          List<Expr> group = new ArrayList<>();
          for (var e : exprList.getExprList()) {
            group.add(fromProto(e));
          }
          groups.add(group);
        }
        yield new Expr.GroupingSetExpr(GroupingSetKind.GROUP_BY, groups);
      }
      case CUBE -> {
        CubeNode cube = proto.getCube();
        List<List<Expr>> groups = new ArrayList<>();
        for (var e : cube.getExprList()) {
          groups.add(List.of(fromProto(e)));
        }
        yield new Expr.GroupingSetExpr(GroupingSetKind.CUBE, groups);
      }
      case ROLLUP -> {
        RollupNode rollup = proto.getRollup();
        List<List<Expr>> groups = new ArrayList<>();
        for (var e : rollup.getExprList()) {
          groups.add(List.of(fromProto(e)));
        }
        yield new Expr.GroupingSetExpr(GroupingSetKind.ROLLUP, groups);
      }
      case PLACEHOLDER -> {
        PlaceholderNode ph = proto.getPlaceholder();
        var dataType =
            ph.hasDataType() ? ArrowTypeProtoConverter.fromProto(ph.getDataType()) : null;
        yield new Expr.PlaceholderExpr(ph.getId(), dataType);
      }
      case UNNEST -> {
        var unnest = proto.getUnnest();
        List<Expr> exprs = new ArrayList<>();
        for (var e : unnest.getExprsList()) {
          exprs.add(fromProto(e));
        }
        yield new Expr.UnnestExpr(exprs);
      }
      default -> new Expr.UnresolvedExpr(proto.toByteArray());
    };
  }

  /** Convert a Java {@link Expr} back to a proto {@code LogicalExprNode}. */
  static LogicalExprNode toProto(Expr expr) {
    LogicalExprNode.Builder builder = LogicalExprNode.newBuilder();
    switch (expr) {
      case Expr.ColumnExpr e -> {
        var colBuilder =
            org.apache.arrow.datafusion.proto.Column.newBuilder().setName(e.column().name());
        if (e.column().relation() != null) {
          colBuilder.setRelation(
              org.apache.arrow.datafusion.proto.ColumnRelation.newBuilder()
                  .setRelation(getRelationString(e.column().relation())));
        }
        builder.setColumn(colBuilder);
      }
      case Expr.AliasExpr e -> {
        var aliasBuilder = AliasNode.newBuilder().setExpr(toProto(e.expr())).setAlias(e.alias());
        if (e.relation() != null) {
          for (var tr : e.relation()) {
            aliasBuilder.addRelation(toProtoTableReference(tr));
          }
        }
        builder.setAlias(aliasBuilder);
      }
      case Expr.LiteralExpr e -> builder.setLiteral(ScalarValueProtoConverter.toProto(e.value()));
      case Expr.BinaryExpr e ->
          builder.setBinaryExpr(
              BinaryExprNode.newBuilder()
                  .addOperands(toProto(e.left()))
                  .addOperands(toProto(e.right()))
                  .setOp(operatorToString(e.op())));
      case Expr.IsNullExpr e ->
          builder.setIsNullExpr(
              org.apache.arrow.datafusion.proto.IsNull.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsNotNullExpr e ->
          builder.setIsNotNullExpr(
              org.apache.arrow.datafusion.proto.IsNotNull.newBuilder().setExpr(toProto(e.expr())));
      case Expr.NotExpr e ->
          builder.setNotExpr(
              org.apache.arrow.datafusion.proto.Not.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsTrueExpr e ->
          builder.setIsTrue(
              org.apache.arrow.datafusion.proto.IsTrue.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsFalseExpr e ->
          builder.setIsFalse(
              org.apache.arrow.datafusion.proto.IsFalse.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsUnknownExpr e ->
          builder.setIsUnknown(
              org.apache.arrow.datafusion.proto.IsUnknown.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsNotTrueExpr e ->
          builder.setIsNotTrue(
              org.apache.arrow.datafusion.proto.IsNotTrue.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsNotFalseExpr e ->
          builder.setIsNotFalse(
              org.apache.arrow.datafusion.proto.IsNotFalse.newBuilder().setExpr(toProto(e.expr())));
      case Expr.IsNotUnknownExpr e ->
          builder.setIsNotUnknown(
              org.apache.arrow.datafusion.proto.IsNotUnknown.newBuilder()
                  .setExpr(toProto(e.expr())));
      case Expr.NegativeExpr e ->
          builder.setNegative(NegativeNode.newBuilder().setExpr(toProto(e.expr())));
      case Expr.BetweenExpr e ->
          builder.setBetween(
              BetweenNode.newBuilder()
                  .setExpr(toProto(e.expr()))
                  .setNegated(e.negated())
                  .setLow(toProto(e.low()))
                  .setHigh(toProto(e.high())));
      case Expr.CaseExpr e -> {
        var caseBuilder = CaseNode.newBuilder();
        if (e.expr() != null) caseBuilder.setExpr(toProto(e.expr()));
        for (var wt : e.whenThen()) {
          caseBuilder.addWhenThenExpr(
              org.apache.arrow.datafusion.proto.WhenThen.newBuilder()
                  .setWhenExpr(toProto(wt.when()))
                  .setThenExpr(toProto(wt.then())));
        }
        if (e.elseExpr() != null) caseBuilder.setElseExpr(toProto(e.elseExpr()));
        builder.setCase(caseBuilder);
      }
      case Expr.CastExpr e ->
          builder.setCast(
              CastNode.newBuilder()
                  .setExpr(toProto(e.expr()))
                  .setArrowType(ArrowTypeProtoConverter.toProto(e.dataType())));
      case Expr.TryCastExpr e ->
          builder.setTryCast(
              TryCastNode.newBuilder()
                  .setExpr(toProto(e.expr()))
                  .setArrowType(ArrowTypeProtoConverter.toProto(e.dataType())));
      case Expr.LikeExpr e -> {
        if (e.caseInsensitive()) {
          var ilikeBuilder =
              ILikeNode.newBuilder()
                  .setNegated(e.negated())
                  .setExpr(toProto(e.expr()))
                  .setPattern(toProto(e.pattern()));
          if (e.escapeChar() != null) ilikeBuilder.setEscapeChar(String.valueOf(e.escapeChar()));
          builder.setIlike(ilikeBuilder);
        } else {
          var likeBuilder =
              LikeNode.newBuilder()
                  .setNegated(e.negated())
                  .setExpr(toProto(e.expr()))
                  .setPattern(toProto(e.pattern()));
          if (e.escapeChar() != null) likeBuilder.setEscapeChar(String.valueOf(e.escapeChar()));
          builder.setLike(likeBuilder);
        }
      }
      case Expr.SimilarToExpr e -> {
        var stBuilder =
            SimilarToNode.newBuilder()
                .setNegated(e.negated())
                .setExpr(toProto(e.expr()))
                .setPattern(toProto(e.pattern()));
        if (e.escapeChar() != null) stBuilder.setEscapeChar(String.valueOf(e.escapeChar()));
        builder.setSimilarTo(stBuilder);
      }
      case Expr.ScalarFunctionExpr e -> {
        var udfBuilder = ScalarUDFExprNode.newBuilder().setFunName(e.funcName());
        for (var a : e.args()) {
          udfBuilder.addArgs(toProto(a));
        }
        builder.setScalarUdfExpr(udfBuilder);
      }
      case Expr.AggregateFunctionExpr e -> {
        var aggBuilder =
            AggregateUDFExprNode.newBuilder().setFunName(e.funcName()).setDistinct(e.distinct());
        for (var a : e.args()) {
          aggBuilder.addArgs(toProto(a));
        }
        if (e.filter() != null) aggBuilder.setFilter(toProto(e.filter()));
        for (var s : e.orderBy()) {
          aggBuilder.addOrderBy(toProtoSortExpr(s));
        }
        if (e.nullTreatment() != null)
          aggBuilder.setNullTreatment(toProtoNullTreatment(e.nullTreatment()));
        builder.setAggregateUdfExpr(aggBuilder);
      }
      case Expr.WindowFunctionExpr e -> {
        var winBuilder = WindowExprNode.newBuilder().setUdwf(e.funcName());
        for (var a : e.args()) {
          winBuilder.addExprs(toProto(a));
        }
        for (var p : e.partitionBy()) {
          winBuilder.addPartitionBy(toProto(p));
        }
        for (var s : e.orderBy()) {
          winBuilder.addOrderBy(toProtoSortExpr(s));
        }
        if (e.windowFrame() != null) winBuilder.setWindowFrame(toProtoWindowFrame(e.windowFrame()));
        if (e.nullTreatment() != null)
          winBuilder.setNullTreatment(toProtoNullTreatment(e.nullTreatment()));
        builder.setWindowExpr(winBuilder);
      }
      case Expr.InListExpr e -> {
        var inBuilder = InListNode.newBuilder().setExpr(toProto(e.expr())).setNegated(e.negated());
        for (var item : e.list()) {
          inBuilder.addList(toProto(item));
        }
        builder.setInList(inBuilder);
      }
      case Expr.WildcardExpr e -> {
        var wBuilder = org.apache.arrow.datafusion.proto.Wildcard.newBuilder();
        if (e.qualifier() != null) wBuilder.setQualifier(toProtoTableReference(e.qualifier()));
        builder.setWildcard(wBuilder);
      }
      case Expr.GroupingSetExpr e -> {
        switch (e.kind()) {
          case CUBE -> {
            var cubeBuilder = CubeNode.newBuilder();
            for (var group : e.groups()) {
              for (var ge : group) {
                cubeBuilder.addExpr(toProto(ge));
              }
            }
            builder.setCube(cubeBuilder);
          }
          case ROLLUP -> {
            var rollupBuilder = RollupNode.newBuilder();
            for (var group : e.groups()) {
              for (var ge : group) {
                rollupBuilder.addExpr(toProto(ge));
              }
            }
            builder.setRollup(rollupBuilder);
          }
          case GROUP_BY -> {
            var gsBuilder = GroupingSetNode.newBuilder();
            for (var group : e.groups()) {
              var listBuilder = LogicalExprList.newBuilder();
              for (var ge : group) {
                listBuilder.addExpr(toProto(ge));
              }
              gsBuilder.addExpr(listBuilder);
            }
            builder.setGroupingSet(gsBuilder);
          }
        }
      }
      case Expr.PlaceholderExpr e -> {
        var phBuilder = PlaceholderNode.newBuilder().setId(e.id());
        if (e.dataType() != null)
          phBuilder.setDataType(ArrowTypeProtoConverter.toProto(e.dataType()));
        builder.setPlaceholder(phBuilder);
      }
      case Expr.UnnestExpr e -> {
        var unnestBuilder = org.apache.arrow.datafusion.proto.Unnest.newBuilder();
        for (var ex : e.exprs()) {
          unnestBuilder.addExprs(toProto(ex));
        }
        builder.setUnnest(unnestBuilder);
      }
      case Expr.UnresolvedExpr e -> {
        // Try to parse back the raw bytes
        try {
          return LogicalExprNode.parseFrom(e.protoBytes());
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
          // Return an empty node
          return builder.build();
        }
      }
    }
    return builder.build();
  }

  // ======== Helper methods ========

  private static Operator operatorFromString(String op) {
    return switch (op) {
      case "Eq" -> Operator.Eq;
      case "NotEq" -> Operator.NotEq;
      case "Lt" -> Operator.Lt;
      case "LtEq" -> Operator.LtEq;
      case "Gt" -> Operator.Gt;
      case "GtEq" -> Operator.GtEq;
      case "Plus" -> Operator.Plus;
      case "Minus" -> Operator.Minus;
      case "Multiply" -> Operator.Multiply;
      case "Divide" -> Operator.Divide;
      case "Modulo" -> Operator.Modulo;
      case "And" -> Operator.And;
      case "Or" -> Operator.Or;
      case "IsDistinctFrom" -> Operator.IsDistinctFrom;
      case "IsNotDistinctFrom" -> Operator.IsNotDistinctFrom;
      case "RegexMatch" -> Operator.RegexMatch;
      case "RegexIMatch" -> Operator.RegexIMatch;
      case "RegexNotMatch" -> Operator.RegexNotMatch;
      case "RegexNotIMatch" -> Operator.RegexNotIMatch;
      case "LikeMatch" -> Operator.LikeMatch;
      case "ILikeMatch" -> Operator.ILikeMatch;
      case "NotLikeMatch" -> Operator.NotLikeMatch;
      case "NotILikeMatch" -> Operator.NotILikeMatch;
      case "BitwiseAnd" -> Operator.BitwiseAnd;
      case "BitwiseOr" -> Operator.BitwiseOr;
      case "BitwiseXor" -> Operator.BitwiseXor;
      case "BitwiseShiftRight" -> Operator.BitwiseShiftRight;
      case "BitwiseShiftLeft" -> Operator.BitwiseShiftLeft;
      case "StringConcat" -> Operator.StringConcat;
      case "AtArrow" -> Operator.AtArrow;
      case "ArrowAt" -> Operator.ArrowAt;
      default -> throw new DataFusionException("Unknown operator: " + op);
    };
  }

  private static String operatorToString(Operator op) {
    return op.name();
  }

  private static SortExpr convertSortExpr(SortExprNode proto) {
    return new SortExpr(fromProto(proto.getExpr()), proto.getAsc(), proto.getNullsFirst());
  }

  private static SortExprNode toProtoSortExpr(SortExpr sortExpr) {
    return SortExprNode.newBuilder()
        .setExpr(toProto(sortExpr.expr()))
        .setAsc(sortExpr.asc())
        .setNullsFirst(sortExpr.nullsFirst())
        .build();
  }

  private static NullTreatment convertNullTreatment(
      org.apache.arrow.datafusion.proto.NullTreatment proto) {
    return switch (proto) {
      case RESPECT_NULLS -> NullTreatment.RESPECT_NULLS;
      case IGNORE_NULLS -> NullTreatment.IGNORE_NULLS;
      default -> NullTreatment.RESPECT_NULLS;
    };
  }

  private static org.apache.arrow.datafusion.proto.NullTreatment toProtoNullTreatment(
      NullTreatment nt) {
    return switch (nt) {
      case RESPECT_NULLS -> org.apache.arrow.datafusion.proto.NullTreatment.RESPECT_NULLS;
      case IGNORE_NULLS -> org.apache.arrow.datafusion.proto.NullTreatment.IGNORE_NULLS;
    };
  }

  private static WindowFrame convertWindowFrame(
      org.apache.arrow.datafusion.proto.WindowFrame proto) {
    WindowFrameType frameType =
        switch (proto.getWindowFrameUnits()) {
          case ROWS -> WindowFrameType.ROWS;
          case RANGE -> WindowFrameType.RANGE;
          case GROUPS -> WindowFrameType.GROUPS;
          default -> WindowFrameType.ROWS;
        };
    WindowFrameBound startBound = convertWindowFrameBound(proto.getStartBound());
    WindowFrameBound endBound =
        proto.hasBound()
            ? convertWindowFrameBound(proto.getBound())
            : new WindowFrameBound.CurrentRow();
    return new WindowFrame(frameType, startBound, endBound);
  }

  private static org.apache.arrow.datafusion.proto.WindowFrame toProtoWindowFrame(
      WindowFrame frame) {
    var builder =
        org.apache.arrow.datafusion.proto.WindowFrame.newBuilder()
            .setWindowFrameUnits(
                switch (frame.frameType()) {
                  case ROWS -> org.apache.arrow.datafusion.proto.WindowFrameUnits.ROWS;
                  case RANGE -> org.apache.arrow.datafusion.proto.WindowFrameUnits.RANGE;
                  case GROUPS -> org.apache.arrow.datafusion.proto.WindowFrameUnits.GROUPS;
                })
            .setStartBound(toProtoWindowFrameBound(frame.startBound()));
    if (frame.endBound() != null) {
      builder.setBound(toProtoWindowFrameBound(frame.endBound()));
    }
    return builder.build();
  }

  private static WindowFrameBound convertWindowFrameBound(
      org.apache.arrow.datafusion.proto.WindowFrameBound proto) {
    if (proto == null) {
      return new WindowFrameBound.CurrentRow();
    }
    return switch (proto.getWindowFrameBoundType()) {
      case CURRENT_ROW -> new WindowFrameBound.CurrentRow();
      case PRECEDING ->
          new WindowFrameBound.Preceding(
              proto.hasBoundValue()
                  ? ScalarValueProtoConverter.fromProto(proto.getBoundValue())
                  : null);
      case FOLLOWING ->
          new WindowFrameBound.Following(
              proto.hasBoundValue()
                  ? ScalarValueProtoConverter.fromProto(proto.getBoundValue())
                  : null);
      default -> new WindowFrameBound.CurrentRow();
    };
  }

  private static org.apache.arrow.datafusion.proto.WindowFrameBound toProtoWindowFrameBound(
      WindowFrameBound bound) {
    var builder = org.apache.arrow.datafusion.proto.WindowFrameBound.newBuilder();
    switch (bound) {
      case WindowFrameBound.CurrentRow ignored ->
          builder.setWindowFrameBoundType(
              org.apache.arrow.datafusion.proto.WindowFrameBoundType.CURRENT_ROW);
      case WindowFrameBound.Preceding p -> {
        builder.setWindowFrameBoundType(
            org.apache.arrow.datafusion.proto.WindowFrameBoundType.PRECEDING);
        if (p.value() != null) builder.setBoundValue(ScalarValueProtoConverter.toProto(p.value()));
      }
      case WindowFrameBound.Following f -> {
        builder.setWindowFrameBoundType(
            org.apache.arrow.datafusion.proto.WindowFrameBoundType.FOLLOWING);
        if (f.value() != null) builder.setBoundValue(ScalarValueProtoConverter.toProto(f.value()));
      }
    }
    return builder.build();
  }

  private static TableReference convertTableReference(
      org.apache.arrow.datafusion.proto.TableReference proto) {
    return TableReferenceFfi.convertTableReference(proto);
  }

  private static org.apache.arrow.datafusion.proto.TableReference toProtoTableReference(
      TableReference ref) {
    return TableReferenceFfi.toProtoTableReference(ref);
  }

  private static String getRelationString(TableReference ref) {
    return switch (ref) {
      case TableReference.Bare b -> b.table();
      case TableReference.Partial p -> p.table();
      case TableReference.Full f -> f.table();
    };
  }
}
