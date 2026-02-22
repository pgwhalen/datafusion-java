package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for {@link SessionContext#parseSqlExpr}. */
public class ParseSqlExprTest {

  private static final Schema INT_SCHEMA =
      new Schema(
          List.of(
              Field.nullable("a", new ArrowType.Int(32, true)),
              Field.nullable("b", new ArrowType.Int(32, true)),
              Field.nullable("x", new ArrowType.Int(64, true))));

  private static final Schema MIXED_SCHEMA =
      new Schema(
          List.of(
              Field.nullable("val", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
              Field.nullable("name", new ArrowType.Utf8()),
              Field.nullable("flag_a", new ArrowType.Bool()),
              Field.nullable("flag_b", new ArrowType.Bool())));

  @Test
  void simpleArithmetic() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("1 + 2", new Schema(List.of()));
      assertInstanceOf(Expr.BinaryExpr.class, expr);
      Expr.BinaryExpr bin = (Expr.BinaryExpr) expr;
      assertEquals(Operator.Plus, bin.op());
      assertInstanceOf(Expr.LiteralExpr.class, bin.left());
      assertInstanceOf(Expr.LiteralExpr.class, bin.right());
    }
  }

  @Test
  void columnReference() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("a + b", INT_SCHEMA);
      assertInstanceOf(Expr.BinaryExpr.class, expr);
      Expr.BinaryExpr bin = (Expr.BinaryExpr) expr;
      assertEquals(Operator.Plus, bin.op());
      assertInstanceOf(Expr.ColumnExpr.class, bin.left());
      assertInstanceOf(Expr.ColumnExpr.class, bin.right());
      assertEquals("a", ((Expr.ColumnExpr) bin.left()).column().name());
      assertEquals("b", ((Expr.ColumnExpr) bin.right()).column().name());
    }
  }

  @Test
  void comparison() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("x > 10", INT_SCHEMA);
      assertInstanceOf(Expr.BinaryExpr.class, expr);
      Expr.BinaryExpr bin = (Expr.BinaryExpr) expr;
      assertEquals(Operator.Gt, bin.op());
    }
  }

  @Test
  void booleanLogic() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("flag_a AND flag_b", MIXED_SCHEMA);
      assertInstanceOf(Expr.BinaryExpr.class, expr);
      Expr.BinaryExpr bin = (Expr.BinaryExpr) expr;
      assertEquals(Operator.And, bin.op());
    }
  }

  @Test
  void functionCall() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("abs(val)", MIXED_SCHEMA);
      assertInstanceOf(Expr.ScalarFunctionExpr.class, expr);
      Expr.ScalarFunctionExpr func = (Expr.ScalarFunctionExpr) expr;
      assertEquals("abs", func.funcName());
      assertEquals(1, func.args().size());
    }
  }

  @Test
  void castExpression() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("CAST(val AS INT)", MIXED_SCHEMA);
      assertInstanceOf(Expr.CastExpr.class, expr);
      Expr.CastExpr cast = (Expr.CastExpr) expr;
      assertInstanceOf(Expr.ColumnExpr.class, cast.expr());
    }
  }

  @Test
  void stringLiteral() {
    try (SessionContext ctx = new SessionContext()) {
      Expr expr = ctx.parseSqlExpr("name = 'Alice'", MIXED_SCHEMA);
      assertInstanceOf(Expr.BinaryExpr.class, expr);
      Expr.BinaryExpr bin = (Expr.BinaryExpr) expr;
      assertEquals(Operator.Eq, bin.op());
      assertInstanceOf(Expr.ColumnExpr.class, bin.left());
      assertInstanceOf(Expr.LiteralExpr.class, bin.right());
    }
  }

  @Test
  void invalidSqlThrows() {
    try (SessionContext ctx = new SessionContext()) {
      assertThrows(
          DataFusionException.class, () -> ctx.parseSqlExpr("+++invalid", new Schema(List.of())));
    }
  }

  @Test
  void unknownColumnThrows() {
    try (SessionContext ctx = new SessionContext()) {
      assertThrows(
          DataFusionException.class,
          () -> ctx.parseSqlExpr("nonexistent_col + 1", new Schema(List.of())));
    }
  }
}
