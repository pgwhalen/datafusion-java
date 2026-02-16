package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

/**
 * Tests for Expr proto round-trip serialization.
 *
 * <p>Each test constructs Java Expr records, serializes to proto bytes via {@link
 * ExprProtoConverter}, deserializes back, and asserts equality.
 */
public class ExprTest {

  @Test
  void testColumnExprRoundTrip() {
    Expr original = new Expr.ColumnExpr(new Column("id", null, null));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testColumnExprWithRelation() {
    Column col = new Column("name", new TableReference.Bare("my_table"), null);
    Expr original = new Expr.ColumnExpr(col);
    Expr result = roundTrip(original);
    assertInstanceOf(Expr.ColumnExpr.class, result);
    Expr.ColumnExpr colExpr = (Expr.ColumnExpr) result;
    assertEquals("name", colExpr.column().name());
  }

  @Test
  void testLiteralInt64RoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.Int64(42L));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralUtf8RoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.Utf8("hello"));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralBoolRoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.BooleanValue(true));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralFloat64RoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.Float64(3.14));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralNullRoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.Null());
    Expr result = roundTrip(original);
    assertInstanceOf(Expr.LiteralExpr.class, result);
    assertInstanceOf(ScalarValue.Null.class, ((Expr.LiteralExpr) result).value());
  }

  @Test
  void testLiteralDate32RoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.Date32(19000));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralTimestampRoundTrip() {
    Expr original = new Expr.LiteralExpr(new ScalarValue.TimestampMicrosecond(1234567890L, "UTC"));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLiteralDecimal128RoundTrip() {
    Expr original =
        new Expr.LiteralExpr(new ScalarValue.Decimal128(BigInteger.valueOf(12345), 10, 2));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testBinaryExprRoundTrip() {
    Expr left = new Expr.ColumnExpr(new Column("id", null, null));
    Expr right = new Expr.LiteralExpr(new ScalarValue.Int64(10L));
    Expr original = new Expr.BinaryExpr(left, Operator.Gt, right);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testAllOperatorsRoundTrip() {
    Expr left = new Expr.LiteralExpr(new ScalarValue.Int64(1L));
    Expr right = new Expr.LiteralExpr(new ScalarValue.Int64(2L));

    for (Operator op : Operator.values()) {
      Expr original = new Expr.BinaryExpr(left, op, right);
      Expr result = roundTrip(original);
      assertInstanceOf(Expr.BinaryExpr.class, result, "Failed for operator: " + op);
      assertEquals(op, ((Expr.BinaryExpr) result).op(), "Operator mismatch for: " + op);
    }
  }

  @Test
  void testNestedBinaryExprRoundTrip() {
    // (id > 1) AND (name = 'Alice')
    Expr idGt1 =
        new Expr.BinaryExpr(
            new Expr.ColumnExpr(new Column("id", null, null)),
            Operator.Gt,
            new Expr.LiteralExpr(new ScalarValue.Int64(1L)));
    Expr nameEqAlice =
        new Expr.BinaryExpr(
            new Expr.ColumnExpr(new Column("name", null, null)),
            Operator.Eq,
            new Expr.LiteralExpr(new ScalarValue.Utf8("Alice")));
    Expr original = new Expr.BinaryExpr(idGt1, Operator.And, nameEqAlice);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testNotExprRoundTrip() {
    Expr original = new Expr.NotExpr(new Expr.LiteralExpr(new ScalarValue.BooleanValue(false)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsNullRoundTrip() {
    Expr original = new Expr.IsNullExpr(new Expr.ColumnExpr(new Column("x", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsNotNullRoundTrip() {
    Expr original = new Expr.IsNotNullExpr(new Expr.ColumnExpr(new Column("x", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsTrueRoundTrip() {
    Expr original = new Expr.IsTrueExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsFalseRoundTrip() {
    Expr original = new Expr.IsFalseExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsUnknownRoundTrip() {
    Expr original = new Expr.IsUnknownExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsNotTrueRoundTrip() {
    Expr original = new Expr.IsNotTrueExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsNotFalseRoundTrip() {
    Expr original = new Expr.IsNotFalseExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testIsNotUnknownRoundTrip() {
    Expr original = new Expr.IsNotUnknownExpr(new Expr.ColumnExpr(new Column("flag", null, null)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testNegativeExprRoundTrip() {
    Expr original = new Expr.NegativeExpr(new Expr.LiteralExpr(new ScalarValue.Int64(42L)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testBetweenExprRoundTrip() {
    Expr original =
        new Expr.BetweenExpr(
            new Expr.ColumnExpr(new Column("age", null, null)),
            false,
            new Expr.LiteralExpr(new ScalarValue.Int64(18L)),
            new Expr.LiteralExpr(new ScalarValue.Int64(65L)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testBetweenNegatedRoundTrip() {
    Expr original =
        new Expr.BetweenExpr(
            new Expr.ColumnExpr(new Column("age", null, null)),
            true,
            new Expr.LiteralExpr(new ScalarValue.Int64(18L)),
            new Expr.LiteralExpr(new ScalarValue.Int64(65L)));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testCaseExprRoundTrip() {
    Expr original =
        new Expr.CaseExpr(
            null,
            List.of(
                new WhenThen(
                    new Expr.BinaryExpr(
                        new Expr.ColumnExpr(new Column("id", null, null)),
                        Operator.Eq,
                        new Expr.LiteralExpr(new ScalarValue.Int64(1L))),
                    new Expr.LiteralExpr(new ScalarValue.Utf8("one"))),
                new WhenThen(
                    new Expr.BinaryExpr(
                        new Expr.ColumnExpr(new Column("id", null, null)),
                        Operator.Eq,
                        new Expr.LiteralExpr(new ScalarValue.Int64(2L))),
                    new Expr.LiteralExpr(new ScalarValue.Utf8("two")))),
            new Expr.LiteralExpr(new ScalarValue.Utf8("other")));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testCastExprRoundTrip() {
    Expr original =
        new Expr.CastExpr(
            new Expr.ColumnExpr(new Column("val", null, null)),
            new ArrowType.FloatingPoint(
                org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testTryCastExprRoundTrip() {
    Expr original =
        new Expr.TryCastExpr(
            new Expr.ColumnExpr(new Column("val", null, null)), new ArrowType.Int(32, true));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testLikeExprRoundTrip() {
    Expr original =
        new Expr.LikeExpr(
            false,
            new Expr.ColumnExpr(new Column("name", null, null)),
            new Expr.LiteralExpr(new ScalarValue.Utf8("%alice%")),
            null,
            false);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testILikeExprRoundTrip() {
    Expr original =
        new Expr.LikeExpr(
            false,
            new Expr.ColumnExpr(new Column("name", null, null)),
            new Expr.LiteralExpr(new ScalarValue.Utf8("%alice%")),
            null,
            true);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testSimilarToExprRoundTrip() {
    Expr original =
        new Expr.SimilarToExpr(
            false,
            new Expr.ColumnExpr(new Column("name", null, null)),
            new Expr.LiteralExpr(new ScalarValue.Utf8("%(test|prod)%")),
            null);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testInListExprRoundTrip() {
    Expr original =
        new Expr.InListExpr(
            new Expr.ColumnExpr(new Column("status", null, null)),
            List.of(
                new Expr.LiteralExpr(new ScalarValue.Utf8("active")),
                new Expr.LiteralExpr(new ScalarValue.Utf8("pending"))),
            false);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testInListNegatedRoundTrip() {
    Expr original =
        new Expr.InListExpr(
            new Expr.ColumnExpr(new Column("id", null, null)),
            List.of(
                new Expr.LiteralExpr(new ScalarValue.Int64(1L)),
                new Expr.LiteralExpr(new ScalarValue.Int64(2L))),
            true);
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testScalarFunctionExprRoundTrip() {
    Expr original =
        new Expr.ScalarFunctionExpr(
            "abs", List.of(new Expr.ColumnExpr(new Column("value", null, null))));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testPlaceholderExprRoundTrip() {
    Expr original = new Expr.PlaceholderExpr("$1", new ArrowType.Int(64, true));
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testAliasExprRoundTrip() {
    Expr original =
        new Expr.AliasExpr(new Expr.ColumnExpr(new Column("id", null, null)), "my_id", List.of());
    Expr result = roundTrip(original);
    assertEquals(original, result);
  }

  @Test
  void testMultipleExprsRoundTrip() {
    // Test serializing multiple expressions in a single batch
    Expr[] originals =
        new Expr[] {
          new Expr.BinaryExpr(
              new Expr.ColumnExpr(new Column("id", null, null)),
              Operator.Gt,
              new Expr.LiteralExpr(new ScalarValue.Int64(1L))),
          new Expr.BinaryExpr(
              new Expr.ColumnExpr(new Column("name", null, null)),
              Operator.Eq,
              new Expr.LiteralExpr(new ScalarValue.Utf8("Alice")))
        };

    byte[] bytes = ExprProtoConverter.toProtoBytes(originals);
    Expr[] results = ExprProtoConverter.fromProtoBytes(bytes);

    assertEquals(originals.length, results.length);
    for (int i = 0; i < originals.length; i++) {
      assertEquals(originals[i], results[i], "Expr at index " + i + " should match");
    }
  }

  @Test
  void testEmptyExprsRoundTrip() {
    byte[] bytes = ExprProtoConverter.toProtoBytes(new Expr[0]);
    Expr[] results = ExprProtoConverter.fromProtoBytes(bytes);
    assertEquals(0, results.length);
  }

  /** Serializes an Expr to proto bytes and deserializes back. */
  private static Expr roundTrip(Expr expr) {
    byte[] bytes = ExprProtoConverter.toProtoBytes(new Expr[] {expr});
    Expr[] results = ExprProtoConverter.fromProtoBytes(bytes);
    assertEquals(1, results.length, "Should have exactly one expression");
    return results[0];
  }
}
