package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.Functions.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Comprehensive tests for the DataFrame and Expression builder API. */
public class DataFrameTest {

  // ==========================================================================
  // Phase 1: Expression builder tests (pure Java, no FFI)
  // ==========================================================================

  @Test
  void testExprComparison() {
    Expr a = col("a");
    Expr b = lit(10);

    assertInstanceOf(Expr.BinaryExpr.class, a.eq(b));
    assertEquals(Operator.Eq, ((Expr.BinaryExpr) a.eq(b)).op());
    assertEquals(Operator.NotEq, ((Expr.BinaryExpr) a.notEq(b)).op());
    assertEquals(Operator.Lt, ((Expr.BinaryExpr) a.lt(b)).op());
    assertEquals(Operator.LtEq, ((Expr.BinaryExpr) a.ltEq(b)).op());
    assertEquals(Operator.Gt, ((Expr.BinaryExpr) a.gt(b)).op());
    assertEquals(Operator.GtEq, ((Expr.BinaryExpr) a.gtEq(b)).op());
  }

  @Test
  void testExprLogical() {
    Expr a = col("a").gt(lit(0));
    Expr b = col("b").lt(lit(10));

    Expr andExpr = a.and(b);
    assertInstanceOf(Expr.BinaryExpr.class, andExpr);
    assertEquals(Operator.And, ((Expr.BinaryExpr) andExpr).op());

    Expr orExpr = a.or(b);
    assertInstanceOf(Expr.BinaryExpr.class, orExpr);
    assertEquals(Operator.Or, ((Expr.BinaryExpr) orExpr).op());

    Expr notExpr = a.not();
    assertInstanceOf(Expr.NotExpr.class, notExpr);
  }

  @Test
  void testExprArithmetic() {
    Expr a = col("a");
    Expr b = col("b");

    assertEquals(Operator.Plus, ((Expr.BinaryExpr) a.plus(b)).op());
    assertEquals(Operator.Minus, ((Expr.BinaryExpr) a.minus(b)).op());
    assertEquals(Operator.Multiply, ((Expr.BinaryExpr) a.multiply(b)).op());
    assertEquals(Operator.Divide, ((Expr.BinaryExpr) a.divide(b)).op());
    assertEquals(Operator.Modulo, ((Expr.BinaryExpr) a.modulo(b)).op());
    assertInstanceOf(Expr.NegativeExpr.class, a.negate());
  }

  @Test
  void testExprLike() {
    Expr a = col("name");
    Expr pattern = lit("%alice%");

    Expr likeExpr = a.like(pattern);
    assertInstanceOf(Expr.LikeExpr.class, likeExpr);
    assertFalse(((Expr.LikeExpr) likeExpr).negated());
    assertFalse(((Expr.LikeExpr) likeExpr).caseInsensitive());

    Expr notLikeExpr = a.notLike(pattern);
    assertTrue(((Expr.LikeExpr) notLikeExpr).negated());

    Expr ilikeExpr = a.ilike(pattern);
    assertTrue(((Expr.LikeExpr) ilikeExpr).caseInsensitive());
  }

  @Test
  void testExprNullChecks() {
    Expr a = col("a");
    assertInstanceOf(Expr.IsNullExpr.class, a.isNull());
    assertInstanceOf(Expr.IsNotNullExpr.class, a.isNotNull());
    assertInstanceOf(Expr.IsTrueExpr.class, a.isTrue());
    assertInstanceOf(Expr.IsFalseExpr.class, a.isFalse());
  }

  @Test
  void testExprInList() {
    Expr a = col("a");
    List<Expr> values = List.of(lit(1), lit(2), lit(3));

    Expr inExpr = a.inList(values);
    assertInstanceOf(Expr.InListExpr.class, inExpr);
    assertFalse(((Expr.InListExpr) inExpr).negated());

    Expr notInExpr = a.notInList(values);
    assertTrue(((Expr.InListExpr) notInExpr).negated());
  }

  @Test
  void testExprBetween() {
    Expr a = col("a");
    Expr betweenExpr = a.between(lit(1), lit(10));
    assertInstanceOf(Expr.BetweenExpr.class, betweenExpr);
    assertFalse(((Expr.BetweenExpr) betweenExpr).negated());

    Expr notBetweenExpr = a.notBetween(lit(1), lit(10));
    assertTrue(((Expr.BetweenExpr) notBetweenExpr).negated());
  }

  @Test
  void testExprAlias() {
    Expr aliased = col("a").alias("my_col");
    assertInstanceOf(Expr.AliasExpr.class, aliased);
    assertEquals("my_col", ((Expr.AliasExpr) aliased).alias());
  }

  @Test
  void testExprSort() {
    Expr a = col("a");

    SortExpr ascSort = a.sortAsc();
    assertTrue(ascSort.asc());
    assertFalse(ascSort.nullsFirst());

    SortExpr descSort = a.sortDesc();
    assertFalse(descSort.asc());
    assertTrue(descSort.nullsFirst());

    SortExpr customSort = a.sort(false, false);
    assertFalse(customSort.asc());
    assertFalse(customSort.nullsFirst());
  }

  @Test
  void testFunctionsCol() {
    Expr simple = col("x");
    assertInstanceOf(Expr.ColumnExpr.class, simple);
    assertEquals("x", ((Expr.ColumnExpr) simple).column().name());

    Expr qualified = col("t", "x");
    assertInstanceOf(Expr.ColumnExpr.class, qualified);
    Column c = ((Expr.ColumnExpr) qualified).column();
    assertEquals("x", c.name());
    assertNotNull(c.relation());
  }

  @Test
  void testCaseBuilder() {
    Expr caseExpr =
        when(col("status").eq(lit("active")), lit(1))
            .when(col("status").eq(lit("pending")), lit(0))
            .otherwise(lit(-1));

    assertInstanceOf(Expr.CaseExpr.class, caseExpr);
    Expr.CaseExpr c = (Expr.CaseExpr) caseExpr;
    assertEquals(2, c.whenThen().size());
    assertNotNull(c.elseExpr());
  }

  @Test
  void testCaseBuilderEnd() {
    Expr caseExpr = when(col("x").gt(lit(0)), lit("positive")).end();
    assertInstanceOf(Expr.CaseExpr.class, caseExpr);
    assertNull(((Expr.CaseExpr) caseExpr).elseExpr());
  }

  @Test
  void testFunctionsCastAndNot() {
    Expr castExpr = cast(col("x"), new ArrowType.Utf8());
    assertInstanceOf(Expr.CastExpr.class, castExpr);

    Expr tryCastExpr = tryCast(col("x"), new ArrowType.Int(32, true));
    assertInstanceOf(Expr.TryCastExpr.class, tryCastExpr);

    Expr notExpr = not(col("flag"));
    assertInstanceOf(Expr.NotExpr.class, notExpr);
  }

  // ==========================================================================
  // Phase 2: DataFrame transformations (FFI integration)
  // ==========================================================================

  // Employees: Bob(25), Alice(30), Charlie(35) — filter on age vs 30
  static Stream<Arguments> filterComparisonCases() {
    BiFunction<Expr, Expr, Expr> gt = Expr::gt;
    BiFunction<Expr, Expr, Expr> gtEq = Expr::gtEq;
    BiFunction<Expr, Expr, Expr> lt = Expr::lt;
    BiFunction<Expr, Expr, Expr> ltEq = Expr::ltEq;
    BiFunction<Expr, Expr, Expr> eq = Expr::eq;
    BiFunction<Expr, Expr, Expr> notEq = Expr::notEq;
    return Stream.of(
        Arguments.of("gt", gt, new long[] {35}),
        Arguments.of("gtEq", gtEq, new long[] {30, 35}),
        Arguments.of("lt", lt, new long[] {25}),
        Arguments.of("ltEq", ltEq, new long[] {25, 30}),
        Arguments.of("eq", eq, new long[] {30}),
        Arguments.of("notEq", notEq, new long[] {25, 35}));
  }

  @ParameterizedTest(name = "filter age {0} 30")
  @MethodSource("filterComparisonCases")
  void testFilter(String opName, BiFunction<Expr, Expr, Expr> op, long[] expectedAges) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .filter(op.apply(col("age"), lit(30)))
                  .sort(col("age").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(expectedAges.length, root.getRowCount());
        BigIntVector age = (BigIntVector) root.getVector("age");
        for (int i = 0; i < expectedAges.length; i++) {
          assertEquals(expectedAges[i], age.get(i), "row " + i + " for op " + opName);
        }
      }
    }
  }

  @Test
  void testSelect() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .select(col("name"), col("salary"))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(2, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("salary", schema.getFields().get(1).getName());

        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector salary = (BigIntVector) root.getVector("salary");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(50000, salary.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(30000, salary.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(70000, salary.get(2));
      }
    }
  }

  @Test
  void testSelectColumns() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .selectColumns("name", "age")
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(2, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("age", schema.getFields().get(1).getName());

        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(25, age.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
      }
    }
  }

  // Employees: salary = {30000, 50000, 70000}, all distinct
  // Global aggregation (no group-by) produces exactly 1 row.
  // Note: variance is omitted — DataFusion's proto codec doesn't support the "variance" alias.
  static Stream<Arguments> aggregateFunctionCases() {
    Function<Expr, Expr> sumFn = Functions::sum;
    Function<Expr, Expr> countFn = Functions::count;
    Function<Expr, Expr> minFn = Functions::min;
    Function<Expr, Expr> maxFn = Functions::max;
    Function<Expr, Expr> avgFn = Functions::avg;
    Function<Expr, Expr> medianFn = Functions::median;
    Function<Expr, Expr> stddevFn = Functions::stddev;
    Function<Expr, Expr> countDistinctFn = Functions::countDistinct;
    return Stream.of(
        Arguments.of("sum", sumFn, 150_000.0, 0.01),
        Arguments.of("count", countFn, 3.0, 0.01),
        Arguments.of("min", minFn, 30_000.0, 0.01),
        Arguments.of("max", maxFn, 70_000.0, 0.01),
        Arguments.of("avg", avgFn, 50_000.0, 0.01),
        Arguments.of("median", medianFn, 50_000.0, 0.01),
        Arguments.of("stddev", stddevFn, 20_000.0, 0.01),
        Arguments.of("countDistinct", countDistinctFn, 3.0, 0.01));
  }

  @ParameterizedTest(name = "{0}(salary)")
  @MethodSource("aggregateFunctionCases")
  void testAggregate(String name, Function<Expr, Expr> aggFn, double expected, double delta) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .aggregate(List.of(), List.of(aggFn.apply(col("salary")).alias("result")));
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount(), name + " should produce 1 row");
        double actual = getNumericValue(root, "result", 0);
        assertEquals(expected, actual, delta, name + "(salary)");
      }
    }
  }

  @Test
  void testAggregateCountAll() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .aggregate(List.of(), List.of(countAll().alias("result")));
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        assertEquals(3.0, getNumericValue(stream.getVectorSchemaRoot(), "result", 0), 0.01);
      }
    }
  }

  @Test
  void testAggregateWithGroupBy() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .aggregate(
                      List.of(col("dept")),
                      List.of(sum(col("salary")).alias("total"), count(col("name")).alias("cnt")))
                  .sort(col("dept").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());

        VarCharVector dept = (VarCharVector) root.getVector("dept");
        // Engineering: Alice(50000) + Charlie(70000)
        assertEquals("Engineering", dept.getObject(0).toString());
        assertEquals(120_000.0, getNumericValue(root, "total", 0), 0.01);
        assertEquals(2.0, getNumericValue(root, "cnt", 0), 0.01);

        // Sales: Bob(30000)
        assertEquals("Sales", dept.getObject(1).toString());
        assertEquals(30_000.0, getNumericValue(root, "total", 1), 0.01);
        assertEquals(1.0, getNumericValue(root, "cnt", 1), 0.01);
      }
    }
  }

  @Test
  void testSort() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees").sort(col("salary").sortDesc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector salary = (BigIntVector) root.getVector("salary");
        assertEquals("Charlie", name.getObject(0).toString());
        assertEquals(70000, salary.get(0));
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(50000, salary.get(1));
        assertEquals("Bob", name.getObject(2).toString());
        assertEquals(30000, salary.get(2));
      }
    }
  }

  @Test
  void testLimit() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees").sort(col("age").sortAsc()).limit(0, 2);
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Bob", name.getObject(0).toString());
        assertEquals(25, age.get(0));
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(30, age.get(1));
      }
    }
  }

  @Test
  void testLimitSkip() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees").sort(col("age").sortAsc()).limit(1, 2);
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());
        // Skip first (age=25), should get age=30 and age=35
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals(30, age.get(0));
        assertEquals(35, age.get(1));
      }
    }
  }

  @Test
  void testCollect() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees ORDER BY name");
          RecordBatchStream stream = df.collect(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(25, age.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
      }
    }
  }

  @Test
  void testCount() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        assertEquals(3, df.count());
      }
    }
  }

  @Test
  void testSchema() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        Schema schema = df.schema();
        assertEquals(4, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("age", schema.getFields().get(1).getName());
        assertEquals("salary", schema.getFields().get(2).getName());
        assertEquals("dept", schema.getFields().get(3).getName());
      }
    }
  }

  @Test
  void testShow() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      // Just verify show() doesn't throw
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        assertDoesNotThrow(df::show);
      }
    }
  }

  // ==========================================================================
  // Phase 3: Joins and set operations
  // ==========================================================================

  @Test
  void testJoinOnColumns() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);
      registerDepartments(ctx, allocator);

      try (DataFrame employees = ctx.sql("SELECT * FROM employees");
          DataFrame departments = ctx.sql("SELECT * FROM departments");
          DataFrame joined =
              employees
                  .join(departments, JoinType.INNER, List.of("dept"), List.of("dept_name"))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = joined.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        VarCharVector location = (VarCharVector) root.getVector("location");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
        assertEquals("SF", location.getObject(0).toString());
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(25, age.get(1));
        assertEquals("NYC", location.getObject(1).toString());
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
        assertEquals("SF", location.getObject(2).toString());
      }
    }
  }

  @Test
  void testJoinOn() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);
      registerDepartments(ctx, allocator);

      try (DataFrame employees = ctx.sql("SELECT * FROM employees");
          DataFrame departments = ctx.sql("SELECT * FROM departments");
          DataFrame joined =
              employees
                  .joinOn(departments, JoinType.INNER, List.of(col("dept").eq(col("dept_name"))))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = joined.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(25, age.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
      }
    }
  }

  @Test
  void testJoinWithFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);
      registerDepartments(ctx, allocator);

      // Join on columns with an additional filter: only keep rows where location = 'SF'
      try (DataFrame employees = ctx.sql("SELECT * FROM employees");
          DataFrame departments = ctx.sql("SELECT * FROM departments");
          DataFrame joined =
              employees
                  .join(
                      departments,
                      JoinType.INNER,
                      List.of("dept"),
                      List.of("dept_name"),
                      col("location").eq(lit("SF")))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = joined.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Only Engineering employees (SF): Alice and Charlie
        assertEquals(2, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals("Charlie", name.getObject(1).toString());

        VarCharVector location = (VarCharVector) root.getVector("location");
        assertEquals("SF", location.getObject(0).toString());
        assertEquals("SF", location.getObject(1).toString());
      }
    }
  }

  @Test
  void testLeftJoin() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);
      registerDepartments(ctx, allocator);

      try (DataFrame employees = ctx.sql("SELECT * FROM employees");
          DataFrame departments = ctx.sql("SELECT * FROM departments");
          DataFrame joined =
              employees
                  .join(departments, JoinType.LEFT, List.of("dept"), List.of("dept_name"))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = joined.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Left join: all 3 employees retained
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector location = (VarCharVector) root.getVector("location");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals("SF", location.getObject(0).toString());
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals("NYC", location.getObject(1).toString());
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals("SF", location.getObject(2).toString());
      }
    }
  }

  @Test
  void testUnion() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df1 = ctx.sql("SELECT name, age FROM employees WHERE age <= 30");
          DataFrame df2 = ctx.sql("SELECT name, age FROM employees WHERE age > 25");
          DataFrame unioned = df1.union(df2).sort(col("age").sortAsc(), col("name").sortAsc());
          RecordBatchStream stream = unioned.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Union ALL: Bob(25) + Alice(30) from df1 + Alice(30) + Charlie(35) from df2 = 4 rows
        assertEquals(4, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Bob", name.getObject(0).toString());
        assertEquals(25, age.get(0));
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(30, age.get(1));
        assertEquals("Alice", name.getObject(2).toString());
        assertEquals(30, age.get(2));
        assertEquals("Charlie", name.getObject(3).toString());
        assertEquals(35, age.get(3));
      }
    }
  }

  @Test
  void testUnionDistinct() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df1 = ctx.sql("SELECT name, age FROM employees WHERE age <= 30");
          DataFrame df2 = ctx.sql("SELECT name, age FROM employees WHERE age > 25");
          DataFrame unioned = df1.unionDistinct(df2).sort(col("age").sortAsc());
          RecordBatchStream stream = unioned.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Union distinct: Bob(25) + Alice(30) + Charlie(35) = 3 unique rows
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Bob", name.getObject(0).toString());
        assertEquals(25, age.get(0));
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(30, age.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
      }
    }
  }

  @Test
  void testIntersect() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df1 = ctx.sql("SELECT name, age FROM employees WHERE age <= 30");
          DataFrame df2 = ctx.sql("SELECT name, age FROM employees WHERE age >= 30");
          DataFrame intersected = df1.intersect(df2);
          RecordBatchStream stream = intersected.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Intersection: only Alice(30) is in both
        assertEquals(1, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
      }
    }
  }

  @Test
  void testExcept() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df1 = ctx.sql("SELECT name, age FROM employees");
          DataFrame df2 = ctx.sql("SELECT name, age FROM employees WHERE age = 30");
          DataFrame excepted = df1.except(df2).sort(col("age").sortAsc());
          RecordBatchStream stream = excepted.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Except: all employees minus Alice(30) = Bob(25) and Charlie(35)
        assertEquals(2, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Bob", name.getObject(0).toString());
        assertEquals(25, age.get(0));
        assertEquals("Charlie", name.getObject(1).toString());
        assertEquals(35, age.get(1));
      }
    }
  }

  @Test
  void testDistinct() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT dept FROM employees").distinct().sort(col("dept").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // 2 distinct departments: Engineering and Sales
        assertEquals(2, root.getRowCount());

        VarCharVector dept = (VarCharVector) root.getVector("dept");
        assertEquals("Engineering", dept.getObject(0).toString());
        assertEquals("Sales", dept.getObject(1).toString());
      }
    }
  }

  // ==========================================================================
  // Phase 4: SessionContext readers
  // ==========================================================================

  @Test
  void testTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.table("employees").sort(col("name").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        Schema schema = df.schema();
        assertEquals(4, schema.getFields().size());

        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(30, age.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(25, age.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(35, age.get(2));
      }
    }
  }

  @Test
  void testReadCsv(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.writeString(csvFile, "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(csvFile.toString()).sort(col("id").sortAsc());
        RecordBatchStream stream = df.executeStream(allocator)) {
      Schema schema = df.schema();
      assertEquals(3, schema.getFields().size());
      assertEquals("id", schema.getFields().get(0).getName());
      assertEquals("name", schema.getFields().get(1).getName());
      assertEquals("value", schema.getFields().get(2).getName());

      assertTrue(stream.loadNextBatch());
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());

      BigIntVector id = (BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");
      BigIntVector value = (BigIntVector) root.getVector("value");

      assertEquals(1, id.get(0));
      assertEquals("Alice", name.getObject(0).toString());
      assertEquals(100, value.get(0));

      assertEquals(2, id.get(1));
      assertEquals("Bob", name.getObject(1).toString());
      assertEquals(200, value.get(1));

      assertEquals(3, id.get(2));
      assertEquals("Charlie", name.getObject(2).toString());
      assertEquals(300, value.get(2));

      assertFalse(stream.loadNextBatch());
    }
  }

  @Test
  void testReadJson(@TempDir Path tempDir) throws IOException {
    Path jsonFile = tempDir.resolve("test.json");
    Files.writeString(jsonFile, "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readJson(jsonFile.toString()).sort(col("id").sortAsc());
        RecordBatchStream stream = df.executeStream(allocator)) {
      assertTrue(stream.loadNextBatch());
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertEquals(2, root.getRowCount());

      BigIntVector id = (BigIntVector) root.getVector("id");
      VarCharVector name = (VarCharVector) root.getVector("name");

      assertEquals(1, id.get(0));
      assertEquals("Alice", name.getObject(0).toString());

      assertEquals(2, id.get(1));
      assertEquals("Bob", name.getObject(1).toString());

      assertFalse(stream.loadNextBatch());
    }
  }

  // ==========================================================================
  // Write operations
  // ==========================================================================

  @Test
  void testReadParquet() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readParquet("src/test/resources/test.parquet").sort(col("id").sortAsc());
        RecordBatchStream stream = df.executeStream(allocator)) {
      assertTrue(stream.loadNextBatch());
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertEquals(3, root.getRowCount());

      BigIntVector id = (BigIntVector) root.getVector("id");
      BigIntVector value = (BigIntVector) root.getVector("value");
      assertEquals(1, id.get(0));
      assertEquals(100, value.get(0));
      assertEquals(2, id.get(1));
      assertEquals(200, value.get(1));
      assertEquals(3, id.get(2));
      assertEquals(300, value.get(2));
    }
  }

  @Test
  void testWriteParquet(@TempDir Path tempDir) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output.parquet");
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeParquet(outDir.toString());
      }

      // Read back and verify
      // Note: Parquet read-back may use ViewVarCharVector instead of VarCharVector,
      // so use getObject().toString() for string comparison.
      try (DataFrame df = ctx.readParquet(outDir.toString()).sort(col("age").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals(25, age.get(0));
        assertEquals("Bob", root.getVector("name").getObject(0).toString());
        assertEquals(30, age.get(1));
        assertEquals("Alice", root.getVector("name").getObject(1).toString());
        assertEquals(35, age.get(2));
        assertEquals("Charlie", root.getVector("name").getObject(2).toString());
      }
    }
  }

  @Test
  void testWriteCsv(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output.csv");
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeCsv(outDir.toString());
      }

      // Read back and verify
      try (DataFrame df = ctx.readCsv(outDir.toString()).sort(col("age").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        BigIntVector age = (BigIntVector) root.getVector("age");
        assertEquals(25, age.get(0));
        assertEquals(30, age.get(1));
        assertEquals(35, age.get(2));
      }
    }
  }

  @Test
  void testWriteJson(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output.json");
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeJson(outDir.toString());
      }

      // Read back and verify
      try (DataFrame df = ctx.readJson(outDir.toString()).sort(col("age").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        BigIntVector age = (BigIntVector) root.getVector("age");
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector salary = (BigIntVector) root.getVector("salary");
        assertEquals(25, age.get(0));
        assertEquals("Bob", name.getObject(0).toString());
        assertEquals(30000, salary.get(0));
        assertEquals(30, age.get(1));
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(50000, salary.get(1));
        assertEquals(35, age.get(2));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(70000, salary.get(2));
      }
    }
  }

  // ==========================================================================
  // Phase 5: Advanced features
  // ==========================================================================

  @Test
  void testWithColumn() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .withColumn("bonus", col("salary").multiply(lit(0.1)));
          RecordBatchStream stream = df.executeStream(allocator)) {
        Schema schema = stream.getVectorSchemaRoot().getSchema();
        // Original 4 + bonus = 5 columns
        assertEquals(5, schema.getFields().size());
        assertEquals("bonus", schema.getFields().get(4).getName());

        assertTrue(stream.loadNextBatch());
        Float8Vector bonus = (Float8Vector) stream.getVectorSchemaRoot().getVector("bonus");
        // First employee (Alice): salary=50000, bonus=5000.0
        assertEquals(5000.0, bonus.get(0), 0.01);
      }
    }
  }

  @Test
  void testWithColumnRenamed() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees").withColumnRenamed("salary", "pay")) {
        Schema schema = df.schema();
        List<String> fieldNames = schema.getFields().stream().map(Field::getName).toList();
        assertTrue(fieldNames.contains("pay"));
        assertFalse(fieldNames.contains("salary"));
      }
    }
  }

  @Test
  void testDropColumns() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees").dropColumns("age", "dept")) {
        Schema schema = df.schema();
        assertEquals(2, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("salary", schema.getFields().get(1).getName());
      }
    }
  }

  // ==========================================================================
  // Complex integration tests
  // ==========================================================================

  @Test
  void testChainedTransformations() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      // Complex query chain: filter -> select -> sort -> limit
      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .filter(col("age").gtEq(lit(30)))
                  .select(col("name"), col("salary"))
                  .sort(col("salary").sortDesc())
                  .limit(0, 1);
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        assertEquals(1, stream.getVectorSchemaRoot().getRowCount());
        // Highest salary among age>=30: Charlie(70000) > Alice(50000)
        BigIntVector salary = (BigIntVector) stream.getVectorSchemaRoot().getVector("salary");
        assertEquals(70000, salary.get(0));
      }
    }
  }

  @Test
  void testAggregateWithFilterAndSort() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .aggregate(
                      List.of(col("dept")),
                      List.of(
                          avg(col("salary")).alias("avg_salary"),
                          count(col("name")).alias("headcount")))
                  .sort(col("avg_salary").sortDesc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());

        VarCharVector dept = (VarCharVector) root.getVector("dept");
        // Engineering: avg(50000, 70000) = 60000, headcount = 2
        assertEquals("Engineering", dept.getObject(0).toString());
        assertEquals(60000.0, getNumericValue(root, "avg_salary", 0), 0.01);
        assertEquals(2.0, getNumericValue(root, "headcount", 0), 0.01);

        // Sales: avg(30000) = 30000, headcount = 1
        assertEquals("Sales", dept.getObject(1).toString());
        assertEquals(30000.0, getNumericValue(root, "avg_salary", 1), 0.01);
        assertEquals(1.0, getNumericValue(root, "headcount", 1), 0.01);
      }
    }
  }

  @Test
  void testTableThenFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.table("employees")
                  .selectColumns("name", "salary")
                  .filter(col("salary").gt(lit(50000)));
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Only Charlie with salary > 50000
        assertEquals(1, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector salary = (BigIntVector) root.getVector("salary");
        assertEquals("Charlie", name.getObject(0).toString());
        assertEquals(70000, salary.get(0));
      }
    }
  }

  @Test
  void testWithColumnCaseExpression() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .withColumn(
                      "salary_band",
                      when(col("salary").gt(lit(60000)), lit("high"))
                          .when(col("salary").gt(lit(40000)), lit("mid"))
                          .otherwise(lit("low")))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(5, root.getSchema().getFields().size());
        assertEquals("salary_band", root.getSchema().getFields().get(4).getName());

        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector salaryBand = (VarCharVector) root.getVector("salary_band");
        // Alice(50000) -> mid, Bob(30000) -> low, Charlie(70000) -> high
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals("mid", salaryBand.getObject(0).toString());
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals("low", salaryBand.getObject(1).toString());
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals("high", salaryBand.getObject(2).toString());
      }
    }
  }

  @Test
  void testCsvFilterAndAggregate(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("sales.csv");
    Files.writeString(
        csvFile,
        "region,product,amount\n"
            + "East,Widget,100\n"
            + "East,Gadget,200\n"
            + "West,Widget,150\n"
            + "West,Gadget,250\n"
            + "East,Widget,120\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df =
            ctx.readCsv(csvFile.toString())
                .filter(col("product").eq(lit("Widget")))
                .aggregate(List.of(col("region")), List.of(sum(col("amount")).alias("total")))
                .sort(col("region").sortAsc());
        RecordBatchStream stream = df.executeStream(allocator)) {
      assertTrue(stream.loadNextBatch());
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertEquals(2, root.getRowCount());

      VarCharVector region = (VarCharVector) root.getVector("region");
      // East Widgets: 100 + 120 = 220
      assertEquals("East", region.getObject(0).toString());
      assertEquals(220.0, getNumericValue(root, "total", 0), 0.01);

      // West Widgets: 150
      assertEquals("West", region.getObject(1).toString());
      assertEquals(150.0, getNumericValue(root, "total", 1), 0.01);

      assertFalse(stream.loadNextBatch());
    }
  }

  @Test
  void testSelectWithExpressions() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .select(
                      col("name"),
                      col("salary").multiply(lit(12)).alias("annual_salary"),
                      col("age").plus(lit(1)).alias("next_age"))
                  .sort(col("name").sortAsc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        Schema schema = root.getSchema();
        assertEquals(3, schema.getFields().size());
        assertEquals("name", schema.getFields().get(0).getName());
        assertEquals("annual_salary", schema.getFields().get(1).getName());
        assertEquals("next_age", schema.getFields().get(2).getName());

        assertEquals(3, root.getRowCount());
        VarCharVector name = (VarCharVector) root.getVector("name");
        BigIntVector annualSalary = (BigIntVector) root.getVector("annual_salary");
        BigIntVector nextAge = (BigIntVector) root.getVector("next_age");
        assertEquals("Alice", name.getObject(0).toString());
        assertEquals(600000, annualSalary.get(0));
        assertEquals(31, nextAge.get(0));
        assertEquals("Bob", name.getObject(1).toString());
        assertEquals(360000, annualSalary.get(1));
        assertEquals(26, nextAge.get(1));
        assertEquals("Charlie", name.getObject(2).toString());
        assertEquals(840000, annualSalary.get(2));
        assertEquals(36, nextAge.get(2));
      }
    }
  }

  @Test
  void testLimitNullFetch() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      // limit(1, null) means skip 1 row, no fetch limit
      try (DataFrame df =
              ctx.sql("SELECT * FROM employees").sort(col("age").sortAsc()).limit(1, null);
          RecordBatchStream stream = df.collect(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount()); // Skip 1, return remaining 2

        BigIntVector age = (BigIntVector) root.getVector("age");
        BigIntVector salary = (BigIntVector) root.getVector("salary");
        // Skip first (Bob, age=25), should get Alice(30, 50000) and Charlie(35, 70000)
        assertEquals(30, age.get(0));
        assertEquals(50000, salary.get(0));
        assertEquals(35, age.get(1));
        assertEquals(70000, salary.get(1));
      }
    }
  }

  @Test
  void testMultipleSortColumns() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      // Sort by dept ascending, then salary descending within each dept
      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .sort(col("dept").sortAsc(), col("salary").sortDesc());
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(3, root.getRowCount());

        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector dept = (VarCharVector) root.getVector("dept");
        BigIntVector salary = (BigIntVector) root.getVector("salary");

        // Engineering first (asc), highest salary first (desc): Charlie(70000), Alice(50000)
        assertEquals("Engineering", dept.getObject(0).toString());
        assertEquals("Charlie", name.getObject(0).toString());
        assertEquals(70000, salary.get(0));

        assertEquals("Engineering", dept.getObject(1).toString());
        assertEquals("Alice", name.getObject(1).toString());
        assertEquals(50000, salary.get(1));

        // Sales last: Bob(30000)
        assertEquals("Sales", dept.getObject(2).toString());
        assertEquals("Bob", name.getObject(2).toString());
        assertEquals(30000, salary.get(2));
      }
    }
  }

  @Test
  void testGlobalAggregate() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      // Global aggregation (empty group-by)
      try (DataFrame df =
              ctx.sql("SELECT * FROM employees")
                  .aggregate(
                      List.of(),
                      List.of(
                          sum(col("salary")).alias("total"),
                          count(col("name")).alias("cnt"),
                          min(col("age")).alias("youngest"),
                          max(col("age")).alias("oldest")));
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        BigIntVector total = (BigIntVector) root.getVector("total");
        BigIntVector cnt = (BigIntVector) root.getVector("cnt");
        BigIntVector youngest = (BigIntVector) root.getVector("youngest");
        BigIntVector oldest = (BigIntVector) root.getVector("oldest");
        assertEquals(150000, total.get(0)); // 50000 + 30000 + 70000
        assertEquals(3, cnt.get(0));
        assertEquals(25, youngest.get(0));
        assertEquals(35, oldest.get(0));
      }
    }
  }

  // ==========================================================================
  // Test data helpers
  // ==========================================================================

  /**
   * Registers an "employees" table with columns: name(Utf8), age(Int64), salary(Int64), dept(Utf8).
   *
   * <p>Data: Alice(30, 50000, Engineering), Bob(25, 30000, Sales), Charlie(35, 70000, Engineering)
   */
  private void registerEmployees(SessionContext ctx, BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("salary", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("dept", FieldType.nullable(new ArrowType.Utf8()), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    VarCharVector name = (VarCharVector) root.getVector("name");
    BigIntVector age = (BigIntVector) root.getVector("age");
    BigIntVector salary = (BigIntVector) root.getVector("salary");
    VarCharVector dept = (VarCharVector) root.getVector("dept");

    int rows = 3;
    name.allocateNew(rows);
    age.allocateNew(rows);
    salary.allocateNew(rows);
    dept.allocateNew(rows);

    name.setSafe(0, "Alice".getBytes());
    age.set(0, 30);
    salary.set(0, 50000);
    dept.setSafe(0, "Engineering".getBytes());

    name.setSafe(1, "Bob".getBytes());
    age.set(1, 25);
    salary.set(1, 30000);
    dept.setSafe(1, "Sales".getBytes());

    name.setSafe(2, "Charlie".getBytes());
    age.set(2, 35);
    salary.set(2, 70000);
    dept.setSafe(2, "Engineering".getBytes());

    name.setValueCount(rows);
    age.setValueCount(rows);
    salary.setValueCount(rows);
    dept.setValueCount(rows);
    root.setRowCount(rows);

    ctx.registerTable("employees", root, allocator);
    root.close();
  }

  /**
   * Registers a "departments" table with columns: dept_name(Utf8), location(Utf8).
   *
   * <p>Data: Engineering(SF), Sales(NYC), Marketing(LA)
   */
  private void registerDepartments(SessionContext ctx, BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("dept_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("location", FieldType.nullable(new ArrowType.Utf8()), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    VarCharVector deptName = (VarCharVector) root.getVector("dept_name");
    VarCharVector location = (VarCharVector) root.getVector("location");

    int rows = 3;
    deptName.allocateNew(rows);
    location.allocateNew(rows);

    deptName.setSafe(0, "Engineering".getBytes());
    location.setSafe(0, "SF".getBytes());

    deptName.setSafe(1, "Sales".getBytes());
    location.setSafe(1, "NYC".getBytes());

    deptName.setSafe(2, "Marketing".getBytes());
    location.setSafe(2, "LA".getBytes());

    deptName.setValueCount(rows);
    location.setValueCount(rows);
    root.setRowCount(rows);

    ctx.registerTable("departments", root, allocator);
    root.close();
  }

  /** Extract a numeric value as double from any numeric vector type. */
  private static double getNumericValue(VectorSchemaRoot root, String column, int row) {
    var vector = root.getVector(column);
    if (vector instanceof BigIntVector v) {
      return v.get(row);
    } else if (vector instanceof Float8Vector v) {
      return v.get(row);
    } else {
      throw new AssertionError(
          "Unexpected vector type for column '" + column + "': " + vector.getClass().getName());
    }
  }
}
