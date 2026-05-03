package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.Functions.*;
import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.datafusion.common.Column;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.dataframe.DataFrameWriteOptions;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.JoinType;
import org.apache.arrow.datafusion.logical_expr.Operator;
import org.apache.arrow.datafusion.logical_expr.SortExpr;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
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

    assertEquals(Operator.Plus, ((Expr.BinaryExpr) a.add(b)).op());
    assertEquals(Operator.Minus, ((Expr.BinaryExpr) a.sub(b)).op());
    assertEquals(Operator.Multiply, ((Expr.BinaryExpr) a.mul(b)).op());
    assertEquals(Operator.Divide, ((Expr.BinaryExpr) a.div(b)).op());
    assertEquals(Operator.Modulo, ((Expr.BinaryExpr) a.rem(b)).op());
    assertInstanceOf(Expr.NegativeExpr.class, a.neg());
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        var assertion = expect("age").allowExtraColumns();
        for (long expected : expectedAges) {
          assertion.row(expected);
        }
        assertion.assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("name", "salary")
            .row("Alice", 50000L)
            .row("Bob", 30000L)
            .row("Charlie", 70000L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("name", "age")
            .row("Alice", 30L)
            .row("Bob", 25L)
            .row("Charlie", 35L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Cast expected to Long: works for both BigInt (sum/count/min/max/countDistinct) and
        // Float8 (avg/median/stddev) result columns since all test values are whole numbers.
        expect("result").withDelta(delta).row((long) expected).assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("result").row(3L).assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("dept", "total", "cnt")
            .row("Engineering", 120_000L, 2L)
            .row("Sales", 30_000L, 1L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testSort() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees").sort(col("salary").sortDesc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("name", "salary")
            .allowExtraColumns()
            .row("Charlie", 70000L)
            .row("Alice", 50000L)
            .row("Bob", 30000L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("name", "age")
            .allowExtraColumns()
            .row("Bob", 25L)
            .row("Alice", 30L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Skip first (age=25), should get age=30 and age=35
        expect("age").allowExtraColumns().row(30L).row(35L).assertMatches(stream);
      }
    }
  }

  @Test
  void testCollect() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM employees ORDER BY name");
          SendableRecordBatchStream stream = df.collect(allocator)) {
        expect("name", "age")
            .allowExtraColumns()
            .row("Alice", 30L)
            .row("Bob", 25L)
            .row("Charlie", 35L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = joined.executeStream(allocator)) {
        expect("name", "age", "location")
            .allowExtraColumns()
            .row("Alice", 30L, "SF")
            .row("Bob", 25L, "NYC")
            .row("Charlie", 35L, "SF")
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = joined.executeStream(allocator)) {
        expect("name", "age")
            .allowExtraColumns()
            .row("Alice", 30L)
            .row("Bob", 25L)
            .row("Charlie", 35L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = joined.executeStream(allocator)) {
        // Only Engineering employees (SF): Alice and Charlie
        expect("name", "location")
            .allowExtraColumns()
            .row("Alice", "SF")
            .row("Charlie", "SF")
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = joined.executeStream(allocator)) {
        // Left join: all 3 employees retained
        expect("name", "location")
            .allowExtraColumns()
            .row("Alice", "SF")
            .row("Bob", "NYC")
            .row("Charlie", "SF")
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = unioned.executeStream(allocator)) {
        // Union ALL: Bob(25) + Alice(30) from df1 + Alice(30) + Charlie(35) from df2 = 4 rows
        expect("name", "age")
            .row("Bob", 25L)
            .row("Alice", 30L)
            .row("Alice", 30L)
            .row("Charlie", 35L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = unioned.executeStream(allocator)) {
        // Union distinct: Bob(25) + Alice(30) + Charlie(35) = 3 unique rows
        expect("name", "age")
            .row("Bob", 25L)
            .row("Alice", 30L)
            .row("Charlie", 35L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = intersected.executeStream(allocator)) {
        // Intersection: only Alice(30) is in both
        expect("name", "age").row("Alice", 30L).assertMatches(stream);
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
          SendableRecordBatchStream stream = excepted.executeStream(allocator)) {
        // Except: all employees minus Alice(30) = Bob(25) and Charlie(35)
        expect("name", "age").row("Bob", 25L).row("Charlie", 35L).assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // 2 distinct departments: Engineering and Sales
        expect("dept").row("Engineering").row("Sales").assertMatches(stream);
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

      try (DataFrame df = ctx.table("employees").orElseThrow().sort(col("name").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        Schema schema = df.schema();
        assertEquals(4, schema.getFields().size());

        expect("name", "age")
            .allowExtraColumns()
            .row("Alice", 30L)
            .row("Bob", 25L)
            .row("Charlie", 35L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testTableNotFound() {
    try (SessionContext ctx = new SessionContext()) {
      assertTrue(ctx.table("nonexistent_table").isEmpty());
      assertFalse(ctx.tableExist("nonexistent_table"));
    }
  }

  @Test
  void testTableExist() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      assertFalse(ctx.tableExist("employees"));
      registerEmployees(ctx, allocator);
      assertTrue(ctx.tableExist("employees"));
    }
  }

  @Test
  void testReadCsv(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.writeString(csvFile, "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readCsv(csvFile.toString()).sort(col("id").sortAsc());
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      expect("id", "name", "value")
          .row(1L, "Alice", 100L)
          .row(2L, "Bob", 200L)
          .row(3L, "Charlie", 300L)
          .assertMatches(stream);
    }
  }

  @Test
  void testReadJson(@TempDir Path tempDir) throws IOException {
    Path jsonFile = tempDir.resolve("test.json");
    Files.writeString(jsonFile, "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.readJson(jsonFile.toString()).sort(col("id").sortAsc());
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      expect("id", "name").row(1L, "Alice").row(2L, "Bob").assertMatches(stream);
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
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      expect("id", "value")
          .allowExtraColumns()
          .row(1L, 100L)
          .row(2L, 200L)
          .row(3L, 300L)
          .assertMatches(stream);
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
      try (DataFrame df = ctx.readParquet(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age", "name")
            .allowExtraColumns()
            .row(25L, "Bob")
            .row(30L, "Alice")
            .row(35L, "Charlie")
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age").allowExtraColumns().row(25L).row(30L).row(35L).assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age", "name", "salary")
            .allowExtraColumns()
            .row(25L, "Bob", 30000L)
            .row(30L, "Alice", 50000L)
            .row(35L, "Charlie", 70000L)
            .assertMatches(stream);
      }
    }
  }

  // ==========================================================================
  // Phase 4b: Write operations with options
  // ==========================================================================

  @Test
  void testWriteParquetWithOptions(@TempDir Path tempDir) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_opts.parquet");
      DataFrameWriteOptions writeOpts =
          DataFrameWriteOptions.builder().singleFileOutput(true).build();
      ParquetOptions parquetOpts =
          ParquetOptions.builder().compression("SNAPPY").maxRowGroupSize(100L).build();
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeParquet(outDir.toString(), writeOpts, parquetOpts);
      }

      // Read back and verify data
      try (DataFrame df = ctx.readParquet(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age", "name")
            .row(25L, "Bob")
            .row(30L, "Alice")
            .row(35L, "Charlie")
            .allowExtraColumns()
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testWriteParquetWithWriteOptionsOnly(@TempDir Path tempDir) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_wo.parquet");
      DataFrameWriteOptions writeOpts = DataFrameWriteOptions.builder().build();
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeParquet(outDir.toString(), writeOpts);
      }

      // Read back and verify data
      try (DataFrame df = ctx.readParquet(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age").row(25L).row(30L).row(35L).allowExtraColumns().assertMatches(stream);
      }
    }
  }

  @Test
  void testWriteCsvWithOptions(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_opts.csv");
      DataFrameWriteOptions writeOpts = DataFrameWriteOptions.builder().build();
      CsvOptions csvOpts = CsvOptions.builder().delimiter((byte) '\t').build();
      try (DataFrame df = ctx.sql("SELECT age, salary FROM employees")) {
        df.writeCsv(outDir.toString(), writeOpts, csvOpts);
      }

      // Read back with tab delimiter and verify data
      CsvReadOptions readOpts = CsvReadOptions.builder().delimiter((byte) '\t').build();
      try (DataFrame df =
              ctx.readCsv(outDir.toString(), readOpts, allocator).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age", "salary")
            .row(25L, 30000L)
            .row(30L, 50000L)
            .row(35L, 70000L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testWriteCsvWithWriteOptionsOnly(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_wo.csv");
      DataFrameWriteOptions writeOpts = DataFrameWriteOptions.builder().build();
      try (DataFrame df = ctx.sql("SELECT age, salary FROM employees")) {
        df.writeCsv(outDir.toString(), writeOpts);
      }

      // Read back and verify
      try (DataFrame df = ctx.readCsv(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age").row(25L).row(30L).row(35L).allowExtraColumns().assertMatches(stream);
      }
    }
  }

  @Test
  void testWriteJsonWithOptions(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_opts.json");
      DataFrameWriteOptions writeOpts = DataFrameWriteOptions.builder().build();
      JsonOptions jsonOpts = JsonOptions.builder().build();
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeJson(outDir.toString(), writeOpts, jsonOpts);
      }

      // Read back and verify
      try (DataFrame df = ctx.readJson(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age", "name", "salary")
            .row(25L, "Bob", 30000L)
            .row(30L, "Alice", 50000L)
            .row(35L, "Charlie", 70000L)
            .allowExtraColumns()
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testWriteJsonWithWriteOptionsOnly(@TempDir Path tempDir) throws IOException {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerEmployees(ctx, allocator);

      Path outDir = tempDir.resolve("output_wo.json");
      DataFrameWriteOptions writeOpts = DataFrameWriteOptions.builder().build();
      try (DataFrame df = ctx.sql("SELECT * FROM employees")) {
        df.writeJson(outDir.toString(), writeOpts);
      }

      // Read back and verify
      try (DataFrame df = ctx.readJson(outDir.toString()).sort(col("age").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("age").row(25L).row(30L).row(35L).allowExtraColumns().assertMatches(stream);
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
              ctx.sql("SELECT * FROM employees").withColumn("bonus", col("salary").mul(lit(0.1)));
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        Schema schema = stream.getVectorSchemaRoot().getSchema();
        // Original 4 + bonus = 5 columns
        assertEquals(5, schema.getFields().size());
        assertEquals("bonus", schema.getFields().get(4).getName());

        // Alice(50000)->5000, Bob(30000)->3000, Charlie(70000)->7000
        expect("name", "bonus")
            .row("Alice", 5000.0)
            .row("Bob", 3000.0)
            .row("Charlie", 7000.0)
            .withDelta(0.01)
            .unordered()
            .allowExtraColumns()
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Highest salary among age>=30: Charlie(70000) > Alice(50000)
        expect("salary").row(70000L).allowExtraColumns().assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Engineering: avg(50000, 70000) = 60000, headcount = 2
        // Sales: avg(30000) = 30000, headcount = 1
        expect("dept", "avg_salary", "headcount")
            .row("Engineering", 60000.0, 2L)
            .row("Sales", 30000.0, 1L)
            .withDelta(0.01)
            .assertMatches(stream);
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
                  .orElseThrow()
                  .selectColumns("name", "salary")
                  .filter(col("salary").gt(lit(50000)));
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Only Charlie with salary > 50000
        expect("name", "salary").row("Charlie", 70000L).assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Alice(50000) -> mid, Bob(30000) -> low, Charlie(70000) -> high
        expect("name", "salary_band")
            .row("Alice", "mid")
            .row("Bob", "low")
            .row("Charlie", "high")
            .allowExtraColumns()
            .assertMatches(stream);
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
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      // East Widgets: 100 + 120 = 220; West Widgets: 150
      expect("region", "total").row("East", 220L).row("West", 150L).assertMatches(stream);
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
                      col("salary").mul(lit(12)).alias("annual_salary"),
                      col("age").add(lit(1)).alias("next_age"))
                  .sort(col("name").sortAsc());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("name", "annual_salary", "next_age")
            .row("Alice", 600000L, 31L)
            .row("Bob", 360000L, 26L)
            .row("Charlie", 840000L, 36L)
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.collect(allocator)) {
        // Skip first (Bob, age=25), should get Alice(30, 50000) and Charlie(35, 70000)
        expect("age", "salary")
            .row(30L, 50000L)
            .row(35L, 70000L)
            .allowExtraColumns()
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Engineering first (asc), highest salary first (desc): Charlie(70000), Alice(50000)
        // Sales last: Bob(30000)
        expect("dept", "name", "salary")
            .row("Engineering", "Charlie", 70000L)
            .row("Engineering", "Alice", 50000L)
            .row("Sales", "Bob", 30000L)
            .allowExtraColumns()
            .assertMatches(stream);
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
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // 50000 + 30000 + 70000 = 150000
        expect("total", "cnt", "youngest", "oldest")
            .row(150000L, 3L, 25L, 35L)
            .assertMatches(stream);
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

    ctx.registerBatch("employees", root, allocator);
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

    ctx.registerBatch("departments", root, allocator);
    root.close();
  }

  // ==========================================================================
  // Verify that String[] encoding (replacing null-separated bytes) works
  // correctly with multi-byte UTF-8 and special characters in column names.
  // Note: DataFusion's internal identifier handling may not support actual
  // null bytes (\0) in column names, but the FFI encoding layer must handle
  // them without corruption. We test with multi-byte UTF-8 characters to
  // verify the DiplomatStrSlice encoding works correctly.
  // ==========================================================================

  @Test
  void testJoinOnColumnsWithMultiByteUtf8Names() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      // Use multi-byte UTF-8 column names that would exercise the string encoding
      String unicodeCol = "col_\u00e9\u00e0\u00fc";

      Schema leftSchema =
          new Schema(
              Arrays.asList(
                  new Field(unicodeCol, FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field("val_left", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot leftRoot = VectorSchemaRoot.create(leftSchema, allocator);
      BigIntVector leftKey = (BigIntVector) leftRoot.getVector(unicodeCol);
      BigIntVector leftVal = (BigIntVector) leftRoot.getVector("val_left");
      leftKey.allocateNew(2);
      leftVal.allocateNew(2);
      leftKey.set(0, 1);
      leftVal.set(0, 10);
      leftKey.set(1, 2);
      leftVal.set(1, 20);
      leftKey.setValueCount(2);
      leftVal.setValueCount(2);
      leftRoot.setRowCount(2);
      ctx.registerBatch("left_tbl", leftRoot, allocator);
      leftRoot.close();

      Schema rightSchema =
          new Schema(
              Arrays.asList(
                  new Field(unicodeCol, FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field("val_right", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot rightRoot = VectorSchemaRoot.create(rightSchema, allocator);
      BigIntVector rightKey = (BigIntVector) rightRoot.getVector(unicodeCol);
      BigIntVector rightVal = (BigIntVector) rightRoot.getVector("val_right");
      rightKey.allocateNew(2);
      rightVal.allocateNew(2);
      rightKey.set(0, 1);
      rightVal.set(0, 100);
      rightKey.set(1, 3);
      rightVal.set(1, 300);
      rightKey.setValueCount(2);
      rightVal.setValueCount(2);
      rightRoot.setRowCount(2);
      ctx.registerBatch("right_tbl", rightRoot, allocator);
      rightRoot.close();

      try (DataFrame left = ctx.sql("SELECT * FROM left_tbl");
          DataFrame right = ctx.sql("SELECT * FROM right_tbl");
          DataFrame joined =
              left.join(right, JoinType.INNER, List.of(unicodeCol), List.of(unicodeCol));
          SendableRecordBatchStream stream = joined.executeStream(allocator)) {
        expect("val_left", "val_right").row(10L, 100L).allowExtraColumns().assertMatches(stream);
      }
    }
  }

  @Test
  void testDropColumnsWithMultiByteUtf8Names() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      String unicodeCol = "drop_\u00e9\u00e0\u00fc";
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("keep", FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field(unicodeCol, FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      BigIntVector keep = (BigIntVector) root.getVector("keep");
      BigIntVector drop = (BigIntVector) root.getVector(unicodeCol);
      keep.allocateNew(1);
      drop.allocateNew(1);
      keep.set(0, 42);
      drop.set(0, 99);
      keep.setValueCount(1);
      drop.setValueCount(1);
      root.setRowCount(1);
      ctx.registerBatch("test_tbl", root, allocator);
      root.close();

      try (DataFrame df = ctx.sql("SELECT * FROM test_tbl");
          DataFrame dropped = df.dropColumns(unicodeCol);
          SendableRecordBatchStream stream = dropped.executeStream(allocator)) {
        expect("keep").row(42L).assertMatches(stream);
      }
    }
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
