package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.Functions.*;
import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.SessionState;
import org.apache.arrow.datafusion.logical_expr.JoinType;
import org.apache.arrow.datafusion.logical_expr.LogicalPlan;
import org.apache.arrow.datafusion.logical_expr.LogicalPlanBuilder;
import org.apache.arrow.datafusion.logical_expr.SortExpr;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for {@link LogicalPlanBuilder}. */
public class LogicalPlanBuilderTest {

  @Test
  void testFromPlanAndBuild() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      // Create a plan via SQL, then wrap it in a builder and build it back
      try (LogicalPlan sqlPlan = createPlan(ctx, "SELECT x, y FROM test_table ORDER BY x");
          LogicalPlan builtPlan = LogicalPlanBuilder.from(sqlPlan, ctx).build();
          DataFrame df = ctx.executeLogicalPlan(builtPlan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(1L, 10L).row(2L, 20L).row(3L, 30L).assertMatches(stream);
      }
    }
  }

  @Test
  void testProject() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx).project(List.of(col("x"))).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x").row(1L).row(2L).row(3L).unordered().assertMatches(stream);
      }
    }
  }

  @Test
  void testProjectVarargs() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx).project(col("y"), col("x")).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getSchema().getFields().size());
        assertEquals("y", root.getSchema().getFields().get(0).getName());
        assertEquals("x", root.getSchema().getFields().get(1).getName());
      }
    }
  }

  @Test
  void testFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx).filter(col("x").gt(lit(1))).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(2L, 20L).row(3L, 30L).unordered().assertMatches(stream);
      }
    }
  }

  @Test
  void testSort() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .sort(List.of(new SortExpr(col("x"), false, true)))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Sorted descending
        expect("x", "y").row(3L, 30L).row(2L, 20L).row(1L, 10L).assertMatches(stream);
      }
    }
  }

  @Test
  void testSortVarargs() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .sort(new SortExpr(col("x"), true, false))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(1L, 10L).row(2L, 20L).row(3L, 30L).assertMatches(stream);
      }
    }
  }

  @Test
  void testLimit() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table ORDER BY x");
          LogicalPlan plan = LogicalPlanBuilder.from(tablePlan, ctx).limit(1, 1).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(2L, 20L).assertMatches(stream);
      }
    }
  }

  @Test
  void testAggregate() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .aggregate(List.of(), List.of(sum(col("x"))))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("sum(test_table.x)").row(6L).assertMatches(stream);
      }
    }
  }

  @Test
  void testDistinct() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Register data with duplicates
      Schema schema =
          new Schema(
              List.of(new Field("v", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      BigIntVector vVector = (BigIntVector) root.getVector("v");
      vVector.allocateNew(4);
      vVector.set(0, 1);
      vVector.set(1, 2);
      vVector.set(2, 1);
      vVector.set(3, 2);
      vVector.setValueCount(4);
      root.setRowCount(4);
      ctx.registerBatch("dup_table", root, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM dup_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .distinct()
                  .sort(new SortExpr(col("v"), true, false))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.collect(allocator)) {
        expect("v").row(1L).row(2L).assertMatches(stream);
      }

      root.close();
    }
  }

  @Test
  void testJoin() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);
      registerTestData2(ctx, allocator);

      try (LogicalPlan leftPlan = createPlan(ctx, "SELECT * FROM test_table ORDER BY x");
          LogicalPlan rightPlan = createPlan(ctx, "SELECT * FROM test_table2 ORDER BY a");
          LogicalPlan plan =
              LogicalPlanBuilder.from(leftPlan, ctx)
                  .join(rightPlan, JoinType.INNER, List.of("x"), List.of("a"))
                  .sort(new SortExpr(col("x"), true, false))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // test_table has x=1,2,3; test_table2 has a=2,3,4; inner join matches 2,3
        expect("x", "a").row(2L, 2L).row(3L, 3L).allowExtraColumns().assertMatches(stream);
      }
    }
  }

  @Test
  void testCrossJoin() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);
      registerTestData2(ctx, allocator);

      try (LogicalPlan leftPlan = createPlan(ctx, "SELECT x FROM test_table");
          LogicalPlan rightPlan = createPlan(ctx, "SELECT a FROM test_table2");
          LogicalPlan plan = LogicalPlanBuilder.from(leftPlan, ctx).crossJoin(rightPlan).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.collect(allocator)) {
        // Cross join: test_table.x={1,2,3} × test_table2.a={2,3,4} = 9 rows
        expect("x", "a")
            .row(1L, 2L)
            .row(1L, 3L)
            .row(1L, 4L)
            .row(2L, 2L)
            .row(2L, 3L)
            .row(2L, 4L)
            .row(3L, 2L)
            .row(3L, 3L)
            .row(3L, 4L)
            .unordered()
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testUnion() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan plan1 = createPlan(ctx, "SELECT x FROM test_table WHERE x <= 2");
          LogicalPlan plan2 = createPlan(ctx, "SELECT x FROM test_table WHERE x >= 2");
          LogicalPlan plan =
              LogicalPlanBuilder.from(plan1, ctx)
                  .union(plan2)
                  .sort(new SortExpr(col("x"), true, false))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Union ALL: x=1,2 + x=2,3 = 4 rows
        expect("x").row(1L).row(2L).row(2L).row(3L).assertMatches(stream);
      }
    }
  }

  @Test
  void testUnionDistinct() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan plan1 = createPlan(ctx, "SELECT x FROM test_table WHERE x <= 2");
          LogicalPlan plan2 = createPlan(ctx, "SELECT x FROM test_table WHERE x >= 2");
          LogicalPlan plan =
              LogicalPlanBuilder.from(plan1, ctx)
                  .unionDistinct(plan2)
                  .sort(new SortExpr(col("x"), true, false))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Union DISTINCT: {1,2} U {2,3} = {1,2,3}
        expect("x").row(1L).row(2L).row(3L).assertMatches(stream);
      }
    }
  }

  @Test
  void testIntersect() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan plan1 = createPlan(ctx, "SELECT x FROM test_table WHERE x <= 2");
          LogicalPlan plan2 = createPlan(ctx, "SELECT x FROM test_table WHERE x >= 2");
          LogicalPlan plan = LogicalPlanBuilder.intersect(plan1, plan2, false);
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x").row(2L).assertMatches(stream);
      }
    }
  }

  @Test
  void testExcept() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan plan1 = createPlan(ctx, "SELECT x FROM test_table");
          LogicalPlan plan2 = createPlan(ctx, "SELECT x FROM test_table WHERE x >= 2");
          LogicalPlan plan = LogicalPlanBuilder.except(plan1, plan2, false);
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x").row(1L).assertMatches(stream);
      }
    }
  }

  @Test
  void testAlias() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT x FROM test_table ORDER BY x");
          LogicalPlan plan = LogicalPlanBuilder.from(tablePlan, ctx).alias("my_alias").build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x").row(1L).row(2L).row(3L).assertMatches(stream);
      }
    }
  }

  @Test
  void testExplain() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan = LogicalPlanBuilder.from(tablePlan, ctx).explain(false, false).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        // Explain output is plan-text dependent, so only verify shape.
        assertTrue(root.getRowCount() > 0);
        assertNotNull(root.getVector("plan_type"));
        assertNotNull(root.getVector("plan"));
      }
    }
  }

  @Test
  void testHaving() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create data with groups
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("grp", FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field("val", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      BigIntVector grpVec = (BigIntVector) root.getVector("grp");
      BigIntVector valVec = (BigIntVector) root.getVector("val");
      grpVec.allocateNew(4);
      valVec.allocateNew(4);
      grpVec.set(0, 1);
      grpVec.set(1, 1);
      grpVec.set(2, 2);
      grpVec.set(3, 2);
      valVec.set(0, 10);
      valVec.set(1, 20);
      valVec.set(2, 5);
      valVec.set(3, 5);
      grpVec.setValueCount(4);
      valVec.setValueCount(4);
      root.setRowCount(4);
      ctx.registerBatch("grp_table", root, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM grp_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .aggregate(List.of(col("grp")), List.of(sum(col("val"))))
                  .having(col("sum(grp_table.val)").gt(lit(15)))
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("grp").row(1L).allowExtraColumns().assertMatches(stream);
      }

      root.close();
    }
  }

  @Test
  void testFluentChaining() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      registerTestData(ctx, allocator);

      try (LogicalPlan tablePlan = createPlan(ctx, "SELECT * FROM test_table");
          LogicalPlan plan =
              LogicalPlanBuilder.from(tablePlan, ctx)
                  .filter(col("x").gt(lit(1)))
                  .project(List.of(col("x"), col("y")))
                  .sort(List.of(new SortExpr(col("x"), false, true)))
                  .limit(0, 1)
                  .build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(3L, 30L).assertMatches(stream);
      }
    }
  }

  @Test
  void testEmpty() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      try (LogicalPlan plan = LogicalPlanBuilder.empty(false, ctx).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // Empty relation with produce_one_row=false should produce no rows
        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testEmptyProducesOneRow() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      try (LogicalPlan plan = LogicalPlanBuilder.empty(true, ctx).build();
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        assertEquals(0, root.getSchema().getFields().size());
      }
    }
  }

  // ── Helpers ──

  private void registerTestData(SessionContext ctx, BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("y", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    BigIntVector xVector = (BigIntVector) root.getVector("x");
    BigIntVector yVector = (BigIntVector) root.getVector("y");
    xVector.allocateNew(3);
    yVector.allocateNew(3);
    xVector.set(0, 1);
    xVector.set(1, 2);
    xVector.set(2, 3);
    yVector.set(0, 10);
    yVector.set(1, 20);
    yVector.set(2, 30);
    xVector.setValueCount(3);
    yVector.setValueCount(3);
    root.setRowCount(3);

    ctx.registerBatch("test_table", root, allocator);
    root.close();
  }

  private void registerTestData2(SessionContext ctx, BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("a", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("b", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    BigIntVector aVector = (BigIntVector) root.getVector("a");
    BigIntVector bVector = (BigIntVector) root.getVector("b");
    aVector.allocateNew(3);
    bVector.allocateNew(3);
    aVector.set(0, 2);
    aVector.set(1, 3);
    aVector.set(2, 4);
    bVector.set(0, 200);
    bVector.set(1, 300);
    bVector.set(2, 400);
    aVector.setValueCount(3);
    bVector.setValueCount(3);
    root.setRowCount(3);

    ctx.registerBatch("test_table2", root, allocator);
    root.close();
  }

  // ==========================================================================
  // Null-byte-in-string tests
  // ==========================================================================

  @Test
  void testJoinWithMultiByteUtf8ColumnName() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      String colWithNull = "key_éàü";

      // Register left table
      Schema leftSchema =
          new Schema(
              Arrays.asList(
                  new Field(colWithNull, FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field("left_val", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot leftRoot = VectorSchemaRoot.create(leftSchema, allocator);
      BigIntVector leftKey = (BigIntVector) leftRoot.getVector(colWithNull);
      BigIntVector leftVal = (BigIntVector) leftRoot.getVector("left_val");
      leftKey.allocateNew(1);
      leftVal.allocateNew(1);
      leftKey.set(0, 1);
      leftVal.set(0, 10);
      leftKey.setValueCount(1);
      leftVal.setValueCount(1);
      leftRoot.setRowCount(1);
      ctx.registerBatch("left_tbl", leftRoot, allocator);
      leftRoot.close();

      // Register right table
      Schema rightSchema =
          new Schema(
              Arrays.asList(
                  new Field(colWithNull, FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field("right_val", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      VectorSchemaRoot rightRoot = VectorSchemaRoot.create(rightSchema, allocator);
      BigIntVector rightKey = (BigIntVector) rightRoot.getVector(colWithNull);
      BigIntVector rightVal = (BigIntVector) rightRoot.getVector("right_val");
      rightKey.allocateNew(1);
      rightVal.allocateNew(1);
      rightKey.set(0, 1);
      rightVal.set(0, 100);
      rightKey.setValueCount(1);
      rightVal.setValueCount(1);
      rightRoot.setRowCount(1);
      ctx.registerBatch("right_tbl", rightRoot, allocator);
      rightRoot.close();

      // Use LogicalPlanBuilder to join on the multi-byte UTF-8 column
      try (LogicalPlan leftPlan = createPlan(ctx, "SELECT * FROM left_tbl");
          LogicalPlan rightPlan = createPlan(ctx, "SELECT * FROM right_tbl");
          LogicalPlanBuilder builder = LogicalPlanBuilder.from(leftPlan, ctx);
          LogicalPlanBuilder joined =
              builder.join(rightPlan, JoinType.INNER, List.of(colWithNull), List.of(colWithNull));
          LogicalPlan joinedPlan = joined.build();
          DataFrame df = ctx.executeLogicalPlan(joinedPlan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("left_val", "right_val").row(10L, 100L).allowExtraColumns().assertMatches(stream);
      }
    }
  }

  /** Creates a LogicalPlan from SQL, properly closing the intermediate SessionState. */
  private LogicalPlan createPlan(SessionContext ctx, String sql) {
    try (SessionState state = ctx.state()) {
      return state.createLogicalPlan(sql);
    }
  }
}
