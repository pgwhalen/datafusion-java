package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.SessionState;
import org.apache.arrow.datafusion.logical_expr.LogicalPlan;
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

/** Tests for {@link SessionState} and {@link LogicalPlan}. */
public class SessionStateTest {

  @Test
  void testCreateLogicalPlanFromSimpleSql() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1")) {
      assertNotNull(plan);
    }
  }

  @Test
  void testInvalidSqlThrowsException() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state()) {
      assertThrows(DataFusionError.class, () -> state.createLogicalPlan("NOT VALID SQL %%%"));
    }
  }

  @Test
  void testPlanReferencingRegisteredTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerBatch("test_table", testData, allocator);

      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT x, y FROM test_table WHERE x > 1")) {
        assertNotNull(plan);
      }

      testData.close();
    }
  }

  @Test
  void testMultiplePlansFromSameState() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state()) {

      try (LogicalPlan plan1 = state.createLogicalPlan("SELECT 1");
          LogicalPlan plan2 = state.createLogicalPlan("SELECT 2");
          LogicalPlan plan3 = state.createLogicalPlan("SELECT 1 + 2")) {
        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
      }
    }
  }

  @Test
  void testStateOutlivesContext() {
    SessionState state;

    try (SessionContext ctx = new SessionContext()) {
      state = ctx.state();
    }
    // SessionContext is now closed

    // State should still work for planning SQL after context is closed
    try (state;
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 + 2 AS result")) {
      assertNotNull(plan);
    }
  }

  @Test
  void testExecuteLogicalPlan() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 + 1 AS result");
        DataFrame df = ctx.executeLogicalPlan(plan);
        SendableRecordBatchStream stream = df.executeStream(allocator)) {
      assertTrue(stream.loadNextBatch());
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertEquals(1, root.getRowCount());
      BigIntVector col = (BigIntVector) root.getVector("result");
      assertEquals(2, col.get(0));
      assertFalse(stream.loadNextBatch());
    }
  }

  @Test
  void testExecuteSamePlanTwice() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 42 AS answer")) {

      // Execute the plan twice to verify clone semantics
      try (DataFrame df1 = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream1 = df1.executeStream(allocator)) {
        assertTrue(stream1.loadNextBatch());
        VectorSchemaRoot root1 = stream1.getVectorSchemaRoot();
        BigIntVector col1 = (BigIntVector) root1.getVector("answer");
        assertEquals(42, col1.get(0));
      }

      try (DataFrame df2 = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream2 = df2.executeStream(allocator)) {
        assertTrue(stream2.loadNextBatch());
        VectorSchemaRoot root2 = stream2.getVectorSchemaRoot();
        BigIntVector col2 = (BigIntVector) root2.getVector("answer");
        assertEquals(42, col2.get(0));
      }
    }
  }

  @Test
  void testExecutePlanWithRegisteredTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerBatch("test_table", testData, allocator);

      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT x, y FROM test_table WHERE x > 1 ORDER BY x");
          DataFrame df = ctx.executeLogicalPlan(plan);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch());
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(2, root.getRowCount());
        BigIntVector xCol = (BigIntVector) root.getVector("x");
        BigIntVector yCol = (BigIntVector) root.getVector("y");
        assertEquals(2, xCol.get(0));
        assertEquals(20, yCol.get(0));
        assertEquals(3, xCol.get(1));
        assertEquals(30, yCol.get(1));
        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  private VectorSchemaRoot createTestData(BufferAllocator allocator) {
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

    return root;
  }
}
