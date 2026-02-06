package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
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
      assertThrows(DataFusionException.class, () -> state.createLogicalPlan("NOT VALID SQL %%%"));
    }
  }

  @Test
  void testPlanReferencingRegisteredTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerTable("test_table", testData, allocator);

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
