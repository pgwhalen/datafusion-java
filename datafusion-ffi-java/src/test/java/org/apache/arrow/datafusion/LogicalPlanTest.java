package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.OptionalLong;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.SessionState;
import org.apache.arrow.datafusion.logical_expr.*;
import org.apache.arrow.datafusion.logical_expr.LogicalPlanBuilder;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for the LogicalPlan sealed interface variant hierarchy. */
public class LogicalPlanTest {

  // ── Variant identification ──

  @Test
  void testSelectLiteralIsProjectionOverEmptyRelation() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 AS x")) {
      assertInstanceOf(LogicalPlan.Projection.class, plan);
      LogicalPlan.Projection proj = (LogicalPlan.Projection) plan;
      assertFalse(proj.exprs().isEmpty(), "projection should have expressions");
      assertEquals(1, proj.inputs().size());
      assertInstanceOf(LogicalPlan.EmptyRelation.class, proj.input());
      LogicalPlan.EmptyRelation empty = (LogicalPlan.EmptyRelation) proj.input();
      assertTrue(empty.produceOneRow());
    }
  }

  @Test
  void testFilterVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t WHERE id > 1")) {
        // Plan could be Projection(Filter(TableScan)) - walk down to find Filter
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Filter) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Filter.class, current);
        LogicalPlan.Filter filter = (LogicalPlan.Filter) current;
        assertNotNull(filter.predicate());
        assertFalse(filter.inputs().isEmpty());
      }
    }
  }

  @Test
  void testSortVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t ORDER BY id")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Sort) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Sort.class, current);
        LogicalPlan.Sort sort = (LogicalPlan.Sort) current;
        assertFalse(sort.sortExprs().isEmpty(), "should have sort expressions");
        assertEquals(OptionalLong.empty(), sort.fetch());
      }
    }
  }

  @Test
  void testLimitVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t LIMIT 5")) {
        // Walk down to find Limit or Sort with fetch
        LogicalPlan current = plan;
        boolean foundLimit = false;
        while (current != null) {
          if (current instanceof LogicalPlan.Limit limit) {
            assertTrue(limit.fetchExpr().isPresent(), "fetch should be present for LIMIT 5");
            foundLimit = true;
            break;
          }
          if (current instanceof LogicalPlan.Sort sort && sort.fetch().isPresent()) {
            assertEquals(5L, sort.fetch().getAsLong());
            foundLimit = true;
            break;
          }
          if (current.inputs().isEmpty()) break;
          current = current.inputs().get(0);
        }
        assertTrue(foundLimit, "should find a Limit or Sort-with-fetch node");
      }
    }
  }

  @Test
  void testJoinVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator, "t1");
      registerTestTable(ctx, allocator, "t2");
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Join) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Join.class, current);
        LogicalPlan.Join join = (LogicalPlan.Join) current;
        assertEquals(JoinType.INNER, join.joinType());
        assertEquals(2, join.inputs().size());
      }
    }
  }

  @Test
  void testAggregateVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT name, count(*) FROM t GROUP BY name")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Aggregate) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Aggregate.class, current);
        LogicalPlan.Aggregate agg = (LogicalPlan.Aggregate) current;
        assertFalse(agg.groupExprs().isEmpty(), "should have group expressions");
        assertFalse(agg.aggrExprs().isEmpty(), "should have aggregate expressions");
      }
    }
  }

  @Test
  void testExplainVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("EXPLAIN SELECT * FROM t")) {
        assertInstanceOf(LogicalPlan.Explain.class, plan);
        LogicalPlan.Explain explain = (LogicalPlan.Explain) plan;
        assertFalse(explain.verbose());
        assertNotNull(explain.plan());
      }
    }
  }

  @Test
  void testValuesVariant() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("VALUES (1, 'a'), (2, 'b')")) {
      // Values might be wrapped in Projection
      LogicalPlan current = plan;
      while (!(current instanceof LogicalPlan.Values) && !current.inputs().isEmpty()) {
        current = current.inputs().get(0);
      }
      assertInstanceOf(LogicalPlan.Values.class, current);
      LogicalPlan.Values values = (LogicalPlan.Values) current;
      assertEquals(2, values.values().size(), "should have 2 rows");
      assertEquals(2, values.values().get(0).size(), "each row should have 2 columns");
    }
  }

  @Test
  void testTableScanVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t")) {
        // Walk down to find TableScan
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.TableScan) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.TableScan.class, current);
        LogicalPlan.TableScan scan = (LogicalPlan.TableScan) current;
        assertInstanceOf(TableReference.Bare.class, scan.tableName());
        assertEquals("t", ((TableReference.Bare) scan.tableName()).table());
        assertTrue(scan.inputs().isEmpty(), "TableScan has no children");
      }
    }
  }

  @Test
  void testDistinctVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT DISTINCT name FROM t")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Distinct) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Distinct.class, current);
      }
    }
  }

  @Test
  void testSubqueryAliasVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM (SELECT * FROM t) AS sub")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.SubqueryAlias) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.SubqueryAlias.class, current);
        LogicalPlan.SubqueryAlias alias = (LogicalPlan.SubqueryAlias) current;
        assertEquals("sub", alias.alias());
      }
    }
  }

  @Test
  void testUnionVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT id, name FROM t UNION ALL SELECT id, name FROM t")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Union) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Union.class, current);
        LogicalPlan.Union union = (LogicalPlan.Union) current;
        assertEquals(2, union.inputs().size(), "UNION ALL should have 2 inputs");
      }
    }
  }

  // ── Common methods ──

  @Test
  void testSchemaReturnsCorrectFields() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 AS x, 'hello' AS y")) {
      Schema schema = plan.schema();
      assertNotNull(schema);
      assertEquals(2, schema.getFields().size());
      assertEquals("x", schema.getFields().get(0).getName());
      assertEquals("y", schema.getFields().get(1).getName());
    }
  }

  @Test
  void testDisplayIndentProducesMultiLineOutput() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t WHERE id > 1")) {
        String display = plan.displayIndent();
        assertNotNull(display);
        assertTrue(display.contains("\n"), "display_indent should be multi-line");
      }
    }
  }

  @Test
  void testDisplayGraphvizContainsDot() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1")) {
      String dot = plan.displayGraphviz();
      assertTrue(dot.contains("digraph"), "graphviz output should contain 'digraph'");
    }
  }

  @Test
  void testDisplayPgJsonContainsJson() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1")) {
      String json = plan.displayPgJson();
      assertTrue(json.startsWith("["), "pg_json output should start with '['");
    }
  }

  @Test
  void testDisplaySingle() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 AS x")) {
      String display = plan.display();
      assertNotNull(display);
      assertFalse(display.isEmpty());
    }
  }

  // ── Execution still works ──

  @Test
  void testSealedInterfacePlanCanBeExecuted() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT id, name FROM t ORDER BY id")) {
        // Verify it's a real sealed interface variant
        assertInstanceOf(LogicalPlan.class, plan);
        // Execute via SessionContext
        try (DataFrame df = ctx.executeLogicalPlan(plan);
            SendableRecordBatchStream stream = df.collect(allocator)) {
          assertTrue(stream.loadNextBatch());
          VectorSchemaRoot root = stream.getVectorSchemaRoot();
          assertEquals(3, root.getRowCount());
          IntVector idVec = (IntVector) root.getVector("id");
          assertEquals(1, idVec.get(0));
          assertEquals(2, idVec.get(1));
          assertEquals(3, idVec.get(2));
        }
      }
    }
  }

  @Test
  void testBuilderPlanIsSealed() {
    try (SessionContext ctx = new SessionContext();
        LogicalPlanBuilder builder =
            LogicalPlanBuilder.empty(true, ctx).project(List.of(Functions.lit(42).alias("x")));
        LogicalPlan plan = builder.build()) {
      assertInstanceOf(LogicalPlan.class, plan);
      assertNotNull(plan.schema());
      assertEquals(1, plan.schema().getFields().size());
    }
  }

  // ── Helper ──

  private void registerTestTable(SessionContext ctx, BufferAllocator allocator) {
    registerTestTable(ctx, allocator, "t");
  }

  private void registerTestTable(SessionContext ctx, BufferAllocator allocator, String name) {
    Schema schema =
        new Schema(
            List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector idVec = (IntVector) root.getVector("id");
      VarCharVector nameVec = (VarCharVector) root.getVector("name");
      root.setRowCount(3);
      idVec.allocateNew(3);
      nameVec.allocateNew(3);
      idVec.set(0, 1);
      nameVec.set(0, "Alice".getBytes());
      idVec.set(1, 2);
      nameVec.set(1, "Bob".getBytes());
      idVec.set(2, 3);
      nameVec.set(2, "Charlie".getBytes());
      idVec.setValueCount(3);
      nameVec.setValueCount(3);
      ctx.registerBatch(name, root, allocator);
    }
  }
}
