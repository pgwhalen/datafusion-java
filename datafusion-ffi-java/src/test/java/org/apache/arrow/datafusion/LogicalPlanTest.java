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
  void testDisplayIndent() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t WHERE id > 1")) {
        String expected =
            """
            Projection: t.id, t.name
              Filter: t.id > Int64(1)
                TableScan: t""";
        assertEquals(expected, plan.displayIndent());
      }
    }
  }

  @Test
  void testDisplayGraphviz() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1")) {
      String expected =
          """

          // Begin DataFusion GraphViz Plan,
          // display it online here: https://dreampuf.github.io/GraphvizOnline

          digraph {
            subgraph cluster_1
            {
              graph[label="LogicalPlan"]
              2[shape=box label="Projection: Int64(1)"]
              3[shape=box label="EmptyRelation: rows=1"]
              2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]
            }
            subgraph cluster_4
            {
              graph[label="Detailed LogicalPlan"]
              5[shape=box label="Projection: Int64(1)\\nSchema: [Int64(1):Int64]"]
              6[shape=box label="EmptyRelation: rows=1\\nSchema: []"]
              5 -> 6 [arrowhead=none, arrowtail=normal, dir=back]
            }
          }
          // End DataFusion GraphViz Plan
          """;
      assertEquals(expected, plan.displayGraphviz());
    }
  }

  @Test
  void testDisplayPgJson() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1")) {
      String expected =
          """
          [
            {
              "Plan": {
                "Expressions": [
                  "Int64(1)"
                ],
                "Node Type": "Projection",
                "Output": [
                  "Int64(1)"
                ],
                "Plans": [
                  {
                    "Node Type": "EmptyRelation",
                    "Output": [],
                    "Plans": []
                  }
                ]
              }
            }
          ]""";
      assertEquals(expected, plan.displayPgJson());
    }
  }

  @Test
  void testDisplaySingle() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 AS x")) {
      assertEquals("Projection: Int64(1) AS x", plan.display());
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

  // ── Common diagnostic methods ──

  @Test
  void testDisplayIndentSchema() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t WHERE id > 1")) {
        String expected =
            """
            Projection: t.id, t.name [id:Int32, name:Utf8;N]
              Filter: t.id > Int64(1) [id:Int32, name:Utf8;N]
                TableScan: t [id:Int32, name:Utf8;N]""";
        assertEquals(expected, plan.displayIndentSchema());
      }
    }
  }

  @Test
  void testMaxRowsOnValues() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("VALUES (1), (2), (3)")) {
      LogicalPlan current = plan;
      while (!(current instanceof LogicalPlan.Values) && !current.inputs().isEmpty()) {
        current = current.inputs().get(0);
      }
      assertInstanceOf(LogicalPlan.Values.class, current);
      OptionalLong maxRows = current.maxRows();
      assertTrue(maxRows.isPresent(), "Values node should know its max row count");
      assertEquals(3L, maxRows.getAsLong());
    }
  }

  @Test
  void testMaxRowsOnLimit() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t LIMIT 5")) {
        OptionalLong maxRows = plan.maxRows();
        assertTrue(maxRows.isPresent(), "LIMIT plan should have known max rows");
        assertEquals(5L, maxRows.getAsLong());
      }
    }
  }

  @Test
  void testContainsOuterReferenceIsFalse() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SELECT 1 AS x")) {
      // A simple literal plan has no outer references.
      // Testing the true case would require an intermediate correlated subquery node
      // before decorrelation, which is not easily produced from createLogicalPlan.
      assertFalse(plan.containsOuterReference());
    }
  }

  // ── New variant tests ──

  @Test
  void testWindowVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Window) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Window.class, current);
        LogicalPlan.Window window = (LogicalPlan.Window) current;
        assertFalse(window.windowExprs().isEmpty(), "should have window expressions");
        assertInstanceOf(Expr.WindowFunctionExpr.class, window.windowExprs().get(0));
        Expr.WindowFunctionExpr wfExpr = (Expr.WindowFunctionExpr) window.windowExprs().get(0);
        assertEquals("row_number", wfExpr.funcName());
        assertFalse(wfExpr.orderBy().isEmpty(), "should have ORDER BY");
        assertTrue(wfExpr.partitionBy().isEmpty(), "should have no PARTITION BY");
      }
    }
  }

  @Test
  void testAnalyzeVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("EXPLAIN ANALYZE SELECT * FROM t")) {
        assertInstanceOf(LogicalPlan.Analyze.class, plan);
        LogicalPlan.Analyze analyze = (LogicalPlan.Analyze) plan;
        assertFalse(analyze.verbose());
        assertNotNull(analyze.input());
        assertNotNull(analyze.schema());
      }
    }
  }

  @Test
  void testRecursiveQueryVariant() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan =
            state.createLogicalPlan(
                "WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte")) {
      LogicalPlan current = plan;
      while (!(current instanceof LogicalPlan.RecursiveQuery) && !current.inputs().isEmpty()) {
        current = current.inputs().get(0);
      }
      assertInstanceOf(LogicalPlan.RecursiveQuery.class, current);
      LogicalPlan.RecursiveQuery rq = (LogicalPlan.RecursiveQuery) current;
      assertEquals("cte", rq.name());
      assertFalse(rq.isDistinct(), "UNION ALL should not be distinct");
      assertNotNull(rq.staticTerm());
      assertNotNull(rq.recursiveTerm());
      assertEquals(2, rq.inputs().size(), "should have static and recursive inputs");
    }
  }

  @Test
  void testDdlCreateViewVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("CREATE VIEW my_view AS SELECT * FROM t")) {
        assertInstanceOf(LogicalPlan.Ddl.CreateView.class, plan);
        LogicalPlan.Ddl.CreateView cv = (LogicalPlan.Ddl.CreateView) plan;
        assertEquals("my_view", ((TableReference.Bare) cv.name()).table());
        assertFalse(cv.orReplace());
        assertFalse(cv.temporary());
        assertNotNull(cv.input());
        assertNotNull(cv.schema());
      }
    }
  }

  @Test
  void testDdlDropTableVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("DROP TABLE IF EXISTS t")) {
        assertInstanceOf(LogicalPlan.Ddl.DropTable.class, plan);
        LogicalPlan.Ddl.DropTable dt = (LogicalPlan.Ddl.DropTable) plan;
        assertEquals("t", ((TableReference.Bare) dt.name()).table());
        assertTrue(dt.ifExists());
        assertTrue(dt.inputs().isEmpty());
      }
    }
  }

  @Test
  void testDdlDropViewVariant() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("DROP VIEW IF EXISTS my_view")) {
      assertInstanceOf(LogicalPlan.Ddl.DropView.class, plan);
      LogicalPlan.Ddl.DropView dv = (LogicalPlan.Ddl.DropView) plan;
      assertEquals("my_view", ((TableReference.Bare) dv.name()).table());
      assertTrue(dv.ifExists());
    }
  }

  @Test
  void testDdlCreateSchema() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("CREATE SCHEMA IF NOT EXISTS my_schema")) {
      assertInstanceOf(LogicalPlan.Ddl.CreateCatalogSchema.class, plan);
      LogicalPlan.Ddl.CreateCatalogSchema cs = (LogicalPlan.Ddl.CreateCatalogSchema) plan;
      assertEquals("my_schema", cs.schemaName());
      assertTrue(cs.ifNotExists());
      assertTrue(cs.inputs().isEmpty());
    }
  }

  @Test
  void testDdlCreateDatabase() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("CREATE DATABASE IF NOT EXISTS my_db")) {
      assertInstanceOf(LogicalPlan.Ddl.CreateCatalog.class, plan);
      LogicalPlan.Ddl.CreateCatalog cc = (LogicalPlan.Ddl.CreateCatalog) plan;
      assertEquals("my_db", cc.catalogName());
      assertTrue(cc.ifNotExists());
    }
  }

  @Test
  void testDdlCreateMemoryTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("CREATE TABLE new_t AS SELECT * FROM t")) {
        assertInstanceOf(LogicalPlan.Ddl.CreateMemoryTable.class, plan);
        LogicalPlan.Ddl.CreateMemoryTable cmt = (LogicalPlan.Ddl.CreateMemoryTable) plan;
        assertEquals("new_t", ((TableReference.Bare) cmt.name()).table());
        assertFalse(cmt.ifNotExists());
        assertFalse(cmt.orReplace());
        assertFalse(cmt.temporary());
        assertNotNull(cmt.input());
        assertEquals(1, cmt.inputs().size());
      }
    }
  }

  @Test
  void testDdlCreateExternalTable() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan =
            state.createLogicalPlan(
                "CREATE EXTERNAL TABLE ext_t (id INT) STORED AS CSV LOCATION '/tmp/ext.csv'")) {
      assertInstanceOf(LogicalPlan.Ddl.CreateExternalTable.class, plan);
      LogicalPlan.Ddl.CreateExternalTable cet = (LogicalPlan.Ddl.CreateExternalTable) plan;
      assertEquals("ext_t", ((TableReference.Bare) cet.name()).table());
      assertEquals("/tmp/ext.csv", cet.location());
      assertEquals("CSV", cet.fileType());
      assertFalse(cet.ifNotExists());
      assertFalse(cet.orReplace());
      assertFalse(cet.temporary());
      assertTrue(cet.inputs().isEmpty());
    }
  }

  @Test
  void testDdlCreateIndex() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("CREATE UNIQUE INDEX IF NOT EXISTS my_idx ON t (id)")) {
        assertInstanceOf(LogicalPlan.Ddl.CreateIndex.class, plan);
        LogicalPlan.Ddl.CreateIndex ci = (LogicalPlan.Ddl.CreateIndex) plan;
        assertTrue(ci.name().isPresent());
        assertEquals("my_idx", ci.name().get());
        assertEquals("t", ((TableReference.Bare) ci.table()).table());
        assertTrue(ci.unique());
        assertTrue(ci.ifNotExists());
        assertTrue(ci.inputs().isEmpty());
      }
    }
  }

  @Test
  void testDdlDropSchema() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("DROP SCHEMA IF EXISTS my_schema CASCADE")) {
      assertInstanceOf(LogicalPlan.Ddl.DropCatalogSchema.class, plan);
      LogicalPlan.Ddl.DropCatalogSchema dcs = (LogicalPlan.Ddl.DropCatalogSchema) plan;
      assertEquals("my_schema", dcs.name());
      assertTrue(dcs.ifExists());
      assertTrue(dcs.cascade());
    }
  }

  @Test
  void testDdlCreateFunction() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan =
            state.createLogicalPlan(
                "CREATE OR REPLACE FUNCTION add_one(INTEGER) RETURNS INTEGER AS $$ x + 1 $$")) {
      assertInstanceOf(LogicalPlan.Ddl.CreateFunction.class, plan);
      LogicalPlan.Ddl.CreateFunction cf = (LogicalPlan.Ddl.CreateFunction) plan;
      assertEquals("add_one", cf.name());
      assertTrue(cf.orReplace());
      assertFalse(cf.temporary());
      assertTrue(cf.inputs().isEmpty());
    }
  }

  @Test
  void testDdlDropFunction() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("DROP FUNCTION IF EXISTS add_one")) {
      assertInstanceOf(LogicalPlan.Ddl.DropFunction.class, plan);
      LogicalPlan.Ddl.DropFunction df = (LogicalPlan.Ddl.DropFunction) plan;
      assertEquals("add_one", df.name());
      assertTrue(df.ifExists());
      assertTrue(df.inputs().isEmpty());
    }
  }

  @Test
  void testDmlInsert() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("INSERT INTO t VALUES (4, 'Dave')")) {
        assertInstanceOf(LogicalPlan.Dml.class, plan);
        LogicalPlan.Dml dml = (LogicalPlan.Dml) plan;
        assertEquals("t", ((TableReference.Bare) dml.tableName()).table());
        assertEquals(WriteOp.INSERT_APPEND, dml.op());
        assertNotNull(dml.input());
        assertEquals(1, dml.inputs().size());
      }
    }
  }

  @Test
  void testCopyVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("COPY (SELECT * FROM t) TO '/tmp/output.csv'")) {
        assertInstanceOf(LogicalPlan.Copy.class, plan);
        LogicalPlan.Copy copy = (LogicalPlan.Copy) plan;
        assertNotNull(copy.input());
        assertEquals(1, copy.inputs().size());
        assertNotNull(copy.schema());
      }
    }
  }

  @Test
  void testStatementSetVariableVariant() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("SET datafusion.execution.batch_size = 1024")) {
      assertInstanceOf(LogicalPlan.Statement.SetVariable.class, plan);
      LogicalPlan.Statement.SetVariable sv = (LogicalPlan.Statement.SetVariable) plan;
      assertEquals("datafusion.execution.batch_size", sv.variable());
      assertEquals("1024", sv.value());
      assertTrue(sv.inputs().isEmpty());
      assertNotNull(sv.schema());
    }
  }

  @Test
  void testStatementResetVariable() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("RESET datafusion.execution.batch_size")) {
      assertInstanceOf(LogicalPlan.Statement.ResetVariable.class, plan);
      LogicalPlan.Statement.ResetVariable rv = (LogicalPlan.Statement.ResetVariable) plan;
      assertEquals("datafusion.execution.batch_size", rv.variable());
      assertTrue(rv.inputs().isEmpty());
    }
  }

  @Test
  void testStatementTransactionStart() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan =
            state.createLogicalPlan("START TRANSACTION READ ONLY ISOLATION LEVEL READ COMMITTED")) {
      assertInstanceOf(LogicalPlan.Statement.TransactionStart.class, plan);
      LogicalPlan.Statement.TransactionStart tx = (LogicalPlan.Statement.TransactionStart) plan;
      assertEquals(TransactionAccessMode.READ_ONLY, tx.accessMode());
      assertEquals(TransactionIsolationLevel.READ_COMMITTED, tx.isolationLevel());
      assertTrue(tx.inputs().isEmpty());
    }
  }

  @Test
  void testStatementCommit() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("COMMIT")) {
      assertInstanceOf(LogicalPlan.Statement.TransactionEnd.class, plan);
      LogicalPlan.Statement.TransactionEnd te = (LogicalPlan.Statement.TransactionEnd) plan;
      assertEquals(TransactionConclusion.COMMIT, te.conclusion());
      assertFalse(te.chain());
      assertTrue(te.inputs().isEmpty());
    }
  }

  @Test
  void testStatementRollback() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("ROLLBACK")) {
      assertInstanceOf(LogicalPlan.Statement.TransactionEnd.class, plan);
      LogicalPlan.Statement.TransactionEnd te = (LogicalPlan.Statement.TransactionEnd) plan;
      assertEquals(TransactionConclusion.ROLLBACK, te.conclusion());
    }
  }

  @Test
  void testStatementPrepare() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("PREPARE my_plan AS SELECT 1")) {
      assertInstanceOf(LogicalPlan.Statement.Prepare.class, plan);
      LogicalPlan.Statement.Prepare prep = (LogicalPlan.Statement.Prepare) plan;
      assertEquals("my_plan", prep.name());
      assertNotNull(prep.fields());
      assertNotNull(prep.input());
      assertEquals(1, prep.inputs().size());
    }
  }

  @Test
  void testStatementExecute() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("EXECUTE my_plan")) {
      assertInstanceOf(LogicalPlan.Statement.Execute.class, plan);
      LogicalPlan.Statement.Execute exec = (LogicalPlan.Statement.Execute) plan;
      assertEquals("my_plan", exec.name());
      assertTrue(exec.parameters().isEmpty());
      assertTrue(exec.inputs().isEmpty());
    }
  }

  @Test
  void testStatementDeallocate() {
    try (SessionContext ctx = new SessionContext();
        SessionState state = ctx.state();
        LogicalPlan plan = state.createLogicalPlan("DEALLOCATE my_plan")) {
      assertInstanceOf(LogicalPlan.Statement.Deallocate.class, plan);
      LogicalPlan.Statement.Deallocate dealloc = (LogicalPlan.Statement.Deallocate) plan;
      assertEquals("my_plan", dealloc.name());
      assertTrue(dealloc.inputs().isEmpty());
    }
  }

  @Test
  void testDistinctOnVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan(
                  "SELECT DISTINCT ON (id) id, name FROM t ORDER BY id, name")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Distinct.On) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Distinct.On.class, current);
        LogicalPlan.Distinct.On on = (LogicalPlan.Distinct.On) current;
        assertEquals(1, on.onExprs().size(), "DISTINCT ON one column");
        assertEquals(2, on.selectExprs().size(), "selecting two columns");
        assertTrue(on.sortExprs().isPresent(), "ORDER BY should produce sort exprs");
        assertEquals(2, on.sortExprs().get().size());
        assertNotNull(on.input());
      }
    }
  }

  @Test
  void testDescribeTableVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("DESCRIBE t")) {
        assertInstanceOf(LogicalPlan.DescribeTable.class, plan);
        LogicalPlan.DescribeTable describe = (LogicalPlan.DescribeTable) plan;
        assertTrue(describe.inputs().isEmpty(), "DescribeTable has no children");
        assertNotNull(describe.schema());
        assertFalse(describe.schema().getFields().isEmpty());
      }
    }
  }

  // ── Deeper accessor tests ──

  @Test
  void testJoinAccessors() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator, "t1");
      registerTestTable(ctx, allocator, "t2");
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Join) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Join.class, current);
        LogicalPlan.Join join = (LogicalPlan.Join) current;
        assertEquals(JoinType.LEFT, join.joinType());
        assertEquals(JoinConstraint.ON, join.joinConstraint());
        // The join condition may appear in onLeftKeys/onRightKeys or in filter,
        // depending on the optimizer. Verify all accessors are reachable.
        assertNotNull(join.onLeftKeys(), "onLeftKeys should not be null");
        assertNotNull(join.onRightKeys(), "onRightKeys should not be null");
        assertEquals(
            join.onLeftKeys().size(),
            join.onRightKeys().size(),
            "left and right key lists should have same size");
        // The condition should appear somewhere: either as on-keys or as a filter
        assertTrue(
            !join.onLeftKeys().isEmpty() || join.filter().isPresent(),
            "join condition should be in on-keys or filter");
        assertEquals(NullEquality.NULL_EQUALS_NOTHING, join.nullEquality());
        assertEquals(2, join.inputs().size());
      }
    }
  }

  @Test
  void testTableScanAccessors() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.TableScan) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.TableScan.class, current);
        LogicalPlan.TableScan scan = (LogicalPlan.TableScan) current;
        assertInstanceOf(TableReference.Bare.class, scan.tableName());
        assertEquals("t", ((TableReference.Bare) scan.tableName()).table());
        // Exercise all accessors - values depend on planner optimization
        assertNotNull(scan.projection(), "projection should not be null");
        assertNotNull(scan.filters(), "filters list should not be null");
        assertNotNull(scan.fetch(), "fetch should not be null");
      }
    }
  }

  @Test
  void testLimitWithSkipAndFetch() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan = state.createLogicalPlan("SELECT * FROM t LIMIT 2 OFFSET 1")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Limit) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Limit.class, current);
        LogicalPlan.Limit limit = (LogicalPlan.Limit) current;
        assertTrue(limit.fetchExpr().isPresent(), "LIMIT should set fetchExpr");
        assertInstanceOf(Expr.LiteralExpr.class, limit.fetchExpr().get());
        assertTrue(limit.skipExpr().isPresent(), "OFFSET should set skipExpr");
        assertInstanceOf(Expr.LiteralExpr.class, limit.skipExpr().get());
      }
    }
  }

  @Test
  void testSortExprDescNullsFirst() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      registerTestTable(ctx, allocator);
      try (SessionState state = ctx.state();
          LogicalPlan plan =
              state.createLogicalPlan("SELECT * FROM t ORDER BY id DESC NULLS FIRST")) {
        LogicalPlan current = plan;
        while (!(current instanceof LogicalPlan.Sort) && !current.inputs().isEmpty()) {
          current = current.inputs().get(0);
        }
        assertInstanceOf(LogicalPlan.Sort.class, current);
        LogicalPlan.Sort sort = (LogicalPlan.Sort) current;
        assertEquals(1, sort.sortExprs().size());
        SortExpr sortExpr = sort.sortExprs().get(0);
        assertFalse(sortExpr.asc(), "DESC should mean asc=false");
        assertTrue(sortExpr.nullsFirst(), "NULLS FIRST should mean nullsFirst=true");
        assertInstanceOf(Expr.ColumnExpr.class, sortExpr.expr());
      }
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
