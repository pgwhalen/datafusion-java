package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.ScanArgs;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.TaskContext;
import org.apache.arrow.datafusion.logical_expr.ColumnAssignment;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.InsertOp;
import org.apache.arrow.datafusion.logical_expr.Operator;
import org.apache.arrow.datafusion.logical_expr.TableProviderFilterPushDown;
import org.apache.arrow.datafusion.physical_plan.Boundedness;
import org.apache.arrow.datafusion.physical_plan.EmissionType;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.PlanProperties;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Tests for custom Java-implemented TableProvider, SchemaProvider, and CatalogProvider that
 * DataFusion can query via FFI callbacks.
 */
public class CustomTableProviderTest {

  @Test
  void testCustomTableProviderBasic() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create a simple table provider that returns fixed data
      Schema schema = createUsersSchema();
      TableProvider myTable = new TestTableProvider(schema, usersDataBatch());
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("my_table", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("my_schema", mySchema));

      // Register the catalog
      ctx.registerCatalog("my_catalog", myCatalog, allocator);

      // Query it via fully-qualified table name
      try (DataFrame df = ctx.sql("SELECT * FROM my_catalog.my_schema.my_table");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(1, idVector.get(0));
        assertEquals(2, idVector.get(1));
        assertEquals(3, idVector.get(2));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testCustomTableProviderWithFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      TableProvider myTable = new TestTableProvider(schema, usersDataBatch());
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("test_table", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("public", mySchema));

      ctx.registerCatalog("test_catalog", myCatalog, allocator);

      // Query with WHERE clause - filter happens in DataFusion
      try (DataFrame df =
              ctx.sql("SELECT id, name FROM test_catalog.public.test_table WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount()); // Only id=2 and id=3

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(2, idVector.get(0));
        assertEquals(3, idVector.get(1));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testCustomTableProviderWithProjection() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      TableProvider myTable = new TestTableProvider(schema, usersDataBatch());
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("data", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("main", mySchema));

      ctx.registerCatalog("app", myCatalog, allocator);

      // Query with column selection
      try (DataFrame df = ctx.sql("SELECT name FROM app.main.data WHERE id = 2");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        assertEquals("Bob", new String(nameVector.get(0)));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testMultipleBatchesFromExecutionPlan() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider that returns multiple batches (3 batches × 2 rows = 6 rows)
      TableProvider myTable =
          new TestTableProvider(
              schema, multiBatchData(0, 2), multiBatchData(1, 2), multiBatchData(2, 2));
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("multi_batch", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("batch_catalog", myCatalog, allocator);

      // Query without ORDER BY to avoid coalescing batches during sort
      try (DataFrame df = ctx.sql("SELECT * FROM batch_catalog.test.multi_batch");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        // Collect all rows across all batches - DataFusion may or may not coalesce
        Set<Long> allIds = new HashSet<>();
        int totalRows = 0;
        int batchCount = 0;

        while (stream.loadNextBatch()) {
          batchCount++;
          int rowCount = root.getRowCount();
          totalRows += rowCount;

          BigIntVector idVector = (BigIntVector) root.getVector("id");
          for (int i = 0; i < rowCount; i++) {
            allIds.add(idVector.get(i));
          }
        }

        // Verify we got all 6 rows with ids 1-6
        assertEquals(6, totalRows, "Should have 6 total rows");
        assertEquals(Set.of(1L, 2L, 3L, 4L, 5L, 6L), allIds, "Should have ids 1-6");
        assertTrue(batchCount >= 1, "Should have at least one batch");
      }
    }
  }

  @Test
  void testMultipleTablesInSchema() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create two tables with different schemas and data
      TableProvider usersTable = new TestTableProvider(createUsersSchema(), usersDataBatch());
      TableProvider productsTable =
          new TestTableProvider(createProductsSchema(), productsDataBatch());

      Map<String, TableProvider> tables = new HashMap<>();
      tables.put("users", usersTable);
      tables.put("products", productsTable);

      SchemaProvider mySchema = new SimpleSchemaProvider(tables);
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));

      ctx.registerCatalog("store", myCatalog, allocator);

      // Query the first table
      try (DataFrame df = ctx.sql("SELECT * FROM store.default.users ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(1, idVector.get(0));
        assertEquals(2, idVector.get(1));
        assertEquals(3, idVector.get(2));
      }

      // Query the second table
      try (DataFrame df = ctx.sql("SELECT * FROM store.default.products ORDER BY product_id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("product_id");
        assertEquals(100, idVector.get(0));
        assertEquals(101, idVector.get(1));
      }

      // Query with JOIN between the two tables
      try (DataFrame df =
              ctx.sql(
                  "SELECT u.name, p.product_id, p.price "
                      + "FROM store.default.users u "
                      + "JOIN store.default.products p ON u.id = p.user_id "
                      + "ORDER BY p.product_id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());

        // First row: Alice's product (product_id=100)
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        BigIntVector productIdVector = (BigIntVector) root.getVector("product_id");
        BigIntVector priceVector = (BigIntVector) root.getVector("price");

        assertEquals("Alice", new String(nameVector.get(0)));
        assertEquals(100, productIdVector.get(0));
        assertEquals(999, priceVector.get(0));

        // Second row: Bob's product (product_id=101)
        assertEquals("Bob", new String(nameVector.get(1)));
        assertEquals(101, productIdVector.get(1));
        assertEquals(1499, priceVector.get(1));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testDynamicTableRegistration() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Start with one table
      Schema schema = createUsersSchema();
      TableProvider usersTable = new TestTableProvider(schema, usersDataBatch());

      Map<String, TableProvider> tables = new HashMap<>();
      tables.put("users", usersTable);

      SimpleSchemaProvider mySchema = new SimpleSchemaProvider(tables);
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));

      ctx.registerCatalog("dynamic", myCatalog, allocator);

      // Query the initial table
      try (DataFrame df = ctx.sql("SELECT * FROM dynamic.default.users ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());
      }

      // Dynamically register a second table
      TableProvider productsTable =
          new TestTableProvider(createProductsSchema(), productsDataBatch());
      Optional<TableProvider> previous = mySchema.registerTable("products", productsTable);
      assertTrue(previous.isEmpty(), "No previous table should exist for 'products'");

      // Query the newly registered table
      try (DataFrame df = ctx.sql("SELECT * FROM dynamic.default.products ORDER BY product_id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("product_id");
        assertEquals(100, idVector.get(0));
        assertEquals(101, idVector.get(1));
      }

      // Deregister the original table
      Optional<TableProvider> removed = mySchema.deregisterTable("users");
      assertTrue(removed.isPresent(), "Deregister should return the removed table");

      // Querying the removed table should fail
      assertThrows(
          Exception.class,
          () -> {
            try (DataFrame df = ctx.sql("SELECT * FROM dynamic.default.users")) {
              // Should not reach here
            }
          });
    }
  }

  @Test
  void testLimitPushedToTableProvider() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider that captures the limit value
      AtomicReference<Long> capturedLimit = new AtomicReference<>();
      TableProvider limitCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              capturedLimit.set(args.limit());
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("limit_test", limitCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("limit_catalog", myCatalog, allocator);

      // Query with LIMIT clause
      try (DataFrame df = ctx.sql("SELECT * FROM limit_catalog.test.limit_test LIMIT 2");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        // DataFusion applies the limit, so we get at most 2 rows
        assertTrue(root.getRowCount() <= 2);
      }

      // Verify that the limit was passed to the table provider
      assertNotNull(capturedLimit.get(), "Limit should have been passed to scan()");
      assertEquals(2L, capturedLimit.get(), "Limit value should be 2");
    }
  }

  @Test
  void testNoLimitPassedAsNull() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider that captures the limit value
      AtomicReference<Long> capturedLimit = new AtomicReference<>(999L); // Non-null sentinel
      TableProvider limitCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              capturedLimit.set(args.limit());
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("no_limit_test", limitCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("no_limit_catalog", myCatalog, allocator);

      // Query without LIMIT clause
      try (DataFrame df = ctx.sql("SELECT * FROM no_limit_catalog.test.no_limit_test");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount()); // All rows returned
      }

      // Verify that no limit was passed (null)
      assertNull(capturedLimit.get(), "Limit should be null when no LIMIT clause is used");
    }
  }

  @Test
  void testProjectionSpecificColumns() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      AtomicReference<List<Integer>> capturedProjection = new AtomicReference<>();
      TableProvider projectionCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              capturedProjection.set(args.projection());
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("proj_test", projectionCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("proj_specific_catalog", myCatalog, allocator);

      // SELECT * resolves to explicit column indices via the optimizer
      try (DataFrame df = ctx.sql("SELECT * FROM proj_specific_catalog.test.proj_test");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(1, idVector.get(0));
        assertEquals(2, idVector.get(1));
        assertEquals(3, idVector.get(2));
      }

      List<Integer> projection = capturedProjection.get();
      assertNotNull(projection, "Projection should be non-null (specific columns)");
      assertFalse(projection.isEmpty(), "Projection should contain column indices");
      assertTrue(projection.contains(0), "Projection should include column 0 (id)");
      assertTrue(projection.contains(1), "Projection should include column 1 (name)");
    }
  }

  @Test
  void testProjectionEmptyForCountStar() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      Schema emptySchema = new Schema(Collections.emptyList());

      Object sentinel = new Object();
      AtomicReference<Object> capturedProjection = new AtomicReference<>(sentinel);
      TableProvider projectionCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              capturedProjection.set(args.projection());
              // count(*) passes empty projection — return an execution plan with empty schema
              List<Integer> projection = args.projection();
              Schema scanSchema =
                  (projection != null && projection.isEmpty()) ? emptySchema : schema;
              BatchPopulator emptyBatch = root -> root.setRowCount(3);
              BatchPopulator fullBatch = usersDataBatch();
              return new TestExecutionPlan(
                  scanSchema,
                  List.of((projection != null && projection.isEmpty()) ? emptyBatch : fullBatch));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("count_test", projectionCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("proj_empty_catalog", myCatalog, allocator);

      // count(*) should pass empty projection (zero columns explicitly requested)
      try (DataFrame df = ctx.sql("SELECT count(*) FROM proj_empty_catalog.test.count_test");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        BigIntVector countVector = (BigIntVector) root.getVector(0);
        assertEquals(3L, countVector.get(0));

        assertFalse(stream.loadNextBatch());
      }

      assertNotSame(sentinel, capturedProjection.get(), "scan() should have been called");
      @SuppressWarnings("unchecked")
      List<Integer> projection = (List<Integer>) capturedProjection.get();
      assertNotNull(
          projection, "count(*) should pass empty list, not null (null means all columns)");
      assertTrue(projection.isEmpty(), "count(*) should pass empty projection (zero columns)");
    }
  }

  @Test
  void testCustomPlanProperties() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider with a custom execution plan that uses non-default properties
      TableProvider customPropsTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              return new ExecutionPlan() {
                @Override
                public Schema schema() {
                  return schema;
                }

                @Override
                public PlanProperties properties() {
                  return new PlanProperties(1, EmissionType.BOTH, Boundedness.BOUNDED);
                }

                @Override
                public RecordBatchReader execute(
                    int partition, TaskContext taskContext, BufferAllocator alloc) {
                  return new TestRecordBatchReader(schema, List.of(usersDataBatch()), alloc);
                }
              };
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("props_test", customPropsTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));

      ctx.registerCatalog("props_catalog", myCatalog, allocator);

      // Query the table - should work correctly regardless of plan properties
      try (DataFrame df = ctx.sql("SELECT * FROM props_catalog.default.props_test ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(1, idVector.get(0));
        assertEquals(2, idVector.get(1));
        assertEquals(3, idVector.get(2));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testExactFilterPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider that returns EXACT for "id" column filters, INEXACT for others
      AtomicReference<List<TableProviderFilterPushDown>> capturedPushdowns =
          new AtomicReference<>();
      TableProvider filterTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public List<TableProviderFilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
              // Return EXACT for all filters (we can't inspect Expr contents easily,
              // but we can verify the callback is invoked with the right count)
              List<TableProviderFilterPushDown> result =
                  new ArrayList<>(
                      java.util.Collections.nCopies(
                          filters.size(), TableProviderFilterPushDown.EXACT));
              capturedPushdowns.set(result);
              return result;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("filter_test", filterTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("filter_catalog", myCatalog, allocator);

      // Query with WHERE clause - filters are pushed down
      try (DataFrame df = ctx.sql("SELECT * FROM filter_catalog.test.filter_test WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        // With EXACT pushdown, DataFusion trusts our provider to handle the filter.
        // Since our provider returns all data, we get all 3 rows.
        assertEquals(3, root.getRowCount());
        assertFalse(stream.loadNextBatch());
      }

      // Verify the callback was invoked
      assertNotNull(capturedPushdowns.get(), "supportsFiltersPushdown should have been called");
      assertFalse(capturedPushdowns.get().isEmpty(), "Should have received at least one filter");
    }
  }

  @Test
  void testUnsupportedFilterPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Create a table provider that returns UNSUPPORTED for all filters
      TableProvider unsupportedTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public List<TableProviderFilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
              return java.util.Collections.nCopies(
                  filters.size(), TableProviderFilterPushDown.UNSUPPORTED);
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("unsupported_test", unsupportedTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("unsupported_catalog", myCatalog, allocator);

      // Query with WHERE clause
      try (DataFrame df =
              ctx.sql("SELECT * FROM unsupported_catalog.test.unsupported_test WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        // DataFusion applies the filter post-scan since provider said UNSUPPORTED
        assertEquals(2, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(2, idVector.get(0));
        assertEquals(3, idVector.get(1));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testDefaultFilterPushdownIsInexact() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Use the default supportsFiltersPushdown (does NOT override it)
      // This tests backward compatibility — the default returns INEXACT for all filters
      TableProvider defaultTable = new TestTableProvider(schema, usersDataBatch());

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("default_test", defaultTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("default_catalog", myCatalog, allocator);

      // Query with WHERE clause — INEXACT means DataFusion re-applies filter post-scan
      try (DataFrame df = ctx.sql("SELECT * FROM default_catalog.test.default_test WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        // DataFusion applies filter, so only id=2 and id=3
        assertEquals(2, root.getRowCount());

        BigIntVector idVector = (BigIntVector) root.getVector("id");
        assertEquals(2, idVector.get(0));
        assertEquals(3, idVector.get(1));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testFilterExprStructure() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Capture the filters from scan callback
      AtomicReference<List<Expr>> capturedFilters = new AtomicReference<>();
      TableProvider filterCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              capturedFilters.set(args.filters());
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("expr_test", filterCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("expr_catalog", myCatalog, allocator);

      // Query with WHERE clause - filter is "id > 1"
      try (DataFrame df = ctx.sql("SELECT * FROM expr_catalog.test.expr_test WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        while (stream.loadNextBatch()) {
          // consume all batches
        }
      }

      // Verify the captured filter structure
      List<Expr> filters = capturedFilters.get();
      assertNotNull(filters, "Filters should have been captured");
      assertFalse(filters.isEmpty(), "Should have at least one filter");

      // The filter should be a BinaryExpr: id > 1
      Expr filter = filters.get(0);
      assertInstanceOf(Expr.BinaryExpr.class, filter, "Filter should be a BinaryExpr");
      Expr.BinaryExpr binExpr = (Expr.BinaryExpr) filter;

      assertEquals(Operator.Gt, binExpr.op(), "Operator should be Gt (>)");

      // Left side should be a Column reference to "id"
      assertInstanceOf(Expr.ColumnExpr.class, binExpr.left(), "Left should be a ColumnExpr");
      Expr.ColumnExpr colExpr = (Expr.ColumnExpr) binExpr.left();
      assertEquals("id", colExpr.column().name(), "Column name should be 'id'");

      // Right side should be a literal value
      assertInstanceOf(Expr.LiteralExpr.class, binExpr.right(), "Right should be a LiteralExpr");
      Expr.LiteralExpr litExpr = (Expr.LiteralExpr) binExpr.right();
      assertInstanceOf(ScalarValue.Int64.class, litExpr.value(), "Literal should be Int64");
      assertEquals(1L, ((ScalarValue.Int64) litExpr.value()).value(), "Literal value should be 1");
    }
  }

  @Test
  void testComplexFilterExprStructure() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Capture the filters from supportsFiltersPushdown callback
      AtomicReference<List<Expr>> capturedFilters = new AtomicReference<>();
      TableProvider filterCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public List<TableProviderFilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
              capturedFilters.set(filters);
              return java.util.Collections.nCopies(
                  filters.size(), TableProviderFilterPushDown.INEXACT);
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("complex_test", filterCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("complex_catalog", myCatalog, allocator);

      // Query with complex WHERE clause: id > 1 AND name = 'Bob'
      try (DataFrame df =
              ctx.sql(
                  "SELECT * FROM complex_catalog.test.complex_test WHERE id > 1 AND name = 'Bob'");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        while (stream.loadNextBatch()) {
          // consume all batches
        }
      }

      // Verify the captured filters from supportsFiltersPushdown
      List<Expr> filters = capturedFilters.get();
      assertNotNull(filters, "Filters should have been captured from supportsFiltersPushdown");

      // DataFusion may send the AND as a single compound filter or two separate filters.
      // With our proto deserialization, we should be able to see the structure either way.
      boolean foundIdGt = false;
      boolean foundNameEq = false;

      for (Expr filter : filters) {
        // Recursively check the expression tree for our expected patterns
        if (containsBinaryOp(filter, Operator.Gt, "id")) {
          foundIdGt = true;
        }
        if (containsBinaryOp(filter, Operator.Eq, "name")) {
          foundNameEq = true;
        }
      }

      assertTrue(foundIdGt, "Should find 'id > ...' filter");
      assertTrue(foundNameEq, "Should find 'name = ...' filter");
    }
  }

  // ==========================================================================
  // DML tests: INSERT INTO, DELETE FROM, UPDATE
  // ==========================================================================

  @Test
  void testDeleteFromCustomTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      MutableTableProvider mutableTable =
          new MutableTableProvider(
              schema,
              new Object[] {1L, "Alice"},
              new Object[] {2L, "Bob"},
              new Object[] {3L, "Charlie"});

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("users", mutableTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));
      ctx.registerCatalog("dml", myCatalog, allocator);

      // DELETE rows where id > 1
      try (DataFrame df = ctx.sql("DELETE FROM dml.default.users WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        // DML results produce a "count" column — read as long regardless of vector type
        long count = readCountColumn(root);
        assertEquals(2L, count, "Should delete 2 rows (Bob and Charlie)");
        assertFalse(stream.loadNextBatch());
      }

      // Verify remaining data
      assertEquals(1, mutableTable.rows.size());
      assertEquals(1L, mutableTable.rows.get(0)[0]);
      assertEquals("Alice", mutableTable.rows.get(0)[1]);
    }
  }

  @Test
  void testDeleteFromAllRows() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      MutableTableProvider mutableTable =
          new MutableTableProvider(schema, new Object[] {1L, "Alice"}, new Object[] {2L, "Bob"});

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("users", mutableTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));
      ctx.registerCatalog("dml_del_all", myCatalog, allocator);

      // DELETE all rows (no WHERE)
      try (DataFrame df = ctx.sql("DELETE FROM dml_del_all.default.users");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        long count = readCountColumn(root);
        assertEquals(2L, count, "Should delete all 2 rows");
      }

      assertTrue(mutableTable.rows.isEmpty(), "Table should be empty after DELETE all");
    }
  }

  @Test
  void testUpdateCustomTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      MutableTableProvider mutableTable =
          new MutableTableProvider(
              schema,
              new Object[] {1L, "Alice"},
              new Object[] {2L, "Bob"},
              new Object[] {3L, "Charlie"});

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("users", mutableTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));
      ctx.registerCatalog("dml_upd", myCatalog, allocator);

      // UPDATE name to 'updated' where id = 1
      try (DataFrame df =
              ctx.sql("UPDATE dml_upd.default.users SET name = 'updated' WHERE id = 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        long count = readCountColumn(root);
        assertEquals(1L, count, "Should update 1 row");
      }

      // Verify the updated data
      assertEquals(3, mutableTable.rows.size());
      assertEquals("updated", mutableTable.rows.get(0)[1]);
      assertEquals("Bob", mutableTable.rows.get(1)[1]);
      assertEquals("Charlie", mutableTable.rows.get(2)[1]);
    }
  }

  @Test
  void testInsertIntoCustomTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      MutableTableProvider mutableTable =
          new MutableTableProvider(schema, new Object[] {1L, "Alice"});

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("users", mutableTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));
      ctx.registerCatalog("dml_ins", myCatalog, allocator);

      // INSERT new rows
      try (DataFrame df =
              ctx.sql("INSERT INTO dml_ins.default.users VALUES (2, 'Bob'), (3, 'Charlie')");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        long count = readCountColumn(root);
        assertEquals(2L, count, "Should insert 2 rows");
      }

      // Verify all data
      assertEquals(3, mutableTable.rows.size());
      assertEquals(1L, mutableTable.rows.get(0)[0]);
      assertEquals("Alice", mutableTable.rows.get(0)[1]);
      assertEquals(2L, mutableTable.rows.get(1)[0]);
      assertEquals("Bob", mutableTable.rows.get(1)[1]);
      assertEquals(3L, mutableTable.rows.get(2)[0]);
      assertEquals("Charlie", mutableTable.rows.get(2)[1]);
    }
  }

  @Test
  void testInsertIntoMultipleBatches() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      MutableTableProvider mutableTable = new MutableTableProvider(schema);

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("users", mutableTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));
      ctx.registerCatalog("dml_multi", myCatalog, allocator);

      // Insert enough rows to potentially span multiple batches
      StringBuilder sb = new StringBuilder("INSERT INTO dml_multi.default.users VALUES ");
      for (int i = 1; i <= 10; i++) {
        if (i > 1) sb.append(", ");
        sb.append("(").append(i).append(", 'Name").append(i).append("')");
      }

      try (DataFrame df = ctx.sql(sb.toString());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        long count = readCountColumn(root);
        assertEquals(10L, count, "Should insert 10 rows");
      }

      // Verify all rows were inserted
      assertEquals(10, mutableTable.rows.size());
      for (int i = 0; i < 10; i++) {
        assertEquals((long) (i + 1), mutableTable.rows.get(i)[0]);
        assertEquals("Name" + (i + 1), mutableTable.rows.get(i)[1]);
      }
    }
  }

  /**
   * Reads the "count" column from a DML result batch. DataFusion DML operations return the count as
   * UInt64, which Arrow Java represents as a UInt8Vector (unsigned 64-bit). Our
   * MutableTableProvider returns Int64 (BigIntVector). This helper handles both.
   */
  private long readCountColumn(VectorSchemaRoot root) {
    var vector = root.getVector("count");
    if (vector instanceof BigIntVector bigInt) {
      return bigInt.get(0);
    } else if (vector instanceof UInt8Vector uint8) {
      return uint8.get(0);
    } else {
      throw new AssertionError(
          "Unexpected count vector type: " + vector.getClass().getSimpleName());
    }
  }

  /**
   * Recursively checks if an expression tree contains a BinaryExpr with the given op and column.
   */
  private boolean containsBinaryOp(Expr expr, Operator op, String columnName) {
    if (expr instanceof Expr.BinaryExpr bin) {
      if (bin.op() == op) {
        if (bin.left() instanceof Expr.ColumnExpr col && col.column().name().equals(columnName)) {
          return true;
        }
      }
      // Check children recursively (e.g., AND combining two BinaryExprs)
      return containsBinaryOp(bin.left(), op, columnName)
          || containsBinaryOp(bin.right(), op, columnName);
    }
    return false;
  }

  // Helper methods to create test schemas

  private Schema createUsersSchema() {
    return new Schema(
        Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
  }

  private Schema createProductsSchema() {
    return new Schema(
        Arrays.asList(
            new Field("product_id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("user_id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("price", FieldType.nullable(new ArrowType.Int(64, true)), null)));
  }

  // Test implementation classes

  /** Functional interface for populating a batch of data in a VectorSchemaRoot. */
  @FunctionalInterface
  interface BatchPopulator {
    void populate(VectorSchemaRoot root);
  }

  /** A flexible TableProvider for testing that generates batches via BatchPopulator functions. */
  static class TestTableProvider implements TableProvider {
    private final Schema schema;
    private final List<BatchPopulator> batches;

    TestTableProvider(Schema schema, BatchPopulator... batches) {
      this.schema = schema;
      this.batches = List.of(batches);
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
      return new TestExecutionPlan(schema, batches);
    }
  }

  /** An ExecutionPlan that creates readers using BatchPopulator functions. */
  static class TestExecutionPlan implements ExecutionPlan {
    private final Schema schema;
    private final List<BatchPopulator> batches;

    TestExecutionPlan(Schema schema, List<BatchPopulator> batches) {
      this.schema = schema;
      this.batches = batches;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public RecordBatchReader execute(
        int partition, TaskContext taskContext, BufferAllocator allocator) {
      return new TestRecordBatchReader(schema, batches, allocator);
    }
  }

  /** A RecordBatchReader that returns batches by invoking BatchPopulator functions. */
  static class TestRecordBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private final List<BatchPopulator> batches;
    private int currentBatch = 0;

    TestRecordBatchReader(Schema schema, List<BatchPopulator> batches, BufferAllocator allocator) {
      this.root = VectorSchemaRoot.create(schema, allocator);
      this.batches = batches;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public boolean loadNextBatch() {
      if (currentBatch >= batches.size()) {
        return false;
      }
      batches.get(currentBatch++).populate(root);
      return true;
    }

    @Override
    public void close() {
      root.close();
    }
  }

  /**
   * A mutable in-memory TableProvider that supports INSERT, DELETE, and UPDATE for DML tests.
   *
   * <p>Data is stored as a List of Object[] rows. Each DML operation returns a count execution
   * plan.
   */
  static class MutableTableProvider implements TableProvider {
    private final Schema schema;
    final List<Object[]> rows;

    MutableTableProvider(Schema schema, Object[]... initialRows) {
      this.schema = schema;
      this.rows = new ArrayList<>(Arrays.asList(initialRows));
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
      // Snapshot the current data into a batch populator
      List<Object[]> snapshot = new ArrayList<>(rows);
      BatchPopulator populator =
          root -> {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            idVec.allocateNew(snapshot.size());
            nameVec.allocateNew(snapshot.size());
            for (int i = 0; i < snapshot.size(); i++) {
              idVec.set(i, (long) snapshot.get(i)[0]);
              nameVec.setSafe(i, ((String) snapshot.get(i)[1]).getBytes());
            }
            idVec.setValueCount(snapshot.size());
            nameVec.setValueCount(snapshot.size());
            root.setRowCount(snapshot.size());
          };
      return new TestExecutionPlan(schema, List.of(populator));
    }

    @Override
    public ExecutionPlan deleteFrom(Session session, List<Expr> filters) {
      // Simple delete: evaluate filters in Java by matching column expressions
      int deleted = 0;
      java.util.Iterator<Object[]> it = rows.iterator();
      while (it.hasNext()) {
        Object[] row = it.next();
        if (matchesFilters(row, filters)) {
          it.remove();
          deleted++;
        }
      }
      return countPlan(deleted);
    }

    @Override
    public ExecutionPlan update(
        Session session, List<ColumnAssignment> assignments, List<Expr> filters) {
      int updated = 0;
      for (Object[] row : rows) {
        if (matchesFilters(row, filters)) {
          for (ColumnAssignment assignment : assignments) {
            int colIdx = columnIndex(assignment.column());
            if (colIdx >= 0 && assignment.value() instanceof Expr.LiteralExpr lit) {
              row[colIdx] = scalarToJava(lit);
            }
          }
          updated++;
        }
      }
      return countPlan(updated);
    }

    @Override
    public ExecutionPlan insertInto(
        Session session,
        org.apache.arrow.datafusion.physical_plan.RecordBatchReader input,
        InsertOp insertOp) {
      int inserted = 0;
      VectorSchemaRoot root = input.getVectorSchemaRoot();
      while (input.loadNextBatch()) {
        BigIntVector idVec = (BigIntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        for (int i = 0; i < root.getRowCount(); i++) {
          rows.add(new Object[] {idVec.get(i), new String(nameVec.get(i))});
          inserted++;
        }
      }
      return countPlan(inserted);
    }

    private int columnIndex(String name) {
      for (int i = 0; i < schema.getFields().size(); i++) {
        if (schema.getFields().get(i).getName().equals(name)) return i;
      }
      return -1;
    }

    /**
     * Simple filter matching for test purposes. Supports basic column op literal comparisons on the
     * "id" (Int64) and "name" (Utf8) columns.
     */
    private boolean matchesFilters(Object[] row, List<Expr> filters) {
      if (filters.isEmpty()) return true;
      for (Expr filter : filters) {
        if (!matchesFilter(row, filter)) return false;
      }
      return true;
    }

    private boolean matchesFilter(Object[] row, Expr filter) {
      if (filter instanceof Expr.BinaryExpr bin) {
        if (bin.left() instanceof Expr.ColumnExpr col
            && bin.right() instanceof Expr.LiteralExpr lit) {
          int colIdx = columnIndex(col.column().name());
          if (colIdx < 0) return true;
          return compareOp(row[colIdx], scalarToJava(lit), bin.op());
        }
      }
      return true; // Unknown filter — include the row
    }

    @SuppressWarnings("unchecked")
    private boolean compareOp(Object rowVal, Object litVal, Operator op) {
      if (rowVal instanceof Comparable c1 && litVal instanceof Comparable c2) {
        int cmp = c1.compareTo(c2);
        return switch (op) {
          case Eq -> cmp == 0;
          case NotEq -> cmp != 0;
          case Lt -> cmp < 0;
          case LtEq -> cmp <= 0;
          case Gt -> cmp > 0;
          case GtEq -> cmp >= 0;
          default -> true;
        };
      }
      return true;
    }

    private Object scalarToJava(Expr.LiteralExpr lit) {
      return switch (lit.value()) {
        case org.apache.arrow.datafusion.common.ScalarValue.Int64 i -> i.value();
        case org.apache.arrow.datafusion.common.ScalarValue.Utf8 s -> s.value();
        default -> null;
      };
    }

    /** Creates an ExecutionPlan that returns a single batch with a UInt64 "count" column. */
    private ExecutionPlan countPlan(long count) {
      Schema countSchema =
          new Schema(
              List.of(new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null)));
      return new TestExecutionPlan(
          countSchema,
          List.of(
              root -> {
                BigIntVector vec = (BigIntVector) root.getVector("count");
                vec.allocateNew(1);
                vec.set(0, count);
                vec.setValueCount(1);
                root.setRowCount(1);
              }));
    }
  }

  /** A simple SchemaProvider backed by a map of table names to providers. */
  static class SimpleSchemaProvider implements SchemaProvider {
    private final Map<String, TableProvider> tables;

    SimpleSchemaProvider(Map<String, TableProvider> tables) {
      this.tables = new HashMap<>(tables);
    }

    @Override
    public List<String> tableNames() {
      return List.copyOf(tables.keySet());
    }

    @Override
    public Optional<TableProvider> table(String name) {
      return Optional.ofNullable(tables.get(name));
    }

    @Override
    public Optional<TableProvider> registerTable(String name, TableProvider table) {
      return Optional.ofNullable(tables.put(name, table));
    }

    @Override
    public Optional<TableProvider> deregisterTable(String name) {
      return Optional.ofNullable(tables.remove(name));
    }
  }

  /** A simple CatalogProvider backed by a map of schema names to providers. */
  static class SimpleCatalogProvider implements CatalogProvider {
    private final Map<String, SchemaProvider> schemas;

    SimpleCatalogProvider(Map<String, SchemaProvider> schemas) {
      this.schemas = new HashMap<>(schemas);
    }

    @Override
    public List<String> schemaNames() {
      return List.copyOf(schemas.keySet());
    }

    @Override
    public Optional<SchemaProvider> schema(String name) {
      return Optional.ofNullable(schemas.get(name));
    }
  }

  /**
   * Verifies that executeStream() is truly lazy: it returns before any batches are fetched, and
   * each subsequent loadNextBatch() fetches exactly one batch from the underlying reader.
   *
   * <p>This test works by failing on the second batch. With the old (broken) eager implementation,
   * executeStream() itself would have thrown because it eagerly collected all batches. With the
   * correct lazy implementation, executeStream() succeeds, the first loadNextBatch() returns data,
   * and only the second loadNextBatch() throws.
   */
  @Test
  void testExecuteStreamIsLazy() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();
      AtomicInteger batchCallCount = new AtomicInteger(0);

      TableProvider lazyTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
              return new ExecutionPlan() {
                @Override
                public Schema schema() {
                  return schema;
                }

                @Override
                public RecordBatchReader execute(
                    int partition, TaskContext taskContext, BufferAllocator alloc) {
                  VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
                  return new RecordBatchReader() {
                    @Override
                    public VectorSchemaRoot getVectorSchemaRoot() {
                      return root;
                    }

                    @Override
                    public boolean loadNextBatch() {
                      int call = batchCallCount.incrementAndGet();
                      if (call == 1) {
                        // First batch: ids 10 and 20
                        BigIntVector id = (BigIntVector) root.getVector("id");
                        VarCharVector name = (VarCharVector) root.getVector("name");
                        id.allocateNew(2);
                        name.allocateNew(2);
                        id.set(0, 10);
                        name.setSafe(0, "Ten".getBytes());
                        id.set(1, 20);
                        name.setSafe(1, "Twenty".getBytes());
                        id.setValueCount(2);
                        name.setValueCount(2);
                        root.setRowCount(2);
                        return true;
                      }
                      // Second call: signal end of stream
                      return false;
                    }

                    @Override
                    public void close() {
                      root.close();
                    }
                  };
                }
              };
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("lazy_table", lazyTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("s", mySchema));
      ctx.registerCatalog("lazy_cat", myCatalog, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM lazy_cat.s.lazy_table");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        // executeStream() must return without fetching any batches yet.
        assertEquals(
            0,
            batchCallCount.get(),
            "executeStream() must not fetch any batches before loadNextBatch() is called");

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        // First batch
        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());
        BigIntVector id = (BigIntVector) root.getVector("id");
        assertEquals(10, id.get(0));
        assertEquals(20, id.get(1));

        // End of stream
        assertFalse(stream.loadNextBatch());
      }
    }
  }

  /**
   * Verifies that executeStream() correctly handles a provider that returns multiple distinct
   * batches, delivering each batch in order via successive loadNextBatch() calls.
   */
  @Test
  void testExecuteStreamMultipleBatches() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createUsersSchema();

      // Three distinct batches: [id=1], [id=2,id=3], [id=4]
      TableProvider multiTable =
          new TestTableProvider(
              schema,
              multiBatchData(0, 1), // batch 0: id=1,  name=Name1
              multiBatchData(1, 2), // batch 1: id=3,  name=Name3 ; id=4, name=Name4
              multiBatchData(4, 1) // batch 2: id=5,  name=Name5 (baseId=4*1+1=5)
              );

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("multi", multiTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("s", mySchema));
      ctx.registerCatalog("multi_cat", myCatalog, allocator);

      // Collect all results from the lazy stream into a flat list of (id, name) pairs
      List<Long> ids = new ArrayList<>();
      List<String> names = new ArrayList<>();

      try (DataFrame df = ctx.sql("SELECT * FROM multi_cat.s.multi ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        while (stream.loadNextBatch()) {
          BigIntVector id = (BigIntVector) root.getVector("id");
          VarCharVector name = (VarCharVector) root.getVector("name");
          for (int i = 0; i < root.getRowCount(); i++) {
            ids.add(id.get(i));
            names.add(new String(name.get(i)));
          }
        }
      }

      // All 4 rows should be present (multiBatchData gives 1+2+1 = 4 rows with ids 1,3,4,5)
      assertEquals(List.of(1L, 3L, 4L, 5L), ids);
      assertEquals(List.of("Name1", "Name3", "Name4", "Name5"), names);
    }
  }

  // BatchPopulator factory methods for common test data patterns

  /** Creates a batch populator for the standard test data (Alice, Bob, Charlie). */
  private BatchPopulator usersDataBatch() {
    return root -> {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      idVector.allocateNew(3);
      nameVector.allocateNew(3);

      idVector.set(0, 1);
      nameVector.setSafe(0, "Alice".getBytes());
      idVector.set(1, 2);
      nameVector.setSafe(1, "Bob".getBytes());
      idVector.set(2, 3);
      nameVector.setSafe(2, "Charlie".getBytes());

      idVector.setValueCount(3);
      nameVector.setValueCount(3);
      root.setRowCount(3);
    };
  }

  /** Creates a batch populator for product data. */
  private BatchPopulator productsDataBatch() {
    return root -> {
      BigIntVector idVector = (BigIntVector) root.getVector("product_id");
      BigIntVector userIdVector = (BigIntVector) root.getVector("user_id");
      BigIntVector priceVector = (BigIntVector) root.getVector("price");

      idVector.allocateNew(2);
      userIdVector.allocateNew(2);
      priceVector.allocateNew(2);

      idVector.set(0, 100);
      userIdVector.set(0, 1); // Belongs to Alice (id=1)
      priceVector.set(0, 999);
      idVector.set(1, 101);
      userIdVector.set(1, 2); // Belongs to Bob (id=2)
      priceVector.set(1, 1499);

      idVector.setValueCount(2);
      userIdVector.setValueCount(2);
      priceVector.setValueCount(2);
      root.setRowCount(2);
    };
  }

  /** Creates a batch populator for multi-batch test (2 rows per batch with sequential ids). */
  private BatchPopulator multiBatchData(int batchIndex, int rowsPerBatch) {
    return root -> {
      BigIntVector idVector = (BigIntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      idVector.allocateNew(rowsPerBatch);
      nameVector.allocateNew(rowsPerBatch);

      int baseId = batchIndex * rowsPerBatch + 1;
      for (int i = 0; i < rowsPerBatch; i++) {
        int id = baseId + i;
        idVector.set(i, id);
        nameVector.setSafe(i, ("Name" + id).getBytes());
      }

      idVector.setValueCount(rowsPerBatch);
      nameVector.setValueCount(rowsPerBatch);
      root.setRowCount(rowsPerBatch);
    };
  }

  // ==========================================================================
  // Null-byte round-trip tests via custom catalog callbacks
  // ==========================================================================

  /**
   * Verifies that table names containing null bytes survive the Java→Rust→Java round-trip through
   * the DfStringArray callback path. This was previously broken because null-separated encoding
   * split on \0.
   */
  @Test
  void testTableNamesWithNullByteRoundTrip() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      Schema simpleSchema = createUsersSchema();
      BatchPopulator simpleBatch = usersDataBatch();

      // Create schema provider with table names containing null bytes
      String tableWithNull = "tbl\0name";
      String normalTable = "normal";
      Map<String, TableProvider> tables = new HashMap<>();
      tables.put(tableWithNull, new TestTableProvider(simpleSchema, simpleBatch));
      tables.put(normalTable, new TestTableProvider(simpleSchema, simpleBatch));
      SchemaProvider mySchema = new SimpleSchemaProvider(tables);

      // Create catalog provider with schema names containing null bytes
      String schemaWithNull = "sch\0ema";
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of(schemaWithNull, mySchema));
      ctx.registerCatalog("test_cat", myCatalog, allocator);

      // Verify schema names round-trip correctly
      Optional<CatalogProvider> catalog = ctx.catalog("test_cat");
      assertTrue(catalog.isPresent());
      List<String> schemaNames = catalog.get().schemaNames();
      assertTrue(
          schemaNames.contains(schemaWithNull),
          "Schema name with null byte should survive round-trip. Got: " + schemaNames);

      // Verify table names round-trip correctly
      Optional<SchemaProvider> schema = catalog.get().schema(schemaWithNull);
      assertTrue(schema.isPresent());
      List<String> tableNames = schema.get().tableNames();
      assertTrue(
          tableNames.contains(tableWithNull),
          "Table name with null byte should survive round-trip. Got: " + tableNames);
      assertTrue(
          tableNames.contains(normalTable),
          "Normal table name should also be present. Got: " + tableNames);
    }
  }
}
