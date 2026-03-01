package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class DiplomatBridgeTest {

  @Test
  void testCreateAndDestroyContext() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void testSessionId() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      String id = ctx.sessionId();
      assertNotNull(id);
      assertFalse(id.isEmpty());
    }
  }

  @Test
  void testSessionStartTimeMillis() {
    long before = System.currentTimeMillis();
    try (DfSessionContext ctx = new DfSessionContext()) {
      long startTime = ctx.sessionStartTimeMillis();
      long after = System.currentTimeMillis();
      assertTrue(startTime >= before - 1000);
      assertTrue(startTime <= after + 1000);
    }
  }

  @Test
  void testSqlSuccess() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      try (DfDataFrame df = ctx.sql("SELECT 1 + 1 AS result")) {
        assertNotNull(df);
      }
    }
  }

  @Test
  void testSqlError() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      DfError error =
          assertThrows(DfError.class, () -> ctx.sql("SELECT * FROM nonexistent_table_xyz"));
      String msg = error.toDisplay();
      assertTrue(msg.contains("nonexistent_table_xyz"), "Error: " + msg);
      error.close();
    }
  }

  @Test
  void testSessionIdStableAcrossOperations() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      String id1 = ctx.sessionId();
      try (DfDataFrame df = ctx.sql("SELECT 42 AS answer")) {
        assertNotNull(df);
      }
      String id2 = ctx.sessionId();
      assertEquals(id1, id2);
    }
  }

  @Test
  void testCollectToString() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      try (DfDataFrame df = ctx.sql("SELECT 1 AS a, 2 AS b")) {
        String output = df.collectToString();
        assertTrue(output.contains("a"), "Output should contain column 'a': " + output);
        assertTrue(output.contains("b"), "Output should contain column 'b': " + output);
        assertTrue(output.contains("1"), "Output should contain value 1: " + output);
        assertTrue(output.contains("2"), "Output should contain value 2: " + output);
      }
    }
  }

  @Test
  void testRegisterTableAndQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      VectorSchemaRoot root = createTestData(allocator);
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);
        try (DfArrowBatch batch =
            DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
          ctx.registerTable("test", batch);
        }
      }

      try (DfDataFrame df = ctx.sql("SELECT x, y FROM test WHERE x > 1")) {
        String output = df.collectToString();
        // Should contain x=2,y=20 and x=3,y=30 but not x=1,y=10
        assertTrue(output.contains("2"), "Output should contain x=2: " + output);
        assertTrue(output.contains("3"), "Output should contain x=3: " + output);
        assertTrue(output.contains("20"), "Output should contain y=20: " + output);
        assertTrue(output.contains("30"), "Output should contain y=30: " + output);
      }

      root.close();
    }
  }

  @Test
  void testRegisterTableAggregate() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      VectorSchemaRoot root = createTestData(allocator);
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);
        try (DfArrowBatch batch =
            DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
          ctx.registerTable("test", batch);
        }
      }

      try (DfDataFrame df = ctx.sql("SELECT SUM(y) AS total FROM test")) {
        String output = df.collectToString();
        // 10 + 20 + 30 = 60
        assertTrue(output.contains("60"), "Output should contain sum 60: " + output);
      }

      root.close();
    }
  }

  @Test
  void testParseSqlExpr() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema =
          new Schema(
              List.of(
                  Field.nullable("a", new ArrowType.Int(32, true)),
                  Field.nullable("b", new ArrowType.Int(32, true))));

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);
        try (DfArrowSchema dfSchema = DfArrowSchema.fromAddress(ffiSchema.memoryAddress());
            DfExprBytes exprBytes = ctx.parseSqlExpr("a + b", dfSchema)) {

          long len = exprBytes.len();
          assertTrue(len > 0, "Expression bytes should not be empty");

          try (Arena arena = Arena.ofConfined()) {
            MemorySegment dest = arena.allocate(len);
            exprBytes.copyTo(dest.address(), len);
            byte[] bytes = dest.toArray(ValueLayout.JAVA_BYTE);

            List<Expr> exprs = ExprProtoConverter.fromProtoBytes(bytes);
            assertEquals(1, exprs.size());
            assertInstanceOf(Expr.BinaryExpr.class, exprs.get(0));
            Expr.BinaryExpr bin = (Expr.BinaryExpr) exprs.get(0);
            assertEquals(Operator.Plus, bin.op());
            assertInstanceOf(Expr.ColumnExpr.class, bin.left());
            assertInstanceOf(Expr.ColumnExpr.class, bin.right());
            assertEquals("a", ((Expr.ColumnExpr) bin.left()).column().name());
            assertEquals("b", ((Expr.ColumnExpr) bin.right()).column().name());
          }
        }
      }
    }
  }

  @Test
  void testParseSqlExprError() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = new Schema(List.of());

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);
        try (DfArrowSchema dfSchema = DfArrowSchema.fromAddress(ffiSchema.memoryAddress())) {
          DfError error =
              assertThrows(DfError.class, () -> ctx.parseSqlExpr("+++invalid", dfSchema));
          String msg = error.toDisplay();
          assertTrue(msg.length() > 0, "Error message should not be empty");
          error.close();
        }
      }
    }
  }

  // ── Diplomat registerCatalog tests ──

  @Test
  void testRegisterCatalogBasic() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      TableProvider myTable = new TestTableProvider(schema, usersDataBatch());
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("my_table", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("my_schema", mySchema));

      ctx.registerCatalog("my_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df = ctx.sql("SELECT * FROM my_catalog.my_schema.my_table ORDER BY id")) {
        String output = df.collectToString();
        assertTrue(output.contains("1"), "Should contain id=1: " + output);
        assertTrue(output.contains("2"), "Should contain id=2: " + output);
        assertTrue(output.contains("3"), "Should contain id=3: " + output);
        assertTrue(output.contains("Alice"), "Should contain Alice: " + output);
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("Charlie"), "Should contain Charlie: " + output);
      }
    }
  }

  @Test
  void testRegisterCatalogWithFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      TableProvider myTable = new TestTableProvider(schema, usersDataBatch());
      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("test_table", myTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("public", mySchema));

      ctx.registerCatalog("test_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df =
          ctx.sql("SELECT id, name FROM test_catalog.public.test_table WHERE id > 1 ORDER BY id")) {
        String output = df.collectToString();
        // Should contain id=2 and id=3 but not id=1
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("Charlie"), "Should contain Charlie: " + output);
        assertFalse(output.contains("Alice"), "Should not contain Alice (filtered out): " + output);
      }
    }
  }

  @Test
  void testRegisterCatalogMultipleTables() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      TableProvider usersTable = new TestTableProvider(createUsersSchema(), usersDataBatch());
      TableProvider productsTable =
          new TestTableProvider(createProductsSchema(), productsDataBatch());

      Map<String, TableProvider> tables = new HashMap<>();
      tables.put("users", usersTable);
      tables.put("products", productsTable);

      SchemaProvider mySchema = new SimpleSchemaProvider(tables);
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));

      ctx.registerCatalog("store", new DfCatalogAdapter(myCatalog, allocator));

      // Query users table
      try (DfDataFrame df = ctx.sql("SELECT * FROM store.default.users ORDER BY id")) {
        String output = df.collectToString();
        assertTrue(output.contains("Alice"), "Should contain Alice: " + output);
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("Charlie"), "Should contain Charlie: " + output);
      }

      // Query products table
      try (DfDataFrame df = ctx.sql("SELECT * FROM store.default.products ORDER BY product_id")) {
        String output = df.collectToString();
        assertTrue(output.contains("100"), "Should contain product_id=100: " + output);
        assertTrue(output.contains("101"), "Should contain product_id=101: " + output);
        assertTrue(output.contains("999"), "Should contain price=999: " + output);
        assertTrue(output.contains("1499"), "Should contain price=1499: " + output);
      }
    }
  }

  @Test
  void testRegisterCatalogJoin() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      TableProvider usersTable = new TestTableProvider(createUsersSchema(), usersDataBatch());
      TableProvider productsTable =
          new TestTableProvider(createProductsSchema(), productsDataBatch());

      Map<String, TableProvider> tables = new HashMap<>();
      tables.put("users", usersTable);
      tables.put("products", productsTable);

      SchemaProvider mySchema = new SimpleSchemaProvider(tables);
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("default", mySchema));

      ctx.registerCatalog("store", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df =
          ctx.sql(
              "SELECT u.name, p.product_id, p.price "
                  + "FROM store.default.users u "
                  + "JOIN store.default.products p ON u.id = p.user_id "
                  + "ORDER BY p.product_id")) {
        String output = df.collectToString();
        assertTrue(output.contains("Alice"), "Should contain Alice: " + output);
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("100"), "Should contain product_id=100: " + output);
        assertTrue(output.contains("101"), "Should contain product_id=101: " + output);
        assertTrue(output.contains("999"), "Should contain price=999: " + output);
        assertTrue(output.contains("1499"), "Should contain price=1499: " + output);
      }
    }
  }

  @Test
  void testRegisterCatalogLimitPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      AtomicReference<Long> capturedLimit = new AtomicReference<>();

      TableProvider limitCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, List<Expr> filters, List<Integer> projection, Long limit) {
              capturedLimit.set(limit);
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("limit_test", limitCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("limit_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df = ctx.sql("SELECT * FROM limit_catalog.test.limit_test LIMIT 2")) {
        String output = df.collectToString();
        assertNotNull(output);
      }

      assertNotNull(capturedLimit.get(), "Limit should have been passed to scan()");
      assertEquals(2L, capturedLimit.get(), "Limit value should be 2");
    }
  }

  @Test
  void testRegisterCatalogNoLimitPassedAsNull() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      AtomicReference<Long> capturedLimit = new AtomicReference<>(999L);

      TableProvider noLimitTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, List<Expr> filters, List<Integer> projection, Long limit) {
              capturedLimit.set(limit);
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("no_limit", noLimitTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("no_limit_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df = ctx.sql("SELECT * FROM no_limit_catalog.test.no_limit")) {
        String output = df.collectToString();
        assertTrue(output.contains("Alice"), "Should contain Alice: " + output);
      }

      assertNull(capturedLimit.get(), "Limit should be null when no LIMIT clause is used");
    }
  }

  @Test
  void testRegisterCatalogExactFilterPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      AtomicReference<List<FilterPushDown>> capturedPushdowns = new AtomicReference<>();

      TableProvider exactFilterTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public List<FilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
              List<FilterPushDown> result =
                  new ArrayList<>(Collections.nCopies(filters.size(), FilterPushDown.EXACT));
              capturedPushdowns.set(result);
              return result;
            }

            @Override
            public ExecutionPlan scan(
                Session session, List<Expr> filters, List<Integer> projection, Long limit) {
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("filter_test", exactFilterTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("exact_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df = ctx.sql("SELECT * FROM exact_catalog.test.filter_test WHERE id > 1")) {
        String output = df.collectToString();
        // With EXACT pushdown, DataFusion trusts our provider to handle the filter.
        // Since our provider returns all data, we get all 3 rows.
        assertTrue(output.contains("Alice"), "EXACT: DataFusion trusts provider: " + output);
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("Charlie"), "Should contain Charlie: " + output);
      }

      assertNotNull(capturedPushdowns.get(), "supportsFiltersPushdown should have been called");
      assertFalse(capturedPushdowns.get().isEmpty(), "Should have received at least one filter");
    }
  }

  @Test
  void testRegisterCatalogUnsupportedFilterPushdown() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();

      TableProvider unsupportedTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public List<FilterPushDown> supportsFiltersPushdown(List<Expr> filters) {
              return Collections.nCopies(filters.size(), FilterPushDown.UNSUPPORTED);
            }

            @Override
            public ExecutionPlan scan(
                Session session, List<Expr> filters, List<Integer> projection, Long limit) {
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("unsupported", unsupportedTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("unsupported_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df =
          ctx.sql("SELECT * FROM unsupported_catalog.test.unsupported WHERE id > 1 ORDER BY id")) {
        String output = df.collectToString();
        // DataFusion applies filter post-scan since provider said UNSUPPORTED
        assertTrue(output.contains("Bob"), "Should contain Bob: " + output);
        assertTrue(output.contains("Charlie"), "Should contain Charlie: " + output);
        assertFalse(
            output.contains("Alice"),
            "Should not contain Alice (filtered by DataFusion): " + output);
      }
    }
  }

  @Test
  void testRegisterCatalogFilterExprStructure() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = createUsersSchema();
      AtomicReference<List<Expr>> capturedFilters = new AtomicReference<>();

      TableProvider filterCapturingTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return schema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, List<Expr> filters, List<Integer> projection, Long limit) {
              capturedFilters.set(filters);
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("expr_test", filterCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("expr_catalog", new DfCatalogAdapter(myCatalog, allocator));

      try (DfDataFrame df = ctx.sql("SELECT * FROM expr_catalog.test.expr_test WHERE id > 1")) {
        df.collectToString(); // consume
      }

      List<Expr> filters = capturedFilters.get();
      assertNotNull(filters, "Filters should have been captured");
      assertFalse(filters.isEmpty(), "Should have at least one filter");

      // The filter should be a BinaryExpr: id > 1
      Expr filter = filters.get(0);
      assertInstanceOf(Expr.BinaryExpr.class, filter, "Filter should be a BinaryExpr");
      Expr.BinaryExpr binExpr = (Expr.BinaryExpr) filter;
      assertEquals(Operator.Gt, binExpr.op(), "Operator should be Gt (>)");

      assertInstanceOf(Expr.ColumnExpr.class, binExpr.left(), "Left should be ColumnExpr");
      assertEquals("id", ((Expr.ColumnExpr) binExpr.left()).column().name());

      assertInstanceOf(Expr.LiteralExpr.class, binExpr.right(), "Right should be LiteralExpr");
      assertInstanceOf(ScalarValue.Int64.class, ((Expr.LiteralExpr) binExpr.right()).value());
      assertEquals(1L, ((ScalarValue.Int64) ((Expr.LiteralExpr) binExpr.right()).value()).value());
    }
  }

  // ── Test helper classes (reusable for catalog chain tests) ──

  @FunctionalInterface
  interface BatchPopulator {
    void populate(VectorSchemaRoot root);
  }

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
    public ExecutionPlan scan(
        Session session, List<Expr> filters, List<Integer> projection, Long limit) {
      return new TestExecutionPlan(schema, batches);
    }
  }

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
    public RecordBatchReader execute(int partition, BufferAllocator allocator) {
      return new TestRecordBatchReader(schema, batches, allocator);
    }
  }

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
  }

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

  // ── Shared helper methods ──

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

  private BatchPopulator productsDataBatch() {
    return root -> {
      BigIntVector idVector = (BigIntVector) root.getVector("product_id");
      BigIntVector userIdVector = (BigIntVector) root.getVector("user_id");
      BigIntVector priceVector = (BigIntVector) root.getVector("price");

      idVector.allocateNew(2);
      userIdVector.allocateNew(2);
      priceVector.allocateNew(2);

      idVector.set(0, 100);
      userIdVector.set(0, 1);
      priceVector.set(0, 999);
      idVector.set(1, 101);
      userIdVector.set(1, 2);
      priceVector.set(1, 1499);

      idVector.setValueCount(2);
      userIdVector.setValueCount(2);
      priceVector.setValueCount(2);
      root.setRowCount(2);
    };
  }
}
