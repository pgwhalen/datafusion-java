package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
          RecordBatchStream stream = df.executeStream(allocator)) {

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
            public ExecutionPlan scan(int[] projection, Long limit) {
              capturedLimit.set(limit);
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema = new SimpleSchemaProvider(Map.of("limit_test", limitCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("limit_catalog", myCatalog, allocator);

      // Query with LIMIT clause
      try (DataFrame df = ctx.sql("SELECT * FROM limit_catalog.test.limit_test LIMIT 2");
          RecordBatchStream stream = df.executeStream(allocator)) {

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
            public ExecutionPlan scan(int[] projection, Long limit) {
              capturedLimit.set(limit);
              return new TestExecutionPlan(schema, List.of(usersDataBatch()));
            }
          };

      SchemaProvider mySchema =
          new SimpleSchemaProvider(Map.of("no_limit_test", limitCapturingTable));
      CatalogProvider myCatalog = new SimpleCatalogProvider(Map.of("test", mySchema));

      ctx.registerCatalog("no_limit_catalog", myCatalog, allocator);

      // Query without LIMIT clause
      try (DataFrame df = ctx.sql("SELECT * FROM no_limit_catalog.test.no_limit_test");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount()); // All rows returned
      }

      // Verify that no limit was passed (null)
      assertNull(capturedLimit.get(), "Limit should be null when no LIMIT clause is used");
    }
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
    public ExecutionPlan scan(int[] projection, Long limit) {
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
    public RecordBatchReader execute(int partition, BufferAllocator allocator) {
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
}
