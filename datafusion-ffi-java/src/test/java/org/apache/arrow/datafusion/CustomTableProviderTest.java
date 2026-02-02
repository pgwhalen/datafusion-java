package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      Schema schema = createTestSchema();
      VectorSchemaRoot data = createTestData(allocator, schema);

      TableProvider myTable = new SimpleTableProvider(schema, data, allocator);
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

      data.close();
    }
  }

  @Test
  void testCustomTableProviderWithFilter() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createTestSchema();
      VectorSchemaRoot data = createTestData(allocator, schema);

      TableProvider myTable = new SimpleTableProvider(schema, data, allocator);
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

      data.close();
    }
  }

  @Test
  void testCustomTableProviderWithProjection() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = createTestSchema();
      VectorSchemaRoot data = createTestData(allocator, schema);

      TableProvider myTable = new SimpleTableProvider(schema, data, allocator);
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

      data.close();
    }
  }

  @Test
  void testMultipleTablesInSchema() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create two tables with different data
      Schema schema1 = createTestSchema();
      VectorSchemaRoot data1 = createTestData(allocator, schema1);

      Schema schema2 = createSecondTestSchema();
      VectorSchemaRoot data2 = createSecondTestData(allocator, schema2);

      TableProvider table1 = new SimpleTableProvider(schema1, data1, allocator);
      TableProvider table2 = new SimpleTableProvider(schema2, data2, allocator);

      Map<String, TableProvider> tables = new HashMap<>();
      tables.put("users", table1);
      tables.put("products", table2);

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

      data1.close();
      data2.close();
    }
  }

  // Helper methods to create test schemas and data

  private Schema createTestSchema() {
    return new Schema(
        Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
  }

  private VectorSchemaRoot createTestData(BufferAllocator allocator, Schema schema) {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

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

    return root;
  }

  private Schema createSecondTestSchema() {
    return new Schema(
        Arrays.asList(
            new Field("product_id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("price", FieldType.nullable(new ArrowType.Int(64, true)), null)));
  }

  private VectorSchemaRoot createSecondTestData(BufferAllocator allocator, Schema schema) {
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    BigIntVector idVector = (BigIntVector) root.getVector("product_id");
    BigIntVector priceVector = (BigIntVector) root.getVector("price");

    idVector.allocateNew(2);
    priceVector.allocateNew(2);

    idVector.set(0, 100);
    priceVector.set(0, 999);

    idVector.set(1, 101);
    priceVector.set(1, 1499);

    idVector.setValueCount(2);
    priceVector.setValueCount(2);
    root.setRowCount(2);

    return root;
  }

  // Simple implementation classes for testing

  /** A simple TableProvider that returns fixed data from a VectorSchemaRoot. */
  static class SimpleTableProvider implements TableProvider {
    private final Schema schema;
    private final VectorSchemaRoot data;
    private final BufferAllocator allocator;

    SimpleTableProvider(Schema schema, VectorSchemaRoot data, BufferAllocator allocator) {
      this.schema = schema;
      this.data = data;
      this.allocator = allocator;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ExecutionPlan scan(int[] projection, Long limit) {
      return new SimpleExecutionPlan(schema, data);
    }
  }

  /** A simple ExecutionPlan that returns data from a VectorSchemaRoot. */
  static class SimpleExecutionPlan implements ExecutionPlan {
    private final Schema schema;
    private final VectorSchemaRoot data;

      SimpleExecutionPlan(Schema schema, VectorSchemaRoot data) {
        this.schema = schema;
        this.data = data;
      }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public RecordBatchReader execute(int partition, BufferAllocator allocator) {
      // Copy the data to a new VectorSchemaRoot for the reader
      VectorSchemaRoot copy = VectorSchemaRoot.create(schema, allocator);

      // Transfer the data
      for (int i = 0; i < schema.getFields().size(); i++) {
        copy.getFieldVectors().get(i).allocateNew();
        for (int row = 0; row < data.getRowCount(); row++) {
          copy.getFieldVectors().get(i).copyFromSafe(row, row, data.getFieldVectors().get(i));
        }
        copy.getFieldVectors().get(i).setValueCount(data.getRowCount());
      }
      copy.setRowCount(data.getRowCount());

      return new SimpleRecordBatchReader(copy);
    }
  }

  /** A simple RecordBatchReader that returns a single batch. */
  static class SimpleRecordBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private boolean consumed = false;

    SimpleRecordBatchReader(VectorSchemaRoot root) {
      this.root = root;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public boolean loadNextBatch() {
      if (consumed) {
        return false;
      }
      consumed = true;
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
    public TableProvider table(String name) {
      return tables.get(name);
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
    public SchemaProvider schema(String name) {
      return schemas.get(name);
    }
  }
}
