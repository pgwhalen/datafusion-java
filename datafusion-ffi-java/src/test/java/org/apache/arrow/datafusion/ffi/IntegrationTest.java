package org.apache.arrow.datafusion.ffi;

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

/** Integration tests for the DataFusion FFI Java bindings. */
public class IntegrationTest {

  @Test
  void testContextCreation() {
    try (SessionContext ctx = new SessionContext()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void testSimpleSqlQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Execute a simple SQL query that doesn't require any tables
      try (DataFrame df = ctx.sql("SELECT 1 as x, 2 as y");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        org.apache.arrow.vector.types.pojo.Schema schema = root.getSchema();

        assertEquals(2, schema.getFields().size());
        assertEquals("x", schema.getFields().get(0).getName());
        assertEquals("y", schema.getFields().get(1).getName());

        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        BigIntVector xValues = (BigIntVector) root.getVector("x");
        BigIntVector yValues = (BigIntVector) root.getVector("y");
        assertEquals(1, xValues.get(0));
        assertEquals(2, yValues.get(0));

        assertFalse(stream.loadNextBatch());
      }
    }
  }

  @Test
  void testRegisterAndQueryTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data: x=[1,2,3], y=[10,20,30]
      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerTable("test", testData, allocator);

      // Query the registered table
      try (DataFrame df = ctx.sql("SELECT x, y FROM test WHERE x > 1");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());

        BigIntVector xValues = (BigIntVector) root.getVector("x");
        BigIntVector yValues = (BigIntVector) root.getVector("y");

        // Should have rows where x > 1, i.e., x=2,y=20 and x=3,y=30
        assertEquals(2, xValues.get(0));
        assertEquals(20, yValues.get(0));
        assertEquals(3, xValues.get(1));
        assertEquals(30, yValues.get(1));

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testAggregateQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data
      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerTable("test", testData, allocator);

      // Execute aggregate query
      try (DataFrame df = ctx.sql("SELECT SUM(y) as total FROM test");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        BigIntVector totalValues = (BigIntVector) root.getVector("total");
        assertEquals(60, totalValues.get(0)); // 10 + 20 + 30 = 60

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testInvalidSqlThrowsException() {
    try (SessionContext ctx = new SessionContext()) {
      assertThrows(RuntimeException.class, () -> ctx.sql("SELECT * FROM nonexistent_table"));
    }
  }

  @Test
  void testMultipleQueries() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerTable("test", testData, allocator);

      // Execute multiple queries on the same context
      for (int i = 0; i < 3; i++) {
        try (DataFrame df = ctx.sql("SELECT COUNT(*) as cnt FROM test");
            RecordBatchStream stream = df.executeStream(allocator)) {

          assertTrue(stream.loadNextBatch());
          BigIntVector countValues = (BigIntVector) stream.getVectorSchemaRoot().getVector("cnt");
          assertEquals(3, countValues.get(0));
        }
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
