package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
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

/** SQL functionality tests for the DataFusion FFI Java bindings. */
public class SqlTest {

  @Test
  void testSimpleSqlQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Execute a simple SQL query that doesn't require any tables
      try (DataFrame df = ctx.sql("SELECT 1 as x, 2 as y");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(1L, 2L).assertMatches(stream);
      }
    }
  }

  @Test
  void testRegisterAndQueryTable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data: x=[1,2,3], y=[10,20,30]
      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerBatch("test", testData, allocator);

      // Query the registered table
      try (DataFrame df = ctx.sql("SELECT x, y FROM test WHERE x > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y").row(2L, 20L).row(3L, 30L).assertMatches(stream);
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
      ctx.registerBatch("test", testData, allocator);

      // Execute aggregate query
      try (DataFrame df = ctx.sql("SELECT SUM(y) as total FROM test");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("total").row(60L).assertMatches(stream); // 10 + 20 + 30 = 60
      }

      testData.close();
    }
  }

  @Test
  void testMultipleQueries() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createTestData(allocator);
      ctx.registerBatch("test", testData, allocator);

      // Execute multiple queries on the same context
      for (int i = 0; i < 3; i++) {
        try (DataFrame df = ctx.sql("SELECT COUNT(*) as cnt FROM test");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          expect("cnt").row(3L).assertMatches(stream);
        }
      }

      testData.close();
    }
  }

  @Test
  void testStringDataQuery() {
    // Test SQL queries with string data
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data with string column containing repeated values
      VectorSchemaRoot testData = createStringTestData(allocator);
      ctx.registerBatch("categories", testData, allocator);

      // GROUP BY on string column
      try (DataFrame df =
              ctx.sql(
                  "SELECT category, COUNT(*) as cnt FROM categories GROUP BY category ORDER BY category");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("category", "cnt").row("A", 2L).row("B", 2L).row("C", 1L).assertMatches(stream);
      }

      testData.close();
    }
  }

  @Test
  void testDistinctQuery() {
    // Test DISTINCT on string columns
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createStringTestData(allocator);
      ctx.registerBatch("categories", testData, allocator);

      try (DataFrame df = ctx.sql("SELECT DISTINCT category FROM categories ORDER BY category");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("category").row("A").row("B").row("C").assertMatches(stream);
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

  private VectorSchemaRoot createStringTestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("category", FieldType.nullable(new ArrowType.Utf8()), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    BigIntVector idVector = (BigIntVector) root.getVector("id");
    VarCharVector categoryVector = (VarCharVector) root.getVector("category");

    // Create data with repeated category values
    // Categories: A, B, A, B, C
    int numRows = 5;
    idVector.allocateNew(numRows);
    categoryVector.allocateNew(numRows);

    idVector.set(0, 1);
    categoryVector.setSafe(0, "A".getBytes());

    idVector.set(1, 2);
    categoryVector.setSafe(1, "B".getBytes());

    idVector.set(2, 3);
    categoryVector.setSafe(2, "A".getBytes());

    idVector.set(3, 4);
    categoryVector.setSafe(3, "B".getBytes());

    idVector.set(4, 5);
    categoryVector.setSafe(4, "C".getBytes());

    idVector.setValueCount(numRows);
    categoryVector.setValueCount(numRows);
    root.setRowCount(numRows);

    return root;
  }
}
