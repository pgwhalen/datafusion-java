package org.apache.arrow.datafusion.ffi;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
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
  void testStringDataWithDictionaryEncoding() {
    // This test exercises the CDataDictionaryProvider by using string data
    // DataFusion uses dictionary encoding for string operations like GROUP BY
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data with string column containing repeated values
      // Repeated values are good candidates for dictionary encoding
      VectorSchemaRoot testData = createStringTestData(allocator);
      ctx.registerTable("categories", testData, allocator);

      // GROUP BY on string column typically produces dictionary-encoded output
      try (DataFrame df =
              ctx.sql(
                  "SELECT category, COUNT(*) as cnt FROM categories GROUP BY category ORDER BY category");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount()); // 3 distinct categories

        // Get the category vector - may be dictionary-encoded or regular string
        FieldVector categoryVector = root.getVector("category");
        BigIntVector countVector = (BigIntVector) root.getVector("cnt");

        // Extract string values (works for both dictionary-encoded and plain strings)
        String[] categories = new String[3];
        for (int i = 0; i < 3; i++) {
          categories[i] = getStringValue(categoryVector, i);
        }

        // Verify results (ordered alphabetically)
        assertEquals("A", categories[0]);
        assertEquals("B", categories[1]);
        assertEquals("C", categories[2]);

        assertEquals(2, countVector.get(0)); // 'A' appears 2 times
        assertEquals(2, countVector.get(1)); // 'B' appears 2 times
        assertEquals(1, countVector.get(2)); // 'C' appears 1 time

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testArrowFunctionOnStrings() {
    // Test Arrow functions that process string data - exercises dictionary handling
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createStringTestData(allocator);
      ctx.registerTable("categories", testData, allocator);

      // arrow_cast forces specific Arrow types and can trigger dictionary handling
      try (DataFrame df = ctx.sql("SELECT DISTINCT category FROM categories ORDER BY category");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        FieldVector categoryVector = root.getVector("category");
        assertEquals("A", getStringValue(categoryVector, 0));
        assertEquals("B", getStringValue(categoryVector, 1));
        assertEquals("C", getStringValue(categoryVector, 2));

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  /** Helper to extract string value from a vector that may be dictionary-encoded */
  private String getStringValue(FieldVector vector, int index) {
    if (vector instanceof VarCharVector) {
      return new String(((VarCharVector) vector).get(index));
    } else {
      // For dictionary-encoded vectors, get the decoded value
      Object value = vector.getObject(index);
      if (value instanceof byte[]) {
        return new String((byte[]) value);
      }
      return value.toString();
    }
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

    // Create data with repeated category values (good for dictionary encoding)
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

  @Test
  void testDictionaryEncodedInputData() {
    // This test creates dictionary-encoded data in Java and passes it to DataFusion.
    // This exercises the CDataDictionaryProvider on the input path.
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create a dictionary with unique string values
      VarCharVector dictVector = new VarCharVector("dict", allocator);
      dictVector.allocateNew(3);
      dictVector.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      dictVector.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      dictVector.setSafe(2, "cherry".getBytes(StandardCharsets.UTF_8));
      dictVector.setValueCount(3);

      // Create the dictionary encoding with ID 1
      DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));
      Dictionary dictionary = new Dictionary(dictVector, encoding);

      // Create a vector with values to encode: apple, banana, apple, cherry, banana
      VarCharVector toEncode = new VarCharVector("fruit", allocator);
      toEncode.allocateNew(5);
      toEncode.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      toEncode.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      toEncode.setSafe(2, "apple".getBytes(StandardCharsets.UTF_8));
      toEncode.setSafe(3, "cherry".getBytes(StandardCharsets.UTF_8));
      toEncode.setSafe(4, "banana".getBytes(StandardCharsets.UTF_8));
      toEncode.setValueCount(5);

      // Encode the vector - this creates an IntVector with indices into the dictionary
      IntVector encodedVector = (IntVector) DictionaryEncoder.encode(toEncode, dictionary);
      toEncode.close(); // No longer needed

      // Create an ID column
      BigIntVector idVector = new BigIntVector("id", allocator);
      idVector.allocateNew(5);
      for (int i = 0; i < 5; i++) {
        idVector.set(i, i + 1);
      }
      idVector.setValueCount(5);

      // Create schema with dictionary-encoded field
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                  new Field(
                      "fruit", new FieldType(true, new ArrowType.Int(32, true), encoding), null)));

      // Create VectorSchemaRoot with the encoded vector
      VectorSchemaRoot root =
          new VectorSchemaRoot(schema, Arrays.asList(idVector, encodedVector), 5);

      // Create dictionary provider
      DictionaryProvider.MapDictionaryProvider provider =
          new DictionaryProvider.MapDictionaryProvider();
      provider.put(dictionary);

      // Register table with dictionary provider
      ctx.registerTable("fruits", root, provider, allocator);

      // Query the table - DataFusion should decode the dictionary
      try (DataFrame df = ctx.sql("SELECT id, fruit FROM fruits ORDER BY id");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot result = stream.getVectorSchemaRoot();

        assertTrue(stream.loadNextBatch());
        assertEquals(5, result.getRowCount());

        BigIntVector resultId = (BigIntVector) result.getVector("id");
        FieldVector fruitVector = result.getVector("fruit");

        // Verify the data came through correctly
        assertEquals(1, resultId.get(0));
        assertEquals(2, resultId.get(1));
        assertEquals(3, resultId.get(2));
        assertEquals(4, resultId.get(3));
        assertEquals(5, resultId.get(4));

        // The fruit vector may be dictionary-encoded - decode it if necessary
        DictionaryEncoding dictEncoding = fruitVector.getField().getDictionary();
        String[] fruitValues = new String[5];
        if (dictEncoding != null) {
          // Vector is dictionary-encoded - decode using the stream's dictionary provider
          long dictId = dictEncoding.getId();
          Dictionary dict = stream.lookup(dictId);
          assertNotNull(dict, "Dictionary with ID " + dictId + " should exist in provider");

          try (VarCharVector decoded =
              (VarCharVector) DictionaryEncoder.decode(fruitVector, dict)) {
            for (int i = 0; i < 5; i++) {
              fruitValues[i] = new String(decoded.get(i), StandardCharsets.UTF_8);
            }
          }
        } else {
          // Vector is already decoded
          for (int i = 0; i < 5; i++) {
            fruitValues[i] = getStringValue(fruitVector, i);
          }
        }

        assertEquals("apple", fruitValues[0]);
        assertEquals("banana", fruitValues[1]);
        assertEquals("apple", fruitValues[2]);
        assertEquals("cherry", fruitValues[3]);
        assertEquals("banana", fruitValues[4]);

        assertFalse(stream.loadNextBatch());
      }

      root.close();
      dictVector.close();
    }
  }
}
