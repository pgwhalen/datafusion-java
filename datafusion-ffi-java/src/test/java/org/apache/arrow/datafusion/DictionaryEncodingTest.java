package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
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

/** Dictionary encoding tests for the DataFusion FFI Java bindings. */
public class DictionaryEncodingTest {

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
      ctx.registerBatch("fruits", root, provider, allocator);

      // Query the table - DataFusion should handle the dictionary-encoded data.
      // The DSL auto-decodes dictionary-encoded VarChar columns via the stream's
      // DictionaryProvider, so we assert against the decoded string values directly.
      try (DataFrame df = ctx.sql("SELECT id, fruit FROM fruits ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "fruit")
            .row(1L, "apple")
            .row(2L, "banana")
            .row(3L, "apple")
            .row(4L, "cherry")
            .row(5L, "banana")
            .assertMatches(stream);
      }

      root.close();
      dictVector.close();
    }
  }
}
