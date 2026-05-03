package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestExecuteStream {
  @Test
  public void executeStream(@TempDir Path tempDir) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      Path csvFilePath = tempDir.resolve("data.csv");

      List<String> lines = Arrays.asList("x,y,z", "1,2,3.5", "4,5,6.5", "7,8,9.5");
      Files.write(csvFilePath, lines);

      context.registerCsv("test", csvFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT y,z FROM test WHERE x > 3")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        expect("y", "z")
            .row(5L, 6.5)
            .row(8L, 9.5)
            .assertMatches(
                stream.getVectorSchemaRoot(), () -> stream.loadNextBatch().join(), stream);
      }
    }
  }

  @Test
  public void readDictionaryData() throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {

      URL fileUrl = this.getClass().getResource("/dictionary_data.parquet");
      Path parquetFilePath = Paths.get(fileUrl.getPath());

      context.registerParquet("test", parquetFilePath).join();

      try (RecordBatchStream stream =
          context
              .sql("SELECT x,y FROM test")
              .thenComposeAsync(df -> df.executeStream(allocator))
              .join()) {
        // Dictionary auto-decode handled by the DSL via the DictionaryProvider.
        // The parquet has 100 rows where x cycles "one", "two", "three" and y cycles
        // "four", "five", "six".
        String[] xExpected = {"one", "two", "three"};
        String[] yExpected = {"four", "five", "six"};
        VectorSchemaRootAssert assertion = expect("x", "y");
        for (int i = 0; i < 100; i++) {
          assertion.row(xExpected[i % 3], yExpected[i % 3]);
        }
        assertion.assertMatches(
            stream.getVectorSchemaRoot(), () -> stream.loadNextBatch().join(), stream);
      }
    }
  }
}
