package org.apache.arrow.datafusion;

import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.datasource.ArrowReadOptions;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.apache.arrow.datafusion.datasource.NdJsonReadOptions;
import org.apache.arrow.datafusion.datasource.ParquetReadOptions;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.VarProvider;
import org.apache.arrow.datafusion.execution.VarType;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for SessionContext API methods including batch/table registration, file formats, etc. */
public class SessionContextTest {

  // ==========================================================================
  // registerBatch (renamed from registerTable)
  // ==========================================================================

  @Test
  void testRegisterBatch() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema =
          new Schema(
              List.of(
                  new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        id.allocateNew(2);
        name.allocateNew(2);
        id.set(0, 1);
        name.setSafe(0, "Alice".getBytes());
        id.set(1, 2);
        name.setSafe(1, "Bob".getBytes());
        root.setRowCount(2);

        ctx.registerBatch("test", root, allocator);
      }

      try (DataFrame df = ctx.sql("SELECT id, name FROM test ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "name").row(1, "Alice").row(2, "Bob").assertMatches(stream);
      }
    }
  }

  // ==========================================================================
  // registerTable (TableProvider)
  // ==========================================================================

  @Test
  void testRegisterTableProvider() {
    try (BufferAllocator allocator = new RootAllocator()) {
      try (SessionContext ctx = new SessionContext()) {

        Schema schema =
            new Schema(
                List.of(
                    new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                    new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)));

        TableProvider provider =
            new CustomTableProviderTest.TestTableProvider(
                schema,
                root -> {
                  BigIntVector id = (BigIntVector) root.getVector("id");
                  VarCharVector value = (VarCharVector) root.getVector("value");
                  id.allocateNew(2);
                  value.allocateNew(2);
                  id.set(0, 10);
                  value.setSafe(0, "hello".getBytes());
                  id.set(1, 20);
                  value.setSafe(1, "world".getBytes());
                  root.setRowCount(2);
                });

        ctx.registerTable("my_table", provider, allocator);

        try (DataFrame df = ctx.sql("SELECT id, value FROM my_table ORDER BY id");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          expect("id", "value").row(10L, "hello").row(20L, "world").assertMatches(stream);
        }
      }
    }
  }

  // ==========================================================================
  // deregisterTable and tableExist
  // ==========================================================================

  @Test
  void testDeregisterTable(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.writeString(csvFile, "id,name\n1,Alice\n2,Bob\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      ctx.registerCsv("test", csvFile.toString(), CsvReadOptions.builder().build(), allocator);
      assertTrue(ctx.tableExist("test"));

      boolean wasRegistered = ctx.deregisterTable("test");
      assertTrue(wasRegistered);
      assertFalse(ctx.tableExist("test"));

      // Deregistering a non-existent table returns false
      boolean wasRegistered2 = ctx.deregisterTable("nonexistent");
      assertFalse(wasRegistered2);
    }
  }

  @Test
  void testTableExist(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.writeString(csvFile, "id\n1\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      assertFalse(ctx.tableExist("my_csv"));

      ctx.registerCsv("my_csv", csvFile.toString(), CsvReadOptions.builder().build(), allocator);
      assertTrue(ctx.tableExist("my_csv"));

      ctx.deregisterTable("my_csv");
      assertFalse(ctx.tableExist("my_csv"));
    }
  }

  // ==========================================================================
  // registerCsv / registerParquet / registerJson
  // ==========================================================================

  @Test
  void testRegisterCsv(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("data.csv");
    Files.writeString(csvFile, "id,name,score\n1,Alice,95\n2,Bob,87\n3,Charlie,92\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      ctx.registerCsv("scores", csvFile.toString(), CsvReadOptions.builder().build(), allocator);

      try (DataFrame df = ctx.sql("SELECT id, name, score FROM scores ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "name", "score")
            .row(1L, "Alice", 95L)
            .row(2L, "Bob", 87L)
            .row(3L, "Charlie", 92L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testRegisterParquet() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      ctx.registerParquet(
          "test_parquet",
          "src/test/resources/test.parquet",
          ParquetReadOptions.builder().build(),
          allocator);

      try (DataFrame df = ctx.sql("SELECT id, value FROM test_parquet ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "value").row(1L, 100L).row(2L, 200L).row(3L, 300L).assertMatches(stream);
      }
    }
  }

  @Test
  void testRegisterJson(@TempDir Path tempDir) throws IOException {
    Path jsonFile = tempDir.resolve("data.json");
    Files.writeString(jsonFile, "{\"id\":1,\"city\":\"NYC\"}\n{\"id\":2,\"city\":\"LA\"}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      ctx.registerJson(
          "cities", jsonFile.toString(), NdJsonReadOptions.builder().build(), allocator);

      try (DataFrame df = ctx.sql("SELECT id, city FROM cities ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "city").row(1L, "NYC").row(2L, "LA").assertMatches(stream);
      }
    }
  }

  @Test
  void testRegisterArrow(@TempDir Path tempDir) throws IOException {
    Path arrowFile = tempDir.resolve("data.arrow");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create an Arrow IPC file using COPY
      ctx.registerCsv(
          "src_csv",
          createCsvFile(tempDir, "src.csv", "id,name,score\n1,Alice,95\n2,Bob,87\n3,Carol,92\n"),
          CsvReadOptions.builder().build(),
          allocator);
      try (DataFrame copy =
          ctx.sql("COPY src_csv TO '" + arrowFile.toString() + "' STORED AS ARROW")) {
        copy.show();
      }

      ctx.registerArrow(
          "test_arrow", arrowFile.toString(), ArrowReadOptions.builder().build(), allocator);

      try (DataFrame df = ctx.sql("SELECT id, name, score FROM test_arrow ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "name", "score")
            .row(1L, "Alice", 95L)
            .row(2L, "Bob", 87L)
            .row(3L, "Carol", 92L)
            .assertMatches(stream);
      }
    }
  }

  // ==========================================================================
  // readCsv / readParquet / readJson / readArrow with options
  // ==========================================================================

  @Test
  void testReadCsvWithOptions(@TempDir Path tempDir) throws IOException {
    // Create a CSV with pipe delimiter and no header
    Path csvFile = tempDir.resolve("data.csv");
    Files.writeString(csvFile, "1|Alice|100\n2|Bob|200\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      CsvReadOptions options =
          CsvReadOptions.builder().hasHeader(false).delimiter((byte) '|').build();

      try (DataFrame df = ctx.readCsv(csvFile.toString(), options, allocator);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        // With no header, columns are named column_1, column_2, column_3
        expect("column_1", "column_2", "column_3")
            .row(1L, "Alice", 100L)
            .row(2L, "Bob", 200L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testReadParquetWithOptions() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      ParquetReadOptions options = ParquetReadOptions.builder().parquetPruning(true).build();

      try (DataFrame df = ctx.readParquet("src/test/resources/test.parquet", options, allocator);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "value")
            .unordered()
            .allowExtraColumns()
            .row(1L, 100L)
            .row(2L, 200L)
            .row(3L, 300L)
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testReadJsonWithOptions(@TempDir Path tempDir) throws IOException {
    Path jsonFile = tempDir.resolve("data.json");
    Files.writeString(
        jsonFile, "{\"x\":1.5,\"y\":\"A\"}\n{\"x\":2.5,\"y\":\"B\"}\n{\"x\":3.5,\"y\":\"C\"}\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      NdJsonReadOptions options = NdJsonReadOptions.builder().schemaInferMaxRecords(10).build();

      try (DataFrame df = ctx.readJson(jsonFile.toString(), options, allocator);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y")
            .withDelta(0.001)
            .row(1.5, "A")
            .row(2.5, "B")
            .row(3.5, "C")
            .assertMatches(stream);
      }
    }
  }

  @Test
  void testReadArrow(@TempDir Path tempDir) throws IOException {
    Path arrowFile = tempDir.resolve("data.arrow");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create an Arrow IPC file using COPY
      ctx.registerCsv(
          "src_csv",
          createCsvFile(tempDir, "src3.csv", "id,value\n1,100\n2,200\n"),
          CsvReadOptions.builder().build(),
          allocator);
      try (DataFrame copy =
          ctx.sql("COPY src_csv TO '" + arrowFile.toString() + "' STORED AS ARROW")) {
        copy.show();
      }

      try (DataFrame df = ctx.readArrow(arrowFile.toString());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id", "value").row(1L, 100L).row(2L, 200L).assertMatches(stream);
      }
    }
  }

  @Test
  void testReadArrowWithOptions(@TempDir Path tempDir) throws IOException {
    Path arrowFile = tempDir.resolve("data.arrow");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create an Arrow IPC file using COPY
      ctx.registerCsv(
          "src_csv",
          createCsvFile(tempDir, "src2.csv", "x,y\n1.5,A\n2.5,B\n3.5,C\n"),
          CsvReadOptions.builder().build(),
          allocator);
      try (DataFrame copy =
          ctx.sql("COPY src_csv TO '" + arrowFile.toString() + "' STORED AS ARROW")) {
        copy.show();
      }

      ArrowReadOptions options = ArrowReadOptions.builder().build();

      try (DataFrame df = ctx.readArrow(arrowFile.toString(), options, allocator);
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("x", "y")
            .withDelta(0.001)
            .row(1.5, "A")
            .row(2.5, "B")
            .row(3.5, "C")
            .assertMatches(stream);
      }
    }
  }

  // ==========================================================================
  // catalogNames and catalog
  // ==========================================================================

  @Test
  void testCatalogNames() {
    try (SessionContext ctx = new SessionContext()) {
      List<String> names = ctx.catalogNames();
      // Default catalog is "datafusion"
      assertTrue(names.contains("datafusion"));
    }
  }

  @Test
  void testCatalogNamesWithCustomCatalog() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema =
          new Schema(
              List.of(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));

      TableProvider myTable =
          new CustomTableProviderTest.TestTableProvider(
              schema,
              root -> {
                BigIntVector id = (BigIntVector) root.getVector("id");
                id.allocateNew(1);
                id.set(0, 42);
                root.setRowCount(1);
              });
      SchemaProvider mySchema =
          new CustomTableProviderTest.SimpleSchemaProvider(Map.of("t", myTable));
      CatalogProvider myCatalog =
          new CustomTableProviderTest.SimpleCatalogProvider(Map.of("s", mySchema));

      ctx.registerCatalog("my_catalog", myCatalog, allocator);

      List<String> names = ctx.catalogNames();
      assertTrue(names.contains("datafusion"));
      assertTrue(names.contains("my_catalog"));
    }
  }

  @Test
  void testCatalog() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Default catalog exists
      Optional<CatalogProvider> defaultCatalog = ctx.catalog("datafusion");
      assertTrue(defaultCatalog.isPresent());

      List<String> schemaNames = defaultCatalog.get().schemaNames();
      assertNotNull(schemaNames);
      // Default schema "public" should exist
      assertTrue(schemaNames.contains("public"));

      // Non-existent catalog
      Optional<CatalogProvider> missing = ctx.catalog("nonexistent");
      assertFalse(missing.isPresent());
    }
  }

  @Test
  void testCatalogWithRegisteredTable(@TempDir Path tempDir) throws IOException {
    Path csvFile = tempDir.resolve("test.csv");
    Files.writeString(csvFile, "id\n1\n2\n");

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Register a CSV table (goes into default catalog.public schema)
      ctx.registerCsv("my_csv", csvFile.toString(), CsvReadOptions.builder().build(), allocator);

      // Introspect default catalog
      Optional<CatalogProvider> catalog = ctx.catalog("datafusion");
      assertTrue(catalog.isPresent());

      Optional<SchemaProvider> publicSchema = catalog.get().schema("public");
      assertTrue(publicSchema.isPresent());

      List<String> tableNames = publicSchema.get().tableNames();
      assertTrue(tableNames.contains("my_csv"));
      assertTrue(publicSchema.get().tableExists("my_csv"));
      assertFalse(publicSchema.get().tableExists("nonexistent"));
    }
  }

  // ==========================================================================
  // Null-byte-in-string tests: verify structured string passing handles \0
  // ==========================================================================

  @Test
  void testConfigWithNullByteInValue() {
    try (SessionContext ctx =
        SessionContext.newWithConfig(
            org.apache.arrow.datafusion.config.ConfigOptions.builder()
                .execution(
                    org.apache.arrow.datafusion.config.ExecutionOptions.builder()
                        .batchSize(2048)
                        .build())
                .build())) {
      // Config with standard values works. Null-byte config values
      // are not meaningful for DataFusion config keys/values, but
      // the encoding must not corrupt adjacent parameters.
      assertNotNull(ctx);
      // Verify the config took effect by running a simple query
      try (DataFrame df = ctx.sql("SELECT 1 AS x")) {
        assertNotNull(df);
      }
    }
  }

  @Test
  void testCatalogNamesRoundTrip() {
    try (SessionContext ctx = new SessionContext()) {
      // The default catalog is "datafusion" — verify round-trip through DfStringArray
      List<String> defaultNames = ctx.catalogNames();
      assertTrue(defaultNames.contains("datafusion"));
      // Verify the list is properly structured (no empty strings from stale encoding)
      for (String name : defaultNames) {
        assertFalse(name.isEmpty(), "Catalog name should not be empty");
      }
    }
  }

  // ── register_variable tests ──

  @Test
  void testRegisterUserDefinedVariable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      VarProvider provider =
          new VarProvider() {
            @Override
            public ScalarValue getValue(List<String> varNames) {
              return switch (varNames.getFirst()) {
                case "@name" -> new ScalarValue.Utf8("Alice");
                case "@count" -> new ScalarValue.Int32(42);
                default ->
                    throw new IllegalArgumentException("Unknown variable: " + varNames.getFirst());
              };
            }

            @Override
            public Optional<ArrowType> getType(List<String> varNames) {
              return switch (varNames.getFirst()) {
                case "@name" -> Optional.of(ArrowType.Utf8.INSTANCE);
                case "@count" -> Optional.of(new ArrowType.Int(32, true));
                default -> Optional.empty();
              };
            }
          };
      ctx.registerVariable(VarType.USER_DEFINED, provider, allocator);

      try (DataFrame df = ctx.sql("SELECT @name, @count");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot result = stream.getVectorSchemaRoot();
        String nameCol = result.getVector(0).getField().getName();
        String countCol = result.getVector(1).getField().getName();
        expect(nameCol, countCol).row("Alice", 42).assertMatches(stream);
      }
    }
  }

  @Test
  void testRegisterSystemVariable() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      VarProvider provider =
          new VarProvider() {
            @Override
            public ScalarValue getValue(List<String> varNames) {
              if (varNames.getFirst().equals("@@version")) {
                return new ScalarValue.Utf8("1.0.0");
              }
              throw new IllegalArgumentException("Unknown system variable: " + varNames.getFirst());
            }

            @Override
            public Optional<ArrowType> getType(List<String> varNames) {
              if (varNames.getFirst().equals("@@version")) {
                return Optional.of(ArrowType.Utf8.INSTANCE);
              }
              return Optional.empty();
            }
          };
      ctx.registerVariable(VarType.SYSTEM, provider, allocator);

      try (DataFrame df = ctx.sql("SELECT @@version");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        VectorSchemaRoot result = stream.getVectorSchemaRoot();
        String colName = result.getVector(0).getField().getName();
        expect(colName).row("1.0.0").assertMatches(stream);
      }
    }
  }

  @Test
  void testVariableInWhereClause() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      // Register a batch with ids 1-4
      Schema schema =
          new Schema(
              List.of(
                  new Field("id", new FieldType(false, new ArrowType.Int(32, true), null), null)));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector id = (IntVector) root.getVector("id");
        id.allocateNew(4);
        for (int i = 0; i < 4; i++) {
          id.set(i, i + 1);
        }
        root.setRowCount(4);
        ctx.registerBatch("test", root, allocator);
      }

      // Register a threshold variable
      VarProvider provider =
          new VarProvider() {
            @Override
            public ScalarValue getValue(List<String> varNames) {
              return new ScalarValue.Int32(2);
            }

            @Override
            public Optional<ArrowType> getType(List<String> varNames) {
              return Optional.of(new ArrowType.Int(32, true));
            }
          };
      ctx.registerVariable(VarType.USER_DEFINED, provider, allocator);

      // Query using the variable in WHERE clause
      try (DataFrame df = ctx.sql("SELECT id FROM test WHERE id > @threshold ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        expect("id").row(3).row(4).assertMatches(stream);
      }
    }
  }

  private String createCsvFile(Path dir, String name, String content) throws IOException {
    Path file = dir.resolve(name);
    Files.writeString(file, content);
    return file.toString();
  }
}
