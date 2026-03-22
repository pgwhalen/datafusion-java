package org.apache.arrow.datafusion.graaltest;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.execution.SessionState;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * GraalVM native image compatibility test for datafusion-ffi-java.
 *
 * <p>Runs 9 test scenarios covering the major FFI paths: downcalls (Java->Rust), upcalls
 * (Rust->Java callbacks), Arrow zero-copy streaming, and custom provider registration. Tests that
 * require Arrow C Data JNI or FFM upcalls are skipped in native image mode where those features
 * are unsupported.
 *
 * <p>Exit code 0 = all tests pass, 1 = any test failed.
 */
public class GraalNativeImageTest {

  private int passed = 0;
  private int failed = 0;

  public static void main(String[] args) {
    GraalNativeImageTest test = new GraalNativeImageTest();
    test.runAll();
    System.out.println();
    System.out.println(
        "Results: " + test.passed + " passed, " + test.failed + " failed, "
            + (test.passed + test.failed) + " total");
    System.exit(test.failed > 0 ? 1 : 0);
  }

  private void runAll() {
    // Test 1: Always runs — pure FFM downcalls, no Arrow JNI
    runTest("SessionContext creation", this::testSessionContextCreation);

    // Tests 2, 3, 5 use Arrow C Data JNI — now works in native image with arrow-java 19.0.0-SNAPSHOT
    // which fixes FindClass class name format (java/lang/Object instead of Ljava/lang/Object;).
    runTest("Simple SQL query", this::testSimpleSqlQuery);
    runTest("Register table and query", this::testRegisterTableAndQuery);

    runTest("Custom TableProvider via catalog", this::testCustomTableProviderViaCatalog);

    runTest("Multi-batch streaming", this::testMultiBatchStreaming);

    // Native-image-friendly tests: exercise FFM downcalls without Arrow C Data JNI
    runTest("SQL to DataFrame", this::testSqlToDataFrame);
    runTest("Session metadata", this::testSessionMetadata);
    runTest("SessionContext with config", this::testSessionContextWithConfig);
    runTest("Multiple independent contexts", this::testMultipleContexts);
  }

  private void runTest(String name, Runnable test) {
    System.out.println("  " + name + " ...");
    try {
      test.run();
      System.out.println("  " + name + " ... PASS");
      passed++;
    } catch (Throwable t) {
      System.out.println("  " + name + " ... FAIL");
      t.printStackTrace(System.out);
      failed++;
    }
  }

  // ── Test 1: SessionContext creation ───────────────────────────────────────

  private void testSessionContextCreation() {
    try (SessionContext ctx = new SessionContext()) {
      String id = ctx.sessionId();
      assertNotNull(id, "sessionId");
      assertFalse(id.isEmpty(), "sessionId should not be empty");
    }
  }

  // ── Test 2: Simple SQL query ─────────────────────────────────────────────

  private void testSimpleSqlQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 + 1 AS result");
        SendableRecordBatchStream stream = df.executeStream(allocator)) {

      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      assertTrue(stream.loadNextBatch(), "should have at least one batch");

      assertEquals(1, root.getRowCount(), "row count");
      BigIntVector resultVec = (BigIntVector) root.getVector("result");
      assertEquals(2L, resultVec.get(0), "1+1 should equal 2");

      assertFalse(stream.loadNextBatch(), "should have no more batches");
    }
  }

  // ── Test 3: Register table and query ─────────────────────────────────────

  private void testRegisterTableAndQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = new Schema(Arrays.asList(
          new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
          new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        BigIntVector idVec = (BigIntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");

        idVec.allocateNew(3);
        nameVec.allocateNew(3);
        idVec.set(0, 10);
        nameVec.setSafe(0, "Alice".getBytes());
        idVec.set(1, 20);
        nameVec.setSafe(1, "Bob".getBytes());
        idVec.set(2, 30);
        nameVec.setSafe(2, "Charlie".getBytes());
        idVec.setValueCount(3);
        nameVec.setValueCount(3);
        root.setRowCount(3);

        ctx.registerBatch("people", root, allocator);

        try (DataFrame df = ctx.sql("SELECT id, name FROM people WHERE id >= 20 ORDER BY id");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {

          VectorSchemaRoot result = stream.getVectorSchemaRoot();
          assertTrue(stream.loadNextBatch(), "should have results");
          assertEquals(2, result.getRowCount(), "filtered row count");

          BigIntVector ids = (BigIntVector) result.getVector("id");
          assertEquals(20L, ids.get(0), "first id");
          assertEquals(30L, ids.get(1), "second id");

          assertFalse(stream.loadNextBatch(), "no more batches");
        }
      }
    }
  }

  // ── Test 4: Custom TableProvider via catalog ─────────────────────────────

  private void testCustomTableProviderViaCatalog() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      Schema schema = new Schema(Arrays.asList(
          new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
          new Field("value", FieldType.nullable(new ArrowType.Utf8()), null)));

      TableProvider table = new SimpleTableProvider(schema);
      SchemaProvider schemaProvider = new SimpleSchemaProvider(Map.of("test_table", table));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("test_schema", schemaProvider));

      ctx.registerCatalog("graal_catalog", catalog, allocator);

      try (DataFrame df =
              ctx.sql("SELECT * FROM graal_catalog.test_schema.test_table ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch(), "should have results from custom provider");
        assertEquals(3, root.getRowCount(), "custom provider row count");

        BigIntVector idVec = (BigIntVector) root.getVector("id");
        assertEquals(1L, idVec.get(0), "row 0 id");
        assertEquals(2L, idVec.get(1), "row 1 id");
        assertEquals(3L, idVec.get(2), "row 2 id");

        assertFalse(stream.loadNextBatch(), "no more batches");
      }
    }
  }

  // ── Test 5: Multi-batch streaming ────────────────────────────────────────

  private void testMultiBatchStreaming() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Use VALUES to generate enough rows that DataFusion may produce multiple batches
      StringBuilder sql = new StringBuilder("SELECT * FROM (VALUES ");
      for (int i = 0; i < 100; i++) {
        if (i > 0) sql.append(", ");
        sql.append("(").append(i).append(")");
      }
      sql.append(") AS t(num)");

      try (DataFrame df = ctx.sql(sql.toString());
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        int totalRows = 0;
        while (stream.loadNextBatch()) {
          totalRows += root.getRowCount();
        }
        assertEquals(100, totalRows, "total rows across all batches");
      }
    }
  }

  // ── Test 6: SQL to DataFrame ────────────────────────────────────────────

  private void testSqlToDataFrame() {
    try (SessionContext ctx = new SessionContext();
        DataFrame df = ctx.sql("SELECT 1 + 1 AS result")) {
      assertNotNull(df, "DataFrame from SQL");
    }
  }

  // ── Test 7: Session metadata ──────────────────────────────────────────────

  private void testSessionMetadata() {
    try (SessionContext ctx = new SessionContext()) {
      String id = ctx.sessionId();
      assertNotNull(id, "sessionId");
      assertFalse(id.isEmpty(), "sessionId should not be empty");

      Instant startTime = ctx.sessionStartTime();
      assertNotNull(startTime, "sessionStartTime");
      assertTrue(startTime.toEpochMilli() > 0, "start time should be positive");

      try (SessionState state = ctx.state()) {
        assertNotNull(state, "session state");
      }
    }
  }

  // ── Test 8: SessionContext with config ─────────────────────────────────────

  private void testSessionContextWithConfig() {
    ConfigOptions config = ConfigOptions.builder()
        .fullStackTrace(true)
        .build();
    try (SessionContext ctx = new SessionContext(config)) {
      String id = ctx.sessionId();
      assertNotNull(id, "sessionId with config");
      assertFalse(id.isEmpty(), "sessionId should not be empty");
    }
  }

  // ── Test 9: Multiple independent contexts ──────────────────────────────────

  private void testMultipleContexts() {
    try (SessionContext ctx1 = new SessionContext();
        SessionContext ctx2 = new SessionContext()) {
      String id1 = ctx1.sessionId();
      String id2 = ctx2.sessionId();
      assertNotNull(id1, "ctx1 sessionId");
      assertNotNull(id2, "ctx2 sessionId");
      assertFalse(id1.equals(id2), "session IDs should be unique");

      try (DataFrame df1 = ctx1.sql("SELECT 42 AS answer");
          DataFrame df2 = ctx2.sql("SELECT 'hello' AS greeting")) {
        assertNotNull(df1, "ctx1 DataFrame");
        assertNotNull(df2, "ctx2 DataFrame");
      }
    }
  }

  // ── Custom provider implementations ──────────────────────────────────────

  static class SimpleTableProvider implements TableProvider {
    private final Schema schema;

    SimpleTableProvider(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public ExecutionPlan scan(
        Session session, List<Expr> filters, List<Integer> projection, Long limit) {
      return new SimpleExecutionPlan(schema);
    }
  }

  static class SimpleExecutionPlan implements ExecutionPlan {
    private final Schema schema;

    SimpleExecutionPlan(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public RecordBatchReader execute(int partition, BufferAllocator allocator) {
      return new SimpleRecordBatchReader(schema, allocator);
    }
  }

  static class SimpleRecordBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private boolean consumed = false;

    SimpleRecordBatchReader(Schema schema, BufferAllocator allocator) {
      this.root = VectorSchemaRoot.create(schema, allocator);
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

      BigIntVector idVec = (BigIntVector) root.getVector("id");
      VarCharVector valueVec = (VarCharVector) root.getVector("value");

      idVec.allocateNew(3);
      valueVec.allocateNew(3);

      idVec.set(0, 1);
      valueVec.setSafe(0, "one".getBytes());
      idVec.set(1, 2);
      valueVec.setSafe(1, "two".getBytes());
      idVec.set(2, 3);
      valueVec.setSafe(2, "three".getBytes());

      idVec.setValueCount(3);
      valueVec.setValueCount(3);
      root.setRowCount(3);
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

  // ── Assertion helpers ────────────────────────────────────────────────────

  private static void assertTrue(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError("Expected true: " + message);
    }
    System.out.println("    OK: " + message);
  }

  private static void assertFalse(boolean condition, String message) {
    if (condition) {
      throw new AssertionError("Expected false: " + message);
    }
    System.out.println("    OK: " + message);
  }

  private static void assertEquals(long expected, long actual, String message) {
    if (expected != actual) {
      throw new AssertionError(message + ": expected " + expected + " but got " + actual);
    }
    System.out.println("    OK: " + message + " = " + actual);
  }

  private static void assertNotNull(Object obj, String message) {
    if (obj == null) {
      throw new AssertionError("Expected non-null: " + message);
    }
    System.out.println("    OK: " + message + " = " + obj);
  }
}
