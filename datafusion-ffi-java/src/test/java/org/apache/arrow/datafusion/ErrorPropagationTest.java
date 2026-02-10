package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Tests for error propagation through the Rust FFI bridge.
 *
 * <p>These tests verify that exceptions thrown in Java callbacks are properly propagated through
 * the Rust FFI bridge back to the Java caller.
 *
 * <p>Note: Some callbacks don't propagate errors - they return empty/None instead:
 *
 * <ul>
 *   <li>CatalogProvider.schemaNames() returns empty list
 *   <li>CatalogProvider.schema() returns None
 *   <li>SchemaProvider.tableNames() returns empty list
 * </ul>
 *
 * <p>The following callbacks DO propagate errors and are tested here:
 *
 * <ul>
 *   <li>SchemaProvider.table()
 *   <li>TableProvider.schema()
 *   <li>TableProvider.scan()
 *   <li>ExecutionPlan.schema()
 *   <li>ExecutionPlan.execute()
 *   <li>RecordBatchReader.loadNextBatch()
 * </ul>
 *
 * <p>Full stack traces can be enabled via {@code SessionConfig.builder().fullStackTrace(true)} or
 * the FULL_JAVA_STACK_TRACE environment variable. This is useful for debugging callback errors.
 */
public class ErrorPropagationTest {

  private Schema createTestSchema() {
    return new Schema(
        Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
  }

  @Test
  void testSchemaProviderTable_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in SchemaProvider.table()";

      SchemaProvider errorSchema =
          new SchemaProvider() {
            @Override
            public List<String> tableNames() {
              return List.of("my_table");
            }

            @Override
            public Optional<TableProvider> table(String name) {
              throw new RuntimeException(errorMessage);
            }
          };

      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", errorSchema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.my_table")) {
                  // Should not reach here
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testTableProviderSchema_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in TableProvider.schema()";

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              throw new RuntimeException(errorMessage);
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              throw new UnsupportedOperationException("Should not be called");
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table")) {
                  // Should not reach here
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testTableProviderScan_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in TableProvider.scan()";
      Schema testSchema = createTestSchema();

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              throw new RuntimeException(errorMessage);
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table");
                    RecordBatchStream stream = df.executeStream(allocator)) {
                  stream.loadNextBatch();
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testExecutionPlanSchema_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in ExecutionPlan.schema()";
      Schema testSchema = createTestSchema();

      ExecutionPlan errorPlan =
          new ExecutionPlan() {
            @Override
            public Schema schema() {
              throw new RuntimeException(errorMessage);
            }

            @Override
            public RecordBatchReader execute(int partition, BufferAllocator allocator) {
              throw new UnsupportedOperationException("Should not be called");
            }
          };

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              return errorPlan;
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table");
                    RecordBatchStream stream = df.executeStream(allocator)) {
                  stream.loadNextBatch();
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testExecutionPlanExecute_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in ExecutionPlan.execute()";
      Schema testSchema = createTestSchema();

      ExecutionPlan errorPlan =
          new ExecutionPlan() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public RecordBatchReader execute(int partition, BufferAllocator allocator) {
              throw new RuntimeException(errorMessage);
            }
          };

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              return errorPlan;
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table");
                    RecordBatchStream stream = df.executeStream(allocator)) {
                  stream.loadNextBatch();
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testRecordBatchReaderLoadNextBatch_errorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Test error in RecordBatchReader.loadNextBatch()";
      Schema testSchema = createTestSchema();

      ExecutionPlan errorPlan =
          new ExecutionPlan() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public RecordBatchReader execute(int partition, BufferAllocator alloc) {
              return new RecordBatchReader() {
                private final VectorSchemaRoot root = VectorSchemaRoot.create(testSchema, alloc);

                @Override
                public VectorSchemaRoot getVectorSchemaRoot() {
                  return root;
                }

                @Override
                public boolean loadNextBatch() {
                  throw new RuntimeException(errorMessage);
                }

                @Override
                public void close() {
                  root.close();
                }
              };
            }
          };

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              return testSchema;
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              return errorPlan;
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table");
                    RecordBatchStream stream = df.executeStream(allocator)) {
                  stream.loadNextBatch();
                }
              });

      assertTrue(
          exception.getMessage().contains(errorMessage),
          "Exception should contain original error message. Got: " + exception.getMessage());
    }
  }

  @Test
  void testStackTraceIncludedWhenConfigured() {
    // This test verifies full stack traces are included when configured via SessionConfig
    SessionConfig config = SessionConfig.builder().fullStackTrace(true).build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {

      String errorMessage = "Stack trace test error";

      // Use a custom exception to make the stack trace identifiable
      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              throw new IllegalStateException(errorMessage);
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              throw new UnsupportedOperationException("Should not be called");
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table")) {
                  // Should not reach here
                }
              });

      String exceptionMsg = exception.getMessage();

      // Verify the original error message is present
      assertTrue(
          exceptionMsg.contains(errorMessage),
          "Exception should contain original error message. Got: " + exceptionMsg);

      // Verify stack trace elements are present (without being too specific about line numbers)
      assertTrue(
          exceptionMsg.contains("IllegalStateException"),
          "Exception should contain exception class name. Got: " + exceptionMsg);

      assertTrue(
          exceptionMsg.contains("at "),
          "Exception should contain stack trace 'at' lines. Got: " + exceptionMsg);

      assertTrue(
          exceptionMsg.contains("ErrorPropagationTest"),
          "Exception should contain test class in stack trace. Got: " + exceptionMsg);
    }
  }

  @Test
  void testStackTraceIncludesCause() {
    // This test verifies that nested exception causes are included in full stack traces
    SessionConfig config = SessionConfig.builder().fullStackTrace(true).build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(config)) {

      String rootCause = "Root cause of the problem";
      String wrapperMessage = "Wrapper exception";

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              Exception cause = new IllegalArgumentException(rootCause);
              throw new RuntimeException(wrapperMessage, cause);
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              throw new UnsupportedOperationException("Should not be called");
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table")) {
                  // Should not reach here
                }
              });

      String exceptionMsg = exception.getMessage();

      // Verify both the wrapper message and root cause are present
      assertTrue(
          exceptionMsg.contains(wrapperMessage),
          "Exception should contain wrapper message. Got: " + exceptionMsg);

      assertTrue(
          exceptionMsg.contains(rootCause),
          "Exception should contain root cause message. Got: " + exceptionMsg);

      assertTrue(
          exceptionMsg.contains("Caused by:"),
          "Exception should contain 'Caused by:' for nested exception. Got: " + exceptionMsg);

      assertTrue(
          exceptionMsg.contains("IllegalArgumentException"),
          "Exception should contain cause exception class. Got: " + exceptionMsg);
    }
  }

  @Test
  void testNoStackTraceByDefault() {
    // This test verifies that stack traces are NOT included by default (only the message)
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      String errorMessage = "Default config error message";

      TableProvider errorTable =
          new TableProvider() {
            @Override
            public Schema schema() {
              throw new IllegalStateException(errorMessage);
            }

            @Override
            public ExecutionPlan scan(
                Session session, Expr[] filters, int[] projection, Long limit) {
              throw new UnsupportedOperationException("Should not be called");
            }
          };

      SchemaProvider schema = new SimpleSchemaProvider(Map.of("error_table", errorTable));
      CatalogProvider catalog = new SimpleCatalogProvider(Map.of("my_schema", schema));
      ctx.registerCatalog("test_catalog", catalog, allocator);

      Exception exception =
          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT * FROM test_catalog.my_schema.error_table")) {
                  // Should not reach here
                }
              });

      String exceptionMsg = exception.getMessage();

      // Verify the error message is present
      assertTrue(
          exceptionMsg.contains(errorMessage),
          "Exception should contain error message. Got: " + exceptionMsg);

      // Verify stack trace elements are NOT present (unless env var is set)
      if (System.getenv("FULL_JAVA_STACK_TRACE") == null
          || System.getenv("FULL_JAVA_STACK_TRACE").isEmpty()) {
        assertFalse(
            exceptionMsg.contains("at org.apache.arrow"),
            "Exception should NOT contain stack trace by default. Got: " + exceptionMsg);
      }
    }
  }

  // Simple helper implementations

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

    @Override
    public Optional<TableProvider> registerTable(String name, TableProvider table) {
      return Optional.ofNullable(tables.put(name, table));
    }

    @Override
    public Optional<TableProvider> deregisterTable(String name) {
      return Optional.ofNullable(tables.remove(name));
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
}
