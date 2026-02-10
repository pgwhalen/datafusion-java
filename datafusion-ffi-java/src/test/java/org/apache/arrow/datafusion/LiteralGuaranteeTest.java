package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for LiteralGuarantee analysis through the full FFI pipeline. */
public class LiteralGuaranteeTest {

  @Test
  void testEqualityFilter() {
    Schema schema = createTestSchema();
    AtomicReference<List<LiteralGuarantee>> capturedGuarantees = new AtomicReference<>();

    TableProvider table = createCapturingTableProvider(schema, capturedGuarantees);
    executeQuery(
        schema, table, "SELECT * FROM test_catalog.test_schema.test_table WHERE name = 'A'");

    List<LiteralGuarantee> guarantees = capturedGuarantees.get();
    assertNotNull(guarantees, "Guarantees should have been captured");
    assertFalse(guarantees.isEmpty(), "Should have at least one guarantee");

    LiteralGuarantee g = guarantees.get(0);
    assertEquals(Guarantee.IN, g.guarantee());
    assertEquals("name", g.column().name());
    assertTrue(
        g.literals().contains(new ScalarValue.Utf8("A")),
        "Literals should contain Utf8('A'), got: " + g.literals());
  }

  @Test
  void testInListFilter() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    AtomicReference<List<LiteralGuarantee>> capturedGuarantees = new AtomicReference<>();

    TableProvider table = createCapturingTableProvider(schema, capturedGuarantees);
    executeQuery(
        schema, table, "SELECT * FROM test_catalog.test_schema.test_table WHERE id IN (1, 2, 3)");

    List<LiteralGuarantee> guarantees = capturedGuarantees.get();
    assertNotNull(guarantees);
    assertFalse(guarantees.isEmpty(), "Should have at least one guarantee for IN list");

    LiteralGuarantee g = guarantees.get(0);
    assertEquals(Guarantee.IN, g.guarantee());
    assertEquals("id", g.column().name());
    assertEquals(3, g.literals().size(), "Should have 3 literals");

    // The literals should be Int32 values
    Set<Class<?>> types = g.literals().stream().map(Object::getClass).collect(Collectors.toSet());
    assertEquals(Set.of(ScalarValue.Int32.class), types, "All literals should be Int32");
  }

  @Test
  void testNotEqualFilter() {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
    AtomicReference<List<LiteralGuarantee>> capturedGuarantees = new AtomicReference<>();

    TableProvider table = createCapturingTableProvider(schema, capturedGuarantees);
    executeQuery(schema, table, "SELECT * FROM test_catalog.test_schema.test_table WHERE id != 5");

    List<LiteralGuarantee> guarantees = capturedGuarantees.get();
    assertNotNull(guarantees);
    assertFalse(guarantees.isEmpty(), "Should have at least one guarantee for !=");

    LiteralGuarantee g = guarantees.get(0);
    assertEquals(Guarantee.NOT_IN, g.guarantee());
    assertEquals("id", g.column().name());
    assertEquals(1, g.literals().size());
  }

  @Test
  void testNoWhereClause() {
    Schema schema = createTestSchema();
    AtomicReference<List<LiteralGuarantee>> capturedGuarantees = new AtomicReference<>();

    // This provider captures scan parameters but handles no-filter case
    TableProvider table =
        new TableProvider() {
          @Override
          public Schema schema() {
            return schema;
          }

          @Override
          public ExecutionPlan scan(Session session, Expr[] filters, int[] projection, Long limit) {
            if (filters.length > 0) {
              try (PhysicalExpr physExpr = session.createPhysicalExpr(schema, filters)) {
                capturedGuarantees.set(LiteralGuarantee.analyze(physExpr));
              }
            } else {
              capturedGuarantees.set(List.of());
            }
            return new TestExecutionPlan(schema);
          }
        };

    executeQuery(schema, table, "SELECT * FROM test_catalog.test_schema.test_table");

    List<LiteralGuarantee> guarantees = capturedGuarantees.get();
    assertNotNull(guarantees);
    assertTrue(guarantees.isEmpty(), "No WHERE clause should produce empty guarantees");
  }

  @Test
  void testFiltersPassedToScan() {
    Schema schema = createTestSchema();
    AtomicReference<Integer> capturedExprArrayLength = new AtomicReference<>();

    TableProvider table =
        new TableProvider() {
          @Override
          public Schema schema() {
            return schema;
          }

          @Override
          public ExecutionPlan scan(Session session, Expr[] filters, int[] projection, Long limit) {
            capturedExprArrayLength.set(filters.length);
            return new TestExecutionPlan(schema);
          }
        };

    executeQuery(
        schema, table, "SELECT * FROM test_catalog.test_schema.test_table WHERE name = 'A'");

    assertNotNull(capturedExprArrayLength.get());
    assertTrue(capturedExprArrayLength.get() > 0, "Filter count should be positive");
  }

  // -- Helpers --

  private Schema createTestSchema() {
    return new Schema(
        Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
  }

  private TableProvider createCapturingTableProvider(
      Schema schema, AtomicReference<List<LiteralGuarantee>> capturedGuarantees) {
    return new TableProvider() {
      @Override
      public Schema schema() {
        return schema;
      }

      @Override
      public ExecutionPlan scan(Session session, Expr[] filters, int[] projection, Long limit) {
        if (filters.length > 0) {
          try (PhysicalExpr physExpr = session.createPhysicalExpr(schema, filters)) {
            capturedGuarantees.set(LiteralGuarantee.analyze(physExpr));
          }
        } else {
          capturedGuarantees.set(List.of());
        }
        return new TestExecutionPlan(schema);
      }
    };
  }

  private void executeQuery(Schema schema, TableProvider table, String sql) {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      SchemaProvider schemaProvider = new SimpleSchemaProvider(Map.of("test_table", table));
      CatalogProvider catalogProvider =
          new SimpleCatalogProvider(Map.of("test_schema", schemaProvider));
      ctx.registerCatalog("test_catalog", catalogProvider, allocator);

      try (DataFrame df = ctx.sql(sql);
          RecordBatchStream stream = df.executeStream(allocator)) {
        // Consume the stream to trigger scan
        while (stream.loadNextBatch()) {
          // consume
        }
      }
    }
  }

  /** Simple execution plan that returns empty batches. */
  static class TestExecutionPlan implements ExecutionPlan {
    private final Schema schema;

    TestExecutionPlan(Schema schema) {
      this.schema = schema;
    }

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public RecordBatchReader execute(int partition, BufferAllocator allocator) {
      return new TestRecordBatchReader(schema, allocator);
    }
  }

  /** Simple reader that returns one empty batch. */
  static class TestRecordBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private boolean done = false;

    TestRecordBatchReader(Schema schema, BufferAllocator allocator) {
      this.root = VectorSchemaRoot.create(schema, allocator);
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public boolean loadNextBatch() {
      if (done) {
        return false;
      }
      done = true;
      root.setRowCount(0);
      return true;
    }

    @Override
    public void close() {
      root.close();
    }
  }

  /** A simple SchemaProvider backed by a map. */
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

  /** A simple CatalogProvider backed by a map. */
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
