package org.apache.arrow.datafusion.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.ScanArgs;
import org.apache.arrow.datafusion.catalog.SchemaProvider;
import org.apache.arrow.datafusion.catalog.Session;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.config.ExecutionOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.physical_plan.ExecutionPlan;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Tests that exercise every public method on {@link TaskContext}, {@link SessionConfig}, {@link
 * MemoryPool}, {@link ScalarFunctionHandle} and {@link AggregateFunctionHandle}. Works by
 * registering a TableProvider whose ExecutionPlan captures the TaskContext handed to it during
 * execute(), then verifying the captured values after the query completes.
 */
public class TaskContextTest {

  private static final Schema SINGLE_INT_SCHEMA =
      new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));

  @Test
  void taskContextExposesSessionAndRuntimeFields() throws Exception {
    AtomicReference<Captured> captured = new AtomicReference<>();
    ConfigOptions cfg =
        ConfigOptions.builder()
            .execution(ExecutionOptions.builder().batchSize(1234).build())
            .build();

    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext(cfg)) {
      registerCapturingCatalog(ctx, captured, allocator);

      try (DataFrame df = ctx.sql("SELECT * FROM cap.def.t");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch(), "reader must yield at least one batch");
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        assertEquals(42L, ((BigIntVector) root.getVector("id")).get(0));
      }
    }

    Captured c = captured.get();
    assertNotNull(c, "execute() should have populated the captured TaskContext snapshot");

    // sessionId
    assertNotNull(c.sessionId);
    assertFalse(c.sessionId.isEmpty(), "session id should be non-empty");

    // taskId — DataFusion's default TaskContext::from(SessionState) leaves task_id as None.
    assertEquals(Optional.empty(), c.taskId);

    // sessionConfig.options() — the batch_size we set should round-trip.
    assertEquals("1234", c.sessionConfigOptions.get("datafusion.execution.batch_size"));

    // scalarFunctions / aggregateFunctions — DataFusion registers builtins by default.
    assertTrue(
        c.scalarNames.contains("abs"), "expected 'abs' in scalar fns; got: " + c.scalarNames);
    assertTrue(
        c.aggregateNames.contains("sum"),
        "expected 'sum' in aggregate fns; got: " + c.aggregateNames);

    // Placeholder handle values
    assertEquals(new ScalarFunctionHandle(), c.scalarHandleSample);
    assertEquals(new AggregateFunctionHandle(), c.aggregateHandleSample);

    // memoryPool — reserved() is >= 0 (unsigned, mapped to long).
    assertTrue(c.memoryPoolReserved >= 0, "reserved() should be non-negative");

    // runtimeEnv — we got a live, non-null handle.
    assertTrue(c.runtimeEnvObserved, "runtimeEnv() should return a non-null handle");

    // withSessionConfig override
    assertEquals("9999", c.withSessionConfigBatchSize);

    // withRuntime produced a new TaskContext we could read session_id from.
    assertEquals(
        c.sessionId, c.withRuntimeSessionId, "withRuntime() preserves session_id from source");
  }

  @Test
  void sessionConfigFromConfigOptionsRoundTrips() {
    ConfigOptions cfg =
        ConfigOptions.fromStringMap(Map.of("datafusion.execution.batch_size", "2048"));
    try (SessionConfig sc = SessionConfig.fromConfigOptions(cfg)) {
      assertEquals("2048", sc.options().get("datafusion.execution.batch_size"));
    }
  }

  private static void registerCapturingCatalog(
      SessionContext ctx, AtomicReference<Captured> out, BufferAllocator allocator) {
    TableProvider table =
        new TableProvider() {
          @Override
          public Schema schema() {
            return SINGLE_INT_SCHEMA;
          }

          @Override
          public ExecutionPlan scanWithArgs(Session session, ScanArgs args) {
            return new CapturingPlan(out);
          }
        };
    SchemaProvider schema = new MapSchemaProvider(Map.of("t", table));
    CatalogProvider catalog = new MapCatalogProvider(Map.of("def", schema));
    ctx.registerCatalog("cap", catalog, allocator);
  }

  /** ExecutionPlan that captures every field of the TaskContext into a snapshot. */
  private static final class CapturingPlan implements ExecutionPlan {
    private final AtomicReference<Captured> out;

    CapturingPlan(AtomicReference<Captured> out) {
      this.out = out;
    }

    @Override
    public Schema schema() {
      return SINGLE_INT_SCHEMA;
    }

    @Override
    public RecordBatchReader execute(
        int partition, TaskContext taskContext, BufferAllocator allocator) {
      Captured c = new Captured();
      c.sessionId = taskContext.sessionId();
      c.taskId = taskContext.taskId();

      try (SessionConfig sc = taskContext.sessionConfig()) {
        c.sessionConfigOptions = sc.options();
      }

      try (RuntimeEnv rt = taskContext.runtimeEnv()) {
        c.runtimeEnvObserved = rt != null;
      }

      try (MemoryPool pool = taskContext.memoryPool()) {
        c.memoryPoolReserved = pool.reserved();
      }

      Map<String, ScalarFunctionHandle> scalars = taskContext.scalarFunctions();
      c.scalarNames = new java.util.HashSet<>(scalars.keySet());
      if (!scalars.isEmpty()) {
        c.scalarHandleSample = scalars.values().iterator().next();
      }

      Map<String, AggregateFunctionHandle> aggregates = taskContext.aggregateFunctions();
      c.aggregateNames = new java.util.HashSet<>(aggregates.keySet());
      if (!aggregates.isEmpty()) {
        c.aggregateHandleSample = aggregates.values().iterator().next();
      }

      SessionConfig overriddenCfg =
          SessionConfig.fromConfigOptions(
              ConfigOptions.fromStringMap(Map.of("datafusion.execution.batch_size", "9999")));
      try (overriddenCfg;
          TaskContext overridden = taskContext.withSessionConfig(overriddenCfg);
          SessionConfig sc2 = overridden.sessionConfig()) {
        c.withSessionConfigBatchSize = sc2.options().get("datafusion.execution.batch_size");
      }

      RuntimeEnv newRt = RuntimeEnvBuilder.builder().build();
      try (newRt;
          TaskContext withRt = taskContext.withRuntime(newRt)) {
        c.withRuntimeSessionId = withRt.sessionId();
      }

      out.set(c);
      return new SingleIntBatchReader(allocator);
    }
  }

  private static final class Captured {
    String sessionId;
    Optional<String> taskId;
    Map<String, String> sessionConfigOptions;
    java.util.Set<String> scalarNames;
    java.util.Set<String> aggregateNames;
    ScalarFunctionHandle scalarHandleSample;
    AggregateFunctionHandle aggregateHandleSample;
    long memoryPoolReserved;
    boolean runtimeEnvObserved;
    String withSessionConfigBatchSize;
    String withRuntimeSessionId;
  }

  private static final class SingleIntBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private boolean produced = false;

    SingleIntBatchReader(BufferAllocator allocator) {
      this.root = VectorSchemaRoot.create(SINGLE_INT_SCHEMA, allocator);
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public boolean loadNextBatch() {
      if (produced) {
        return false;
      }
      produced = true;
      BigIntVector v = (BigIntVector) root.getVector("id");
      v.allocateNew(1);
      v.set(0, 42L);
      v.setValueCount(1);
      root.setRowCount(1);
      return true;
    }

    @Override
    public void close() {
      root.close();
    }
  }

  private static final class MapSchemaProvider implements SchemaProvider {
    private final Map<String, TableProvider> tables;

    MapSchemaProvider(Map<String, TableProvider> tables) {
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

  private static final class MapCatalogProvider implements CatalogProvider {
    private final Map<String, SchemaProvider> schemas;

    MapCatalogProvider(Map<String, SchemaProvider> schemas) {
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
