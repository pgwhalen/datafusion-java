package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Tests for RuntimeEnv and memory pool configuration. */
public class MemoryPoolTest {

  @Test
  void testMemoryLimitedContextExecutesSimpleQuery() {
    try (RuntimeEnv rt = RuntimeEnvBuilder.builder().withMemoryLimit(50_000_000, 1.0).build();
        SessionContext ctx = SessionContext.newWithConfigRt(SessionConfig.defaults(), rt)) {
      try (BufferAllocator allocator = new RootAllocator();
          DataFrame df = ctx.sql("SELECT 1 + 1 AS result");
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch(), "Expected at least one batch");
        assertEquals(2L, stream.getVectorSchemaRoot().getVector("result").getObject(0));
      }
    }
  }

  @Test
  void testMemoryLimitExceeded() {
    try (RuntimeEnv rt = RuntimeEnvBuilder.builder().withMemoryLimit(1024, 1.0).build();
        SessionContext ctx = SessionContext.newWithConfigRt(SessionConfig.defaults(), rt)) {
      try (BufferAllocator allocator = new RootAllocator()) {
        // A query that generates enough data to exceed 1KB memory limit
        RuntimeException ex =
            assertThrows(
                RuntimeException.class,
                () -> {
                  try (DataFrame df =
                          ctx.sql(
                              "SELECT * FROM generate_series(1, 10000) AS t(a) "
                                  + "CROSS JOIN generate_series(1, 100) AS t2(b)");
                      RecordBatchStream stream = df.executeStream(allocator)) {
                    while (stream.loadNextBatch()) {
                      // consume all batches to trigger memory allocation
                    }
                  }
                });
        String msg = ex.getMessage();
        assertTrue(
            msg.contains("memory") || msg.contains("Memory"),
            "Expected memory-related error, got: " + msg);
      }
    }
  }

  @Test
  void testDefaultRuntimeEnvHasNoLimit() {
    try (RuntimeEnv rt = RuntimeEnvBuilder.builder().build();
        SessionContext ctx = SessionContext.newWithConfigRt(SessionConfig.defaults(), rt)) {
      try (BufferAllocator allocator = new RootAllocator();
          DataFrame df = ctx.sql("SELECT 42 AS answer");
          RecordBatchStream stream = df.executeStream(allocator)) {
        assertTrue(stream.loadNextBatch(), "Expected at least one batch");
        assertEquals(42L, stream.getVectorSchemaRoot().getVector("answer").getObject(0));
      }
    }
  }

  @Test
  void testRuntimeEnvBuilderValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeEnvBuilder.builder().withMemoryLimit(-1, 1.0),
        "Should reject negative maxMemory");

    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeEnvBuilder.builder().withMemoryLimit(0, 1.0),
        "Should reject zero maxMemory");

    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeEnvBuilder.builder().withMemoryLimit(1024, 0.0),
        "Should reject zero memoryFraction");

    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeEnvBuilder.builder().withMemoryLimit(1024, 1.5),
        "Should reject memoryFraction > 1.0");

    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeEnvBuilder.builder().withMemoryLimit(1024, -0.5),
        "Should reject negative memoryFraction");
  }

  @Test
  void testStaticFactoryMethods() {
    // Test SessionContext.create()
    try (SessionContext ctx = SessionContext.create()) {
      assertNotNull(ctx.sessionId());
    }

    // Test SessionContext.newWithConfig()
    try (SessionContext ctx = SessionContext.newWithConfig(SessionConfig.defaults())) {
      assertNotNull(ctx.sessionId());
    }
  }
}
