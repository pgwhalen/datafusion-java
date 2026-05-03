package org.apache.arrow.datafusion.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.datafusion.config.CatalogOptions;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.config.ExecutionOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests for the federated {@link SessionContext#newWithFederation(ConfigOptions)} entry
 * point. Cross-remote pushdown is exercised end-to-end in {@code FlightSqlFederationTest} once
 * the Flight SQL test server lands; here we only verify that a federation-enabled context still
 * runs ordinary SQL, so the federation optimizer/planner don't break non-federated queries.
 */
class SessionContextFederationTest {

  @Test
  void federatedContextRunsOrdinarySql() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
      assertNotNull(ctx);
      try (DataFrame df = ctx.sql("SELECT 1 + 1 AS result");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        stream.loadNextBatch();
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        BigIntVector col = (BigIntVector) root.getVector("result");
        assertEquals(2L, col.get(0));
      }
    }
  }

  @Test
  void federatedContextAcceptsConfigOverrides() {
    ConfigOptions config =
        ConfigOptions.builder()
            .catalog(CatalogOptions.builder().informationSchema(true).build())
            .execution(ExecutionOptions.builder().batchSize(512).build())
            .build();
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = SessionContext.newWithFederation(config)) {
      assertNotNull(ctx);
      try (DataFrame df = ctx.sql("SHOW datafusion.execution.batch_size");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        stream.loadNextBatch();
        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertEquals(1, root.getRowCount());
        String value =
            new String(((VarCharVector) root.getVector("value")).get(0), StandardCharsets.UTF_8);
        assertEquals("512", value);
      }
    }
  }
}
