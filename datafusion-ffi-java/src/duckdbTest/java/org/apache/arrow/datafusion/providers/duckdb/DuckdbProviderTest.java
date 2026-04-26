package org.apache.arrow.datafusion.providers.duckdb;

import static org.apache.arrow.datafusion.providers.ProviderTestSupport.deleteQuietly;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readVarCharColumn;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.seedUsersTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.providers.RustTableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/**
 * Smoke test for the opt-in {@code duckdb} feature. Seeds a DuckDB file with a tiny table via the
 * Java DuckDB JDBC driver (to keep the Rust factory test-only code free), then queries it through
 * the Rust pool.
 */
class DuckdbProviderTest {

  @Test
  void registerTableAndQuery() throws Exception {
    Path tempDir = Files.createTempDirectory("duckdb-test-");
    Path dbPath = tempDir.resolve("users.duckdb");
    try {
      seedUsersTable("jdbc:duckdb:" + dbPath.toAbsolutePath(), null, null);

      try (BufferAllocator allocator = new RootAllocator();
          DuckdbConnectionPool pool = DuckdbConnectionPool.file(dbPath.toString());
          RustTableProvider users = pool.openTable("users");
          SessionContext ctx = new SessionContext()) {
        ctx.registerTable("users", users);

        try (DataFrame df = ctx.sql("SELECT name FROM users WHERE id > 1 ORDER BY name");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          assertEquals(List.of("Bob", "Carol"), readVarCharColumn(stream, "name"));
        }
      }
    } finally {
      deleteQuietly(tempDir);
    }
  }
}
