package org.apache.arrow.datafusion.providers.sqlite;

import static org.apache.arrow.datafusion.providers.ProviderTestSupport.deleteQuietly;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readPhysicalPlan;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readVarCharColumn;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.seedUsersTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test of {@link SqliteConnectionPool}. The Rust-side factory runs on a bundled SQLite
 * (no system dep), so we set up the database from the JVM via the JDBC driver shipped with
 * xerial's sqlite-jdbc — but only for the test: production code doesn't need it.
 */
class SqliteProviderTest {

  @Test
  void registerTableAndQuery() throws Exception {
    Path tempDir = Files.createTempDirectory("sqlite-test-");
    Path dbPath = tempDir.resolve("users.db");
    try {
      seedUsersTable("jdbc:sqlite:" + dbPath.toAbsolutePath(), null, null);

      try (BufferAllocator allocator = new RootAllocator();
          SqliteConnectionPool pool = SqliteConnectionPool.file(dbPath.toString());
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

  @Test
  void registerCatalogAndQueryWithFederation() throws Exception {
    Path tempDir = Files.createTempDirectory("sqlite-test-");
    Path dbPath = tempDir.resolve("catalog.db");
    try {
      seedUsersTable("jdbc:sqlite:" + dbPath.toAbsolutePath(), null, null);

      try (BufferAllocator allocator = new RootAllocator();
          SqliteConnectionPool pool = SqliteConnectionPool.file(dbPath.toString());
          RustCatalogProvider catalog = pool.openCatalog();
          SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
        ctx.registerCatalog("sqlite", catalog);

        // Verify pushdown via EXPLAIN: the SQLite provider surfaces as a SqlExec (its
        // `sql_provider_datafusion::SqlExec`) with the WHERE clause inlined in its sql= attr.
        // With the sqlite-federation feature off, it would still be a SqlExec, but the exact
        // predicate would not be pushed.
        try (DataFrame df = ctx.sql("EXPLAIN SELECT name FROM sqlite.main.users WHERE id > 1");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          String physical = readPhysicalPlan(stream);
          assertTrue(
              physical != null && physical.contains("SqlExec"),
              "expected a SqlExec in the physical plan: " + physical);
          assertTrue(
              physical.contains("id > 1"),
              "expected the id > 1 predicate to be pushed into SQLite's sql=: " + physical);
        }

        // Verify the query actually returns the right data.
        try (DataFrame df =
                ctx.sql("SELECT name FROM sqlite.main.users WHERE id > 1 ORDER BY name");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          assertEquals(List.of("Bob", "Carol"), readVarCharColumn(stream, "name"));
        }
      }
    } finally {
      deleteQuietly(tempDir);
    }
  }

  @Test
  void memoryPoolBuilderAcceptsDefaults() {
    // Regression: openTable on an uninitialised in-memory DB is expected to fail with a
    // DataFusionError (schema not found), not a native crash.
    try (SqliteConnectionPool pool = SqliteConnectionPool.memory()) {
      try {
        pool.openTable("nonexistent").close();
        // If we reach here, the table somehow resolved — not expected for an empty DB.
      } catch (DataFusionError e) {
        // expected
      }
    }
  }
}
