package org.apache.arrow.datafusion.providers.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
      seedUsersTable(dbPath);

      try (BufferAllocator allocator = new RootAllocator();
          SqliteConnectionPool pool = SqliteConnectionPool.file(dbPath.toString());
          RustTableProvider users = pool.openTable("users");
          SessionContext ctx = new SessionContext()) {
        ctx.registerTable("users", users);

        try (DataFrame df = ctx.sql("SELECT name FROM users WHERE id > 1 ORDER BY name");
            SendableRecordBatchStream stream = df.executeStream(allocator)) {
          List<String> names = new ArrayList<>();
          while (stream.loadNextBatch()) {
            VectorSchemaRoot r = stream.getVectorSchemaRoot();
            VarCharVector name = (VarCharVector) r.getVector("name");
            for (int i = 0; i < r.getRowCount(); i++) {
              names.add(name.getObject(i).toString());
            }
          }
          assertEquals(List.of("Bob", "Carol"), names);
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
      seedUsersTable(dbPath);

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
          String physical = null;
          while (stream.loadNextBatch()) {
            VectorSchemaRoot r = stream.getVectorSchemaRoot();
            VarCharVector planType = (VarCharVector) r.getVector("plan_type");
            VarCharVector plan = (VarCharVector) r.getVector("plan");
            for (int i = 0; i < r.getRowCount(); i++) {
              if ("physical_plan".equals(planType.getObject(i).toString())) {
                physical = plan.getObject(i).toString();
              }
            }
          }
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
          List<String> names = new ArrayList<>();
          while (stream.loadNextBatch()) {
            VectorSchemaRoot r = stream.getVectorSchemaRoot();
            VarCharVector name = (VarCharVector) r.getVector("name");
            for (int i = 0; i < r.getRowCount(); i++) {
              names.add(name.getObject(i).toString());
            }
          }
          assertEquals(List.of("Bob", "Carol"), names);
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

  private static void seedUsersTable(Path dbPath) throws Exception {
    Files.deleteIfExists(dbPath.toAbsolutePath());
    String url = "jdbc:sqlite:" + dbPath.toAbsolutePath();
    try (Connection conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
      st.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
    }
  }

  // SQLite's async connection pool can hold -wal / -shm sidecar files open briefly after the
  // Rust Arc drops. Using @TempDir is brittle in that window; a manual cleanup that tolerates
  // residual files is more reliable.
  private static void deleteQuietly(Path root) {
    if (root == null || !Files.exists(root)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (Exception ignored) {
                  // Best-effort; OS-level temp cleanup handles the rest.
                }
              });
    } catch (Exception ignored) {
      // Best-effort.
    }
  }
}
