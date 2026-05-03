package org.apache.arrow.datafusion.providers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Assumptions;

/**
 * Helpers shared across the per-provider {@code *Test} source sets. Lives in {@code
 * src/test/java/} so it's automatically visible on every {@code <feature>Test} classpath via
 * {@code sourceSets.test.output} in {@code build.gradle}.
 */
public final class ProviderTestSupport {

  private ProviderTestSupport() {}

  /**
   * Skip the calling test class if {@code SKIP_TESTCONTAINERS=true} is set in the environment.
   * Intended for {@code @BeforeAll} on testcontainers-backed tests.
   */
  public static void assumeTestcontainersAllowed() {
    Assumptions.assumeFalse(
        "true".equalsIgnoreCase(System.getenv("SKIP_TESTCONTAINERS")), "SKIP_TESTCONTAINERS=true");
  }

  /**
   * Run one or more JDBC statements against {@code url}. Both {@code user} and {@code pass} may be
   * {@code null} for embedded engines like SQLite/DuckDB that don't take credentials.
   */
  public static void execJdbc(String url, String user, String pass, String... statements)
      throws Exception {
    Connection conn =
        (user == null && pass == null)
            ? DriverManager.getConnection(url)
            : DriverManager.getConnection(url, user, pass);
    try (conn;
        Statement st = conn.createStatement()) {
      for (String s : statements) {
        st.execute(s);
      }
    }
  }

  /**
   * Seed the canonical test table {@code users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)} with
   * three rows — Alice, Bob, Carol. Default {@code TEXT} works for SQLite, DuckDB, Postgres; use
   * {@link #seedUsersTable(String, String, String, String)} for MySQL ({@code VARCHAR(64)}).
   */
  public static void seedUsersTable(String url, String user, String pass) throws Exception {
    seedUsersTable(url, user, pass, "TEXT");
  }

  /** Same as {@link #seedUsersTable(String, String, String)} with a custom column type for {@code name}. */
  public static void seedUsersTable(String url, String user, String pass, String nameType)
      throws Exception {
    execJdbc(
        url,
        user,
        pass,
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name " + nameType + " NOT NULL)",
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
  }

  /** Drain {@code stream}, returning every value in the named {@code VARCHAR} column. */
  public static List<String> readVarCharColumn(
      SendableRecordBatchStream stream, String columnName) {
    List<String> values = new ArrayList<>();
    while (stream.loadNextBatch()) {
      VectorSchemaRoot r = stream.getVectorSchemaRoot();
      VarCharVector v = (VarCharVector) r.getVector(columnName);
      for (int i = 0; i < r.getRowCount(); i++) {
        values.add(v.getObject(i).toString());
      }
    }
    return values;
  }

  /**
   * Drain the result of an {@code EXPLAIN}, returning the physical-plan string (the row whose
   * {@code plan_type} equals {@code "physical_plan"}). Returns {@code null} if the stream contains
   * no such row.
   */
  public static String readPhysicalPlan(SendableRecordBatchStream stream) {
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
    return physical;
  }

  /**
   * Recursively delete the contents of {@code root}, ignoring errors. Useful for cleaning up a
   * temp dir whose contents may include OS-held file handles (e.g. SQLite's {@code -wal} /
   * {@code -shm} sidecar files that the async pool may briefly hold open after the Rust {@code
   * Arc} drops).
   */
  public static void deleteQuietly(Path root) {
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
                  // Best-effort
                }
              });
    } catch (Exception ignored) {
      // Best-effort
    }
  }
}
