package org.apache.arrow.datafusion.providers.mysql;

import static org.apache.arrow.datafusion.providers.ProviderTestSupport.assumeTestcontainersAllowed;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readPhysicalPlan;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readVarCharColumn;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.seedUsersTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test for {@link MysqlConnectionPool} using a real MySQL server in a
 * testcontainers-managed Docker container.
 *
 * <p>Requires a running Docker daemon; testcontainers fails fast otherwise. Set {@code
 * SKIP_TESTCONTAINERS=true} to skip if Docker is unavailable.
 */
class MysqlProviderTest {

  private static MySQLContainer<?> container;

  @BeforeAll
  static void startContainer() throws Exception {
    assumeTestcontainersAllowed();
    container =
        new MySQLContainer<>(DockerImageName.parse("mysql:8.4"))
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");
    container.start();
    // MySQL doesn't accept TEXT for a primary-key-table column constraint chain in some drivers;
    // VARCHAR(64) keeps the seed portable across versions.
    seedUsersTable(
        container.getJdbcUrl(), container.getUsername(), container.getPassword(), "VARCHAR(64)");
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static MysqlConnectionPool newPool() {
    return MysqlConnectionPool.builder()
        .host(container.getHost())
        .port(container.getMappedPort(MySQLContainer.MYSQL_PORT))
        .database(container.getDatabaseName())
        .user(container.getUsername())
        .password(container.getPassword())
        .sslMode("disabled")
        .build();
  }

  @Test
  void registerTableAndQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        MysqlConnectionPool pool = newPool();
        RustTableProvider users = pool.openTable("users");
        SessionContext ctx = new SessionContext()) {
      ctx.registerTable("users", users);

      try (DataFrame df = ctx.sql("SELECT name FROM users WHERE id > 1 ORDER BY name");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertEquals(List.of("Bob", "Carol"), readVarCharColumn(stream, "name"));
      }
    }
  }

  @Test
  void registerCatalogAndQueryWithFederation() {
    try (BufferAllocator allocator = new RootAllocator();
        MysqlConnectionPool pool = newPool();
        RustCatalogProvider catalog = pool.openCatalog();
        SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
      ctx.registerCatalog("mysql", catalog);

      // MySQL schemas are database names. The seeded table lives in `test.users`.
      String fqn = "mysql.test.users";
      try (DataFrame df = ctx.sql("EXPLAIN SELECT name FROM " + fqn + " WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        String physical = readPhysicalPlan(stream);
        assertTrue(physical != null, "expected a physical plan");
        assertTrue(
            physical.contains("id > 1"),
            "expected the id > 1 predicate to be pushed into MySQL: " + physical);
      }

      // Avoid ORDER BY: DataFusion's default unparser emits NULLS LAST/FIRST, which MySQL
      // doesn't accept. DatabaseSchemaProvider in datafusion-table-providers 0.11 builds
      // SqlTable without the MySQL dialect, so federation-level SQL uses the default.
      try (DataFrame df = ctx.sql("SELECT name FROM " + fqn + " WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        List<String> names = new ArrayList<>(readVarCharColumn(stream, "name"));
        Collections.sort(names);
        assertEquals(List.of("Bob", "Carol"), names);
      }
    }
  }

  @Test
  void builderConnectionStringVariant() {
    String url =
        String.format(
            "mysql://%s:%s@%s:%d/%s",
            container.getUsername(),
            container.getPassword(),
            container.getHost(),
            container.getMappedPort(MySQLContainer.MYSQL_PORT),
            container.getDatabaseName());
    try (BufferAllocator allocator = new RootAllocator();
        MysqlConnectionPool pool =
            MysqlConnectionPool.builder().connectionString(url).sslMode("disabled").build();
        RustTableProvider users = pool.openTable("users");
        SessionContext ctx = new SessionContext()) {
      ctx.registerTable("users", users);

      try (DataFrame df = ctx.sql("SELECT name FROM users");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        List<String> names = new ArrayList<>(readVarCharColumn(stream, "name"));
        Collections.sort(names);
        assertEquals(List.of("Alice", "Bob", "Carol"), names);
      }
    }
  }
}
