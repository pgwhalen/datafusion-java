package org.apache.arrow.datafusion.providers.postgres;

import static org.apache.arrow.datafusion.providers.ProviderTestSupport.assumeTestcontainersAllowed;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readPhysicalPlan;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.readVarCharColumn;
import static org.apache.arrow.datafusion.providers.ProviderTestSupport.seedUsersTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end test for {@link PostgresConnectionPool} using a real Postgres server in a
 * testcontainers-managed Docker container.
 *
 * <p>Requires a running Docker daemon; testcontainers fails fast otherwise. Set {@code
 * SKIP_TESTCONTAINERS=true} to skip if Docker is unavailable.
 */
class PostgresProviderTest {

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void startContainer() throws Exception {
    assumeTestcontainersAllowed();
    container =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test");
    container.start();
    seedUsersTable(container.getJdbcUrl(), container.getUsername(), container.getPassword());
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static PostgresConnectionPool newPool() {
    return PostgresConnectionPool.builder()
        .host(container.getHost())
        .port(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
        .database(container.getDatabaseName())
        .user(container.getUsername())
        .password(container.getPassword())
        .sslMode("disable")
        .build();
  }

  @Test
  void registerTableAndQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        PostgresConnectionPool pool = newPool();
        RustTableProvider users = pool.openTable("public.users");
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
        PostgresConnectionPool pool = newPool();
        RustCatalogProvider catalog = pool.openCatalog();
        SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
      ctx.registerCatalog("pg", catalog);

      // Verify pushdown via EXPLAIN: with postgres-federation enabled, the predicate ends up
      // inside the remote SQL string of the federation VirtualExecutionPlan (or a SqlExec wrapping
      // a federation-rewritten subplan).
      try (DataFrame df = ctx.sql("EXPLAIN SELECT name FROM pg.public.users WHERE id > 1");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        String physical = readPhysicalPlan(stream);
        assertTrue(physical != null, "expected a physical plan");
        assertTrue(
            physical.contains("id > 1"),
            "expected the id > 1 predicate to be pushed into Postgres: " + physical);
      }

      try (DataFrame df = ctx.sql("SELECT name FROM pg.public.users WHERE id > 1 ORDER BY name");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertEquals(List.of("Bob", "Carol"), readVarCharColumn(stream, "name"));
      }
    }
  }

  @Test
  void builderConnectionStringVariant() {
    try (BufferAllocator allocator = new RootAllocator();
        PostgresConnectionPool pool =
            PostgresConnectionPool.builder()
                .connectionString(
                    String.format(
                        "host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
                        container.getHost(),
                        container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT),
                        container.getDatabaseName(),
                        container.getUsername(),
                        container.getPassword()))
                .build();
        RustTableProvider users = pool.openTable("public.users");
        SessionContext ctx = new SessionContext()) {
      ctx.registerTable("users", users);

      try (DataFrame df = ctx.sql("SELECT name FROM users ORDER BY id");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {
        assertEquals(List.of("Alice", "Bob", "Carol"), readVarCharColumn(stream, "name"));
      }
    }
  }
}
