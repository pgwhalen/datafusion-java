package org.apache.arrow.datafusion.providers.postgres;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfPostgresConnectionPool;
import org.apache.arrow.datafusion.generated.DfRustCatalogProvider;
import org.apache.arrow.datafusion.generated.DfRustTableProvider;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;

/**
 * Handle to a PostgreSQL connection pool, from which {@link RustTableProvider} and {@link
 * RustCatalogProvider} instances can be opened.
 *
 * <p>Backed by {@code
 * datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool}. The
 * upstream {@code postgres-federation} feature is enabled, so every table opened through this pool
 * is federation-aware: combine with {@link
 * org.apache.arrow.datafusion.execution.SessionContext#newWithFederation(
 * org.apache.arrow.datafusion.config.ConfigOptions)} to get filter/projection/join pushdown into
 * PostgreSQL automatically.
 *
 * {@snippet :
 * try (PostgresConnectionPool pool = PostgresConnectionPool.builder()
 *         .host("localhost")
 *         .port(5432)
 *         .database("app")
 *         .user("postgres")
 *         .password("postgres")
 *         .sslMode("disable")
 *         .build();
 *     RustTableProvider users = pool.openTable("public.users");
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("users", users);
 *     ctx.sql("SELECT count(*) FROM users").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/postgrespool/struct.PostgresConnectionPool.html">Rust
 *     datafusion-table-providers: PostgresConnectionPool</a>
 */
public final class PostgresConnectionPool implements AutoCloseable {

  private final DfPostgresConnectionPool handle;

  private PostgresConnectionPool(DfPostgresConnectionPool handle) {
    this.handle = handle;
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Opens the named table. {@code tableReference} is parsed by DataFusion — accepts {@code
   * schema.table}, {@code catalog.schema.table}, or just {@code table} (defaults to the search
   * path).
   *
   * {@snippet :
   * try (RustTableProvider t = pool.openTable("public.customers")) {
   *     ctx.registerTable("customers", t);
   *     ctx.sql("SELECT * FROM customers").show();
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/postgres/struct.PostgresTableFactory.html#method.table_provider">Rust
   *     datafusion-table-providers: PostgresTableFactory::table_provider</a>
   */
  public RustTableProvider openTable(String tableReference) {
    Objects.requireNonNull(tableReference, "tableReference");
    try {
      DfRustTableProvider h = handle.openTable(tableReference);
      return new PostgresTableProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("Postgres open_table failed", e);
    }
  }

  /**
   * Opens the whole database as a {@link RustCatalogProvider} — one schema per Postgres schema,
   * one table per Postgres table. Suitable for {@code SessionContext.registerCatalog}.
   *
   * {@snippet :
   * try (RustCatalogProvider catalog = pool.openCatalog()) {
   *     ctx.registerCatalog("pg", catalog);
   *     ctx.sql("SELECT count(*) FROM pg.public.customers").show();
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/common/struct.DatabaseCatalogProvider.html#method.try_new">Rust
   *     datafusion-table-providers: DatabaseCatalogProvider::try_new</a>
   */
  public RustCatalogProvider openCatalog() {
    try {
      DfRustCatalogProvider h = handle.openCatalog();
      return new PostgresCatalogProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("Postgres open_catalog failed", e);
    }
  }

  @Override
  public void close() {
    handle.close();
  }

  /**
   * Builder for {@link PostgresConnectionPool}. Set fields via typed setters, or pass a raw {@code
   * connection_string} via {@link #connectionString(String)}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/postgrespool/struct.PostgresConnectionPool.html#method.new">Rust
   *     PostgresConnectionPool::new</a>
   */
  public static final class Builder {
    private final Map<String, String> options = new LinkedHashMap<>();

    private Builder() {}

    /** PostgreSQL host (e.g. {@code localhost}). */
    public Builder host(String host) {
      return option("host", host);
    }

    /** TCP port. */
    public Builder port(int port) {
      return option("port", Integer.toString(port));
    }

    /** Database name to connect to. */
    public Builder database(String database) {
      return option("db", database);
    }

    /** PostgreSQL username. */
    public Builder user(String user) {
      return option("user", user);
    }

    /** PostgreSQL password. */
    public Builder password(String password) {
      return option("pass", password);
    }

    /** SSL mode: one of {@code disable}, {@code prefer}, {@code require}, {@code verify-full}. */
    public Builder sslMode(String sslMode) {
      return option("sslmode", sslMode);
    }

    /**
     * Raw libpq-style connection string, used in place of the individual {@code host}/{@code
     * user}/etc. setters.
     */
    public Builder connectionString(String connectionString) {
      return option("connection_string", connectionString);
    }

    /** Raw escape hatch for any parameter accepted by {@code PostgresConnectionPool::new}. */
    public Builder option(String key, String value) {
      options.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
      return this;
    }

    /** Open the pool. */
    public PostgresConnectionPool build() {
      String[] keys = options.keySet().toArray(new String[0]);
      String[] values = options.values().toArray(new String[0]);
      try {
        return new PostgresConnectionPool(DfPostgresConnectionPool.open(keys, values));
      } catch (DfError e) {
        throw new NativeDataFusionError(e);
      } catch (RuntimeException e) {
        throw new DataFusionError("Postgres pool open failed", e);
      }
    }
  }

  private static final class PostgresTableProvider extends RustTableProvider {
    PostgresTableProvider(DfRustTableProvider handle) {
      super(handle);
    }
  }

  private static final class PostgresCatalogProvider extends RustCatalogProvider {
    PostgresCatalogProvider(DfRustCatalogProvider handle) {
      super(handle);
    }
  }
}
