package org.apache.arrow.datafusion.providers.mysql;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfMysqlConnectionPool;
import org.apache.arrow.datafusion.generated.DfRustCatalogProvider;
import org.apache.arrow.datafusion.generated.DfRustTableProvider;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;

/**
 * Handle to a MySQL connection pool, from which {@link RustTableProvider} and {@link
 * RustCatalogProvider} instances can be opened.
 *
 * <p>Backed by {@code
 * datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool}. The
 * upstream {@code mysql-federation} feature is enabled, so every table opened through this pool is
 * federation-aware: combine with {@link
 * org.apache.arrow.datafusion.execution.SessionContext#newWithFederation(
 * org.apache.arrow.datafusion.config.ConfigOptions)} to get filter/projection/join pushdown into
 * MySQL automatically.
 *
 * {@snippet :
 * try (MysqlConnectionPool pool = MysqlConnectionPool.builder()
 *         .host("localhost")
 *         .port(3306)
 *         .database("app")
 *         .user("root")
 *         .password("secret")
 *         .sslMode("disabled")
 *         .build();
 *     RustTableProvider users = pool.openTable("users");
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("users", users);
 *     ctx.sql("SELECT count(*) FROM users").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/mysqlpool/struct.MySQLConnectionPool.html">Rust
 *     datafusion-table-providers: MySQLConnectionPool</a>
 */
public final class MysqlConnectionPool implements AutoCloseable {

  private final DfMysqlConnectionPool handle;

  private MysqlConnectionPool(DfMysqlConnectionPool handle) {
    this.handle = handle;
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Opens the named table. {@code tableReference} is parsed by DataFusion — accepts {@code
   * schema.table}, {@code catalog.schema.table}, or just {@code table} (defaults to the connected
   * database).
   *
   * {@snippet :
   * try (RustTableProvider t = pool.openTable("customers")) {
   *     ctx.registerTable("customers", t);
   *     ctx.sql("SELECT * FROM customers").show();
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/mysql/struct.MySQLTableFactory.html#method.table_provider">Rust
   *     datafusion-table-providers: MySQLTableFactory::table_provider</a>
   */
  public RustTableProvider openTable(String tableReference) {
    Objects.requireNonNull(tableReference, "tableReference");
    try {
      DfRustTableProvider h = handle.openTable(tableReference);
      return new MysqlTableProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("MySQL open_table failed", e);
    }
  }

  /**
   * Opens the whole database as a {@link RustCatalogProvider} — one schema per MySQL schema, one
   * table per MySQL table. Suitable for {@code SessionContext.registerCatalog}.
   *
   * {@snippet :
   * try (RustCatalogProvider catalog = pool.openCatalog()) {
   *     ctx.registerCatalog("mysql", catalog);
   *     ctx.sql("SELECT count(*) FROM mysql.app.customers").show();
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
      return new MysqlCatalogProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("MySQL open_catalog failed", e);
    }
  }

  @Override
  public void close() {
    handle.close();
  }

  /**
   * Builder for {@link MysqlConnectionPool}. Set fields via typed setters, or pass a raw {@code
   * connection_string} via {@link #connectionString(String)}.
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/mysqlpool/struct.MySQLConnectionPool.html#method.new">Rust
   *     MySQLConnectionPool::new</a>
   */
  public static final class Builder {
    private final Map<String, String> options = new LinkedHashMap<>();

    private Builder() {}

    /** MySQL host (e.g. {@code localhost}). */
    public Builder host(String host) {
      return option("host", host);
    }

    /** TCP port. */
    public Builder port(int port) {
      return option("tcp_port", Integer.toString(port));
    }

    /** Database name to connect to. */
    public Builder database(String database) {
      return option("db", database);
    }

    /** MySQL username. */
    public Builder user(String user) {
      return option("user", user);
    }

    /** MySQL password. */
    public Builder password(String password) {
      return option("pass", password);
    }

    /** SSL mode: one of {@code disabled}, {@code required}, {@code preferred}. */
    public Builder sslMode(String sslMode) {
      return option("sslmode", sslMode);
    }

    /**
     * Raw MySQL-URL connection string (e.g. {@code mysql://user:pass@host:3306/db}), used in place
     * of the individual setters.
     */
    public Builder connectionString(String connectionString) {
      return option("connection_string", connectionString);
    }

    /** Raw escape hatch for any parameter accepted by {@code MySQLConnectionPool::new}. */
    public Builder option(String key, String value) {
      options.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
      return this;
    }

    /** Open the pool. */
    public MysqlConnectionPool build() {
      String[] keys = options.keySet().toArray(new String[0]);
      String[] values = options.values().toArray(new String[0]);
      try {
        return new MysqlConnectionPool(DfMysqlConnectionPool.open(keys, values));
      } catch (DfError e) {
        throw new NativeDataFusionError(e);
      } catch (RuntimeException e) {
        throw new DataFusionError("MySQL pool open failed", e);
      }
    }
  }

  private static final class MysqlTableProvider extends RustTableProvider {
    MysqlTableProvider(DfRustTableProvider handle) {
      super(handle);
    }
  }

  private static final class MysqlCatalogProvider extends RustCatalogProvider {
    MysqlCatalogProvider(DfRustCatalogProvider handle) {
      super(handle);
    }
  }
}
