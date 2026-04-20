package org.apache.arrow.datafusion.providers.sqlite;

import java.time.Duration;
import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfRustCatalogProvider;
import org.apache.arrow.datafusion.generated.DfRustTableProvider;
import org.apache.arrow.datafusion.generated.DfSqliteConnectionPool;
import org.apache.arrow.datafusion.generated.DfSqliteMode;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;

/**
 * Handle to a SQLite connection pool, from which {@link RustTableProvider} and {@link
 * RustCatalogProvider} instances can be opened.
 *
 * <p>Backed by {@code datafusion_table_providers::sql::db_connection_pool::sqlitepool::
 * SqliteConnectionPool} (compiled with the {@code bundled} feature — no system SQLite needed).
 * The upstream {@code sqlite-federation} feature is also enabled, so every table opened through
 * this pool is federation-aware: combine with {@link
 * org.apache.arrow.datafusion.execution.SessionContext#newWithFederation(
 * org.apache.arrow.datafusion.config.ConfigOptions)} to get filter/projection/join pushdown
 * into SQLite automatically.
 *
 * {@snippet :
 * try (SqliteConnectionPool pool = SqliteConnectionPool.memory();
 *     RustTableProvider users = pool.openTable("users");
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("users", users);
 *     ctx.sql("SELECT count(*) FROM users").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/sqlitepool/struct.SqliteConnectionPool.html">Rust
 *     datafusion-table-providers: SqliteConnectionPool</a>
 */
public final class SqliteConnectionPool implements AutoCloseable {

  /** Default busy timeout applied when none is specified. */
  public static final Duration DEFAULT_BUSY_TIMEOUT = Duration.ofSeconds(5);

  private final DfSqliteConnectionPool handle;

  private SqliteConnectionPool(DfSqliteConnectionPool handle) {
    this.handle = handle;
  }

  /** Open an in-memory SQLite database with the default busy timeout. */
  public static SqliteConnectionPool memory() {
    return open("", Mode.MEMORY, DEFAULT_BUSY_TIMEOUT);
  }

  /** Open (or create) a SQLite file at {@code path} with the default busy timeout. */
  public static SqliteConnectionPool file(String path) {
    return open(path, Mode.FILE, DEFAULT_BUSY_TIMEOUT);
  }

  /**
   * Open a SQLite database.
   *
   * @param path filesystem path (ignored for {@link Mode#MEMORY})
   * @param mode {@link Mode#MEMORY} or {@link Mode#FILE}
   * @param busyTimeout SQLite busy timeout
   */
  public static SqliteConnectionPool open(String path, Mode mode, Duration busyTimeout) {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(mode, "mode");
    Objects.requireNonNull(busyTimeout, "busyTimeout");
    try {
      DfSqliteConnectionPool p =
          DfSqliteConnectionPool.open(path, mode.toNative(), busyTimeout.toMillis());
      return new SqliteConnectionPool(p);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("SQLite pool open failed", e);
    }
  }

  /**
   * Opens the named table. The result can be registered via {@code SessionContext.registerTable}.
   *
   * {@snippet :
   * try (RustTableProvider t = pool.openTable("customers")) {
   *     ctx.registerTable("customers", t);
   *     ctx.sql("SELECT * FROM customers").show();
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sqlite/struct.SqliteTableFactory.html#method.table_provider">Rust
   *     datafusion-table-providers: SqliteTableFactory::table_provider</a>
   */
  public RustTableProvider openTable(String tableName) {
    Objects.requireNonNull(tableName, "tableName");
    try {
      DfRustTableProvider h = handle.openTable(tableName);
      return new SqliteTableProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("SQLite open_table failed", e);
    }
  }

  /**
   * Opens the whole database as a {@link RustCatalogProvider} — one schema per SQLite schema, one
   * table per SQLite table. Suitable for {@code SessionContext.registerCatalog}.
   *
   * {@snippet :
   * try (RustCatalogProvider catalog = pool.openCatalog()) {
   *     ctx.registerCatalog("sqlite", catalog);
   *     ctx.sql("SELECT count(*) FROM sqlite.main.customers").show();
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
      return new SqliteCatalogProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("SQLite open_catalog failed", e);
    }
  }

  @Override
  public void close() {
    handle.close();
  }

  /** SQLite open mode. */
  public enum Mode {
    MEMORY(DfSqliteMode.MEMORY),
    FILE(DfSqliteMode.FILE);

    private final DfSqliteMode native_;

    Mode(DfSqliteMode n) {
      this.native_ = n;
    }

    DfSqliteMode toNative() {
      return native_;
    }
  }

  private static final class SqliteTableProvider extends RustTableProvider {
    SqliteTableProvider(DfRustTableProvider handle) {
      super(handle);
    }
  }

  private static final class SqliteCatalogProvider extends RustCatalogProvider {
    SqliteCatalogProvider(DfRustCatalogProvider handle) {
      super(handle);
    }
  }
}
