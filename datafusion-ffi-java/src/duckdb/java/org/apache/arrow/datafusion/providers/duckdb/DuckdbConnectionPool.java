package org.apache.arrow.datafusion.providers.duckdb;

import java.util.Objects;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.generated.DfDuckdbConnectionPool;
import org.apache.arrow.datafusion.generated.DfDuckdbMode;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfRustTableProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;

/**
 * Handle to a DuckDB connection pool. Compile-time opt-in: this class is only present when the
 * native library is built with the {@code duckdb} Cargo feature (Gradle {@code -Pfeatures=duckdb}).
 *
 * <p>Backed by {@code
 * datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool}. With the
 * upstream {@code duckdb-federation} feature enabled (automatically, via our {@code duckdb}
 * feature), tables opened here are federation-aware: register against {@link
 * org.apache.arrow.datafusion.execution.SessionContext#newWithFederation(
 * org.apache.arrow.datafusion.config.ConfigOptions)} to get filter/projection/join pushdown into
 * DuckDB transparently.
 *
 * {@snippet :
 * try (DuckdbConnectionPool pool = DuckdbConnectionPool.memory();
 *     RustTableProvider taxi = pool.openTable("taxi");
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("taxi", taxi);
 *     ctx.sql("SELECT count(*) FROM taxi").show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/sql/db_connection_pool/duckdbpool/struct.DuckDbConnectionPool.html">Rust
 *     datafusion-table-providers: DuckDbConnectionPool</a>
 */
public final class DuckdbConnectionPool implements AutoCloseable {

  private final DfDuckdbConnectionPool handle;

  private DuckdbConnectionPool(DfDuckdbConnectionPool handle) {
    this.handle = handle;
  }

  /** Open an in-memory DuckDB database. */
  public static DuckdbConnectionPool memory() {
    return open("", Mode.MEMORY);
  }

  /** Open (or create) a DuckDB file at {@code path}. */
  public static DuckdbConnectionPool file(String path) {
    return open(path, Mode.FILE);
  }

  /**
   * Open a DuckDB database.
   *
   * @param path filesystem path (ignored for {@link Mode#MEMORY})
   * @param mode {@link Mode#MEMORY} or {@link Mode#FILE}
   */
  public static DuckdbConnectionPool open(String path, Mode mode) {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(mode, "mode");
    try {
      return new DuckdbConnectionPool(DfDuckdbConnectionPool.open(path, mode.toNative()));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("DuckDB pool open failed", e);
    }
  }

  /**
   * Opens the named table. The result can be registered via {@code SessionContext.registerTable}.
   *
   * {@snippet :
   * try (RustTableProvider t = pool.openTable("events")) {
   *     ctx.registerTable("events", t);
   *     ctx.sql("SELECT * FROM events").show();
   * }
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion-table-providers/0.11.0/datafusion_table_providers/duckdb/struct.DuckDBTableFactory.html#method.table_provider">Rust
   *     datafusion-table-providers: DuckDBTableFactory::table_provider</a>
   */
  public RustTableProvider openTable(String tableName) {
    Objects.requireNonNull(tableName, "tableName");
    try {
      DfRustTableProvider h = handle.openTable(tableName);
      return new DuckdbTableProvider(h);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (RuntimeException e) {
      throw new DataFusionError("DuckDB open_table failed", e);
    }
  }

  @Override
  public void close() {
    handle.close();
  }

  /** DuckDB open mode. */
  public enum Mode {
    MEMORY(DfDuckdbMode.MEMORY),
    FILE(DfDuckdbMode.FILE);

    private final DfDuckdbMode native_;

    Mode(DfDuckdbMode n) {
      this.native_ = n;
    }

    DfDuckdbMode toNative() {
      return native_;
    }
  }

  private static final class DuckdbTableProvider extends RustTableProvider {
    DuckdbTableProvider(DfRustTableProvider handle) {
      super(handle);
    }
  }
}
