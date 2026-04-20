package org.apache.arrow.datafusion.execution;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.NativeCatalogProvider;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.datasource.ArrowReadOptions;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.apache.arrow.datafusion.datasource.FileFormat;
import org.apache.arrow.datafusion.datasource.ListingTable;
import org.apache.arrow.datafusion.datasource.NdJsonReadOptions;
import org.apache.arrow.datafusion.datasource.ParquetReadOptions;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.LogicalPlan;
import org.apache.arrow.datafusion.logical_expr.ScalarUDF;
import org.apache.arrow.datafusion.providers.RustCatalogProvider;
import org.apache.arrow.datafusion.providers.RustTableProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A session context for executing DataFusion queries using the FFM API.
 *
 * <p>This class wraps a native DataFusion SessionContext and provides methods for registering
 * tables and executing SQL queries.
 *
 * <p>Example:
 *
 * {@snippet :
 * import static org.apache.arrow.datafusion.Functions.*;
 *
 * try (SessionContext ctx = new SessionContext()) {
 *     ctx.registerCsv("sales", "/data/sales.csv", CsvReadOptions.defaults(), allocator);
 *     DataFrame result = ctx.sql("SELECT region, SUM(amount) FROM sales GROUP BY region")
 *         .sort(col("region").sortAsc())
 *         .limit(0, 10);
 *     result.show();
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html">Rust
 *     DataFusion: SessionContext</a>
 */
public class SessionContext implements AutoCloseable {
  private final SessionContextBridge bridge;
  private volatile boolean closed = false;

  /** Creates a new session context with default configuration. */
  public SessionContext() {
    this(ConfigOptions.defaults());
  }

  /**
   * Creates a new session context with the specified configuration.
   *
   * @param config the session configuration
   */
  public SessionContext(ConfigOptions config) {
    this.bridge = new SessionContextBridge(config);
  }

  /** Package-private constructor that wraps an existing bridge. */
  SessionContext(SessionContextBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Creates a new session context with default configuration.
   *
   * <p>Equivalent to {@code new SessionContext()}.
   *
   * {@snippet :
   * SessionContext ctx = SessionContext.create();
   * }
   *
   * @return a new SessionContext
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.new">Rust
   *     DataFusion: SessionContext::new</a>
   */
  public static SessionContext create() {
    return new SessionContext();
  }

  /**
   * Creates a new session context with the specified configuration.
   *
   * <p>Equivalent to Rust's {@code SessionContext::new_with_config(config)}.
   *
   * {@snippet :
   * ConfigOptions config = ConfigOptions.defaults();
   * SessionContext ctx = SessionContext.newWithConfig(config);
   * }
   *
   * @param config the session configuration
   * @return a new SessionContext
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.new_with_config">Rust
   *     DataFusion: SessionContext::new_with_config</a>
   */
  public static SessionContext newWithConfig(ConfigOptions config) {
    return new SessionContext(config);
  }

  /**
   * Creates a new session context with the specified configuration and runtime environment.
   *
   * <p>This mirrors Rust's {@code SessionContext::new_with_config_rt(config, Arc<RuntimeEnv>)}. The
   * RuntimeEnv is cloned internally, so the caller retains ownership and must close it separately.
   *
   * <p>Example:
   *
   * {@snippet :
   * RuntimeEnv rt = RuntimeEnvBuilder.builder()
   *     .withMemoryLimit(50_000_000, 1.0)
   *     .build();
   * ConfigOptions config = ConfigOptions.defaults();
   * try (SessionContext ctx = SessionContext.newWithConfigRt(config, rt)) {
   *     DataFrame df = ctx.sql("SELECT 1 + 1");
   * }
   * rt.close();
   * }
   *
   * @param config the session configuration
   * @param runtimeEnv the runtime environment (e.g., with memory pool configuration)
   * @return a new SessionContext
   * @throws DataFusionError if context creation fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.new_with_config_rt">Rust
   *     DataFusion: SessionContext::new_with_config_rt</a>
   */
  public static SessionContext newWithConfigRt(ConfigOptions config, RuntimeEnv runtimeEnv) {
    return new SessionContext(new SessionContextBridge(config, runtimeEnv.bridge.dfRuntimeEnv()));
  }

  /**
   * Creates a new session context with federation support enabled.
   *
   * <p>Federation-aware providers (for example, {@code
   * org.apache.arrow.datafusion.providers.flight.FlightSqlFederatedCatalog}) push filters,
   * projections, joins, and aggregates down to their remote systems. Providers registered with
   * this context that are <em>not</em> federation-aware behave exactly as they do in a regular
   * session — the federation optimizer and planner only rewrite plans whose scans resolve to a
   * {@code FederationProvider}.
   *
   * {@snippet :
   * try (SessionContext ctx = SessionContext.newWithFederation(ConfigOptions.defaults())) {
   *     // Register federated catalogs / schemas here ...
   *     DataFrame df = ctx.sql("SELECT * FROM remote1.public.t1 JOIN remote2.public.t2 USING (id)");
   *     df.show();
   * }
   * }
   *
   * @param config the session configuration
   * @return a new SessionContext wired with {@code FederationOptimizerRule} and
   *     {@code FederatedQueryPlanner}
   * @throws DataFusionError if context creation fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.new_with_state">Rust
   *     DataFusion: SessionContext::new_with_state</a>
   */
  public static SessionContext newWithFederation(ConfigOptions config) {
    return new SessionContext(SessionContextBridge.federated(config));
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context.
   *
   * <p>This is equivalent to Rust's {@code register_batch}: it takes Arrow data directly and wraps
   * it in a MemTable.
   *
   * {@snippet :
   * try (SessionContext ctx = new SessionContext()) {
   *     ctx.registerBatch("my_table", vectorSchemaRoot, allocator);
   *     DataFrame df = ctx.sql("SELECT * FROM my_table");
   *     df.show();
   * }
   * }
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_batch">Rust
   *     DataFusion: SessionContext::register_batch</a>
   */
  public void registerBatch(String name, VectorSchemaRoot root, BufferAllocator allocator) {
    registerBatch(name, root, null, allocator);
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context with dictionary support.
   *
   * <p>This is equivalent to Rust's {@code register_batch}: it takes Arrow data directly and wraps
   * it in a MemTable. See {@link #registerBatch(String, VectorSchemaRoot, BufferAllocator)} for an
   * example.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param provider The DictionaryProvider for dictionary-encoded columns (may be null)
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_batch">Rust
   *     DataFusion: SessionContext::register_batch</a>
   */
  public void registerBatch(
      String name, VectorSchemaRoot root, DictionaryProvider provider, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerBatch(name, root, provider, allocator);
  }

  /**
   * Registers a table provider with the session context.
   *
   * <p>This is equivalent to Rust's {@code register_table}: it registers a {@link TableProvider}
   * implementation that DataFusion will call when the table is queried.
   *
   * {@snippet :
   * try (SessionContext ctx = new SessionContext()) {
   *     ctx.registerTable("my_table", myTableProvider, allocator);
   *     DataFrame df = ctx.sql("SELECT * FROM my_table");
   *     df.show();
   * }
   * }
   *
   * @param name The table name
   * @param provider The table provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_table">Rust
   *     DataFusion: SessionContext::register_table</a>
   */
  public void registerTable(String name, TableProvider provider, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerTableProvider(name, provider, allocator);
  }

  /**
   * Registers a Rust-side table provider (for example one produced by {@link
   * org.apache.arrow.datafusion.providers.flight.FlightSqlTableProvider#builder()}).
   *
   * <p>The provider handle remains owned by the caller — registering it does not transfer
   * ownership, and callers must still {@link RustTableProvider#close() close} the handle when
   * done.
   *
   * {@snippet :
   * try (FlightSqlTableProvider taxi = FlightSqlTableProvider.builder()
   *         .endpoint("http://localhost:32010")
   *         .query("SELECT * FROM taxi")
   *         .build()) {
   *     ctx.registerTable("taxi", taxi);
   *     ctx.sql("SELECT count(*) FROM taxi").show();
   * }
   * }
   *
   * @param name the table name used in SQL
   * @param provider a Rust-backed table provider
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_table">Rust
   *     DataFusion: SessionContext::register_table</a>
   */
  public void registerTable(String name, RustTableProvider provider) {
    checkNotClosed();
    bridge.registerRustTable(name, provider);
  }

  /**
   * Deregisters a table by name.
   *
   * {@snippet :
   * boolean existed = ctx.deregisterTable("my_table");
   * }
   *
   * @param name The table name
   * @return true if a table was previously registered with this name
   * @throws DataFusionError if deregistration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.deregister_table">Rust
   *     DataFusion: SessionContext::deregister_table</a>
   */
  public boolean deregisterTable(String name) {
    checkNotClosed();
    return bridge.deregisterTable(name);
  }

  /**
   * Executes a SQL query and returns a DataFrame.
   *
   * {@snippet :
   * DataFrame df = ctx.sql("SELECT name, age FROM users WHERE age > 21");
   * df.show();
   * }
   *
   * @param query The SQL query to execute
   * @return A DataFrame representing the query result
   * @throws DataFusionError if query execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.sql">Rust
   *     DataFusion: SessionContext::sql</a>
   */
  public DataFrame sql(String query) {
    checkNotClosed();
    return new DataFrame(bridge.sql(query));
  }

  /**
   * Executes a {@link LogicalPlan} and returns a {@link DataFrame}.
   *
   * <p>The plan is cloned internally, so the same plan can be executed multiple times.
   *
   * {@snippet :
   * LogicalPlan plan = LogicalPlanBuilder.scan("my_table", ctx)
   *     .filter(col("id").gt(lit(100)))
   *     .build();
   * DataFrame df = ctx.executeLogicalPlan(plan);
   * df.show();
   * }
   *
   * @param plan the logical plan to execute
   * @return a DataFrame representing the execution result
   * @throws DataFusionError if execution fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.execute_logical_plan">Rust
   *     DataFusion: SessionContext::execute_logical_plan</a>
   */
  public DataFrame executeLogicalPlan(LogicalPlan plan) {
    checkNotClosed();
    return new DataFrame(bridge.executeLogicalPlan(plan.bridge()));
  }

  /**
   * Registers a catalog provider with the session context.
   *
   * <p>The catalog can be accessed in SQL queries using the catalog name. For example, if you
   * register a catalog named "my_catalog" with a schema "my_schema" containing a table "my_table",
   * you can query it with:
   *
   * {@snippet :
   * ctx.registerCatalog("my_catalog", myCatalog, allocator);
   * DataFrame df = ctx.sql("SELECT * FROM my_catalog.my_schema.my_table");
   * df.show();
   * }
   *
   * @param name The catalog name
   * @param catalog The catalog provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_catalog">Rust
   *     DataFusion: SessionContext::register_catalog</a>
   */
  public void registerCatalog(String name, CatalogProvider catalog, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerCatalog(name, catalog, allocator);
  }

  /**
   * Registers a Rust-side catalog provider (for example one produced by {@link
   * org.apache.arrow.datafusion.providers.flight.FlightSqlFederatedCatalog#builder()}).
   *
   * <p>The catalog handle remains owned by the caller — registering it does not transfer
   * ownership, and callers must still {@link RustCatalogProvider#close() close} the handle when
   * done.
   *
   * {@snippet :
   * try (FlightSqlFederatedCatalog remote = FlightSqlFederatedCatalog.builder()
   *         .endpoint("http://localhost:32010")
   *         .computeContext("remote1")
   *         .table("users")
   *         .build()) {
   *     ctx.registerCatalog("remote1", remote);
   *     ctx.sql("SELECT * FROM remote1.public.users").show();
   * }
   * }
   *
   * @param name the catalog name used in SQL
   * @param catalog a Rust-backed catalog provider
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_catalog">Rust
   *     DataFusion: SessionContext::register_catalog</a>
   */
  public void registerCatalog(String name, RustCatalogProvider catalog) {
    checkNotClosed();
    bridge.registerRustCatalog(name, catalog);
  }

  /**
   * Registers a variable provider for resolving variable references in SQL queries.
   *
   * <p>User-defined variables use the {@code @} prefix and system variables use {@code @@}. Once
   * registered, queries can reference these variables directly:
   *
   * {@snippet :
   * VarProvider provider = new VarProvider() {
   *     @Override
   *     public ScalarValue getValue(List<String> varNames) {
   *         return new ScalarValue.Utf8("Alice");
   *     }
   *     @Override
   *     public Optional<ArrowType> getType(List<String> varNames) {
   *         return Optional.of(ArrowType.Utf8.INSTANCE);
   *     }
   * };
   * ctx.registerVariable(VarType.USER_DEFINED, provider, allocator);
   * DataFrame df = ctx.sql("SELECT @name");
   * }
   *
   * @param type the variable type (system or user-defined)
   * @param provider the variable provider implementation
   * @param allocator the buffer allocator to use for Arrow data transfers
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_variable">Rust
   *     DataFusion: SessionContext::register_variable</a>
   */
  public void registerVariable(VarType type, VarProvider provider, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerVariable(type, provider, allocator);
  }

  /**
   * Registers a scalar UDF with the session context.
   *
   * <p>Once registered, the UDF can be used in SQL queries by name. For example:
   *
   * {@snippet :
   * ScalarUDF pow = Functions.createUdf("pow", Volatility.IMMUTABLE,
   *     List.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
   *             new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
   *     new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
   *     (args, numRows, alloc) -> { return resultVector; });
   * ctx.registerUdf(pow, allocator);
   * DataFrame result = ctx.sql("SELECT pow(a, b) FROM t");
   * }
   *
   * @param udf The scalar UDF implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_udf">Rust
   *     DataFusion: SessionContext::register_udf</a>
   */
  public void registerUdf(ScalarUDF udf, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerUdf(udf, allocator);
  }

  /**
   * Registers a listing table with the session context.
   *
   * <p>A listing table scans files in a directory using a user-provided {@link FileFormat}. For
   * example:
   *
   * {@snippet :
   * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
   * ListingOptions options = ListingOptions.builder(myFormat).build();
   * ListingTableConfig config = new ListingTableConfig(url)
   *     .withListingOptions(options)
   *     .withSchema(mySchema);
   * ListingTable table = new ListingTable(config);
   * ctx.registerListingTable("my_table", table, allocator);
   * }
   *
   * @param name The table name
   * @param table The listing table configuration
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_listing_table">Rust
   *     DataFusion: SessionContext::register_listing_table</a>
   */
  public void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerListingTable(name, table, allocator);
  }

  /**
   * Registers a CSV file or directory as a named table.
   *
   * {@snippet :
   * ctx.registerCsv("sales", "/data/sales.csv", CsvReadOptions.defaults(), allocator);
   * DataFrame df = ctx.sql("SELECT * FROM sales");
   * }
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options CSV read options
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_csv">Rust
   *     DataFusion: SessionContext::register_csv</a>
   */
  public void registerCsv(
      String name, String path, CsvReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerCsv(name, path, options, allocator);
  }

  /**
   * Registers a Parquet file or directory as a named table.
   *
   * {@snippet :
   * ctx.registerParquet("events", "/data/events.parquet", ParquetReadOptions.defaults(), allocator);
   * DataFrame df = ctx.sql("SELECT * FROM events");
   * }
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options Parquet read options
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_parquet">Rust
   *     DataFusion: SessionContext::register_parquet</a>
   */
  public void registerParquet(
      String name, String path, ParquetReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerParquet(name, path, options, allocator);
  }

  /**
   * Registers a JSON file or directory as a named table.
   *
   * {@snippet :
   * ctx.registerJson("logs", "/data/logs.json", NdJsonReadOptions.defaults(), allocator);
   * DataFrame df = ctx.sql("SELECT * FROM logs");
   * }
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options JSON read options
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_json">Rust
   *     DataFusion: SessionContext::register_json</a>
   */
  public void registerJson(
      String name, String path, NdJsonReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerJson(name, path, options, allocator);
  }

  /**
   * Registers an Arrow IPC file or directory as a named table.
   *
   * {@snippet :
   * ctx.registerArrow("events", "/data/events.arrow", ArrowReadOptions.builder().build(), allocator);
   * DataFrame df = ctx.sql("SELECT * FROM events");
   * }
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options Arrow read options
   * @param allocator The buffer allocator
   * @throws DataFusionError if registration fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.register_arrow">Rust
   *     DataFusion: SessionContext::register_arrow</a>
   */
  public void registerArrow(
      String name, String path, ArrowReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerArrow(name, path, options, allocator);
  }

  /**
   * Returns whether a table with the given name exists.
   *
   * {@snippet :
   * boolean exists = ctx.tableExist("my_table");
   * }
   *
   * @param name The table name
   * @return true if the table exists, false otherwise
   * @throws DataFusionError if the existence check fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.table_exist">Rust
   *     DataFusion: SessionContext::table_exist</a>
   */
  public boolean tableExist(String name) {
    checkNotClosed();
    return bridge.tableExist(name);
  }

  /**
   * Creates a DataFrame for a registered table, if it exists.
   *
   * <p>Equivalent to Rust's {@code ctx.table("name")}.
   *
   * {@snippet :
   * Optional<DataFrame> df = ctx.table("my_table");
   * df.ifPresent(DataFrame::show);
   * }
   *
   * @param name The table name
   * @return An Optional containing a DataFrame for the specified table, or empty if not found
   * @throws DataFusionError if there is an error other than the table not being found
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.table">Rust
   *     DataFusion: SessionContext::table</a>
   */
  public Optional<DataFrame> table(String name) {
    checkNotClosed();
    return bridge.table(name).map(DataFrame::new);
  }

  /**
   * Reads a Parquet file/directory into a DataFrame.
   *
   * {@snippet :
   * DataFrame df = ctx.readParquet("/data/events.parquet");
   * df.show();
   * }
   *
   * @param path The file or directory path
   * @return A DataFrame for the Parquet data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_parquet">Rust
   *     DataFusion: SessionContext::read_parquet</a>
   */
  public DataFrame readParquet(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readParquet(path));
  }

  /**
   * Reads a Parquet file/directory into a DataFrame with options. See {@link #readParquet(String)}
   * for an example.
   *
   * @param path The file or directory path
   * @param options Parquet read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the Parquet data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_parquet">Rust
   *     DataFusion: SessionContext::read_parquet</a>
   */
  public DataFrame readParquet(String path, ParquetReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readParquet(path, options, allocator));
  }

  /**
   * Reads a CSV file/directory into a DataFrame.
   *
   * {@snippet :
   * DataFrame df = ctx.readCsv("/data/sales.csv");
   * df.show();
   * }
   *
   * @param path The file or directory path
   * @return A DataFrame for the CSV data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_csv">Rust
   *     DataFusion: SessionContext::read_csv</a>
   */
  public DataFrame readCsv(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readCsv(path));
  }

  /**
   * Reads a CSV file/directory into a DataFrame with options. See {@link #readCsv(String)} for an
   * example.
   *
   * @param path The file or directory path
   * @param options CSV read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the CSV data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_csv">Rust
   *     DataFusion: SessionContext::read_csv</a>
   */
  public DataFrame readCsv(String path, CsvReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readCsv(path, options, allocator));
  }

  /**
   * Reads a JSON file/directory into a DataFrame.
   *
   * {@snippet :
   * DataFrame df = ctx.readJson("/data/logs.json");
   * df.show();
   * }
   *
   * @param path The file or directory path
   * @return A DataFrame for the JSON data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_json">Rust
   *     DataFusion: SessionContext::read_json</a>
   */
  public DataFrame readJson(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readJson(path));
  }

  /**
   * Reads a JSON file/directory into a DataFrame with options. See {@link #readJson(String)} for an
   * example.
   *
   * @param path The file or directory path
   * @param options JSON read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the JSON data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_json">Rust
   *     DataFusion: SessionContext::read_json</a>
   */
  public DataFrame readJson(String path, NdJsonReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readJson(path, options, allocator));
  }

  /**
   * Reads an Arrow IPC file/directory into a DataFrame.
   *
   * {@snippet :
   * DataFrame df = ctx.readArrow("/data/events.arrow");
   * df.show();
   * }
   *
   * @param path The file or directory path
   * @return A DataFrame for the Arrow data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_arrow">Rust
   *     DataFusion: SessionContext::read_arrow</a>
   */
  public DataFrame readArrow(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readArrow(path));
  }

  /**
   * Reads an Arrow IPC file/directory into a DataFrame with options. See {@link
   * #readArrow(String)} for an example.
   *
   * @param path The file or directory path
   * @param options Arrow read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the Arrow data
   * @throws DataFusionError if reading fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.read_arrow">Rust
   *     DataFusion: SessionContext::read_arrow</a>
   */
  public DataFrame readArrow(String path, ArrowReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readArrow(path, options, allocator));
  }

  /**
   * Returns the names of all registered catalogs.
   *
   * {@snippet :
   * List<String> names = ctx.catalogNames();
   * }
   *
   * @return list of catalog names
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.catalog_names">Rust
   *     DataFusion: SessionContext::catalog_names</a>
   */
  public List<String> catalogNames() {
    checkNotClosed();
    return bridge.catalogNames();
  }

  /**
   * Returns a catalog provider for the named catalog, if it exists.
   *
   * <p>The returned provider supports introspection (listing schemas and tables) but does not
   * support retrieving native {@link TableProvider} instances. Use {@link #table(String)} to query
   * native tables.
   *
   * {@snippet :
   * Optional<CatalogProvider> catalog = ctx.catalog("datafusion");
   * }
   *
   * @param name The catalog name
   * @return An Optional containing the catalog provider, or empty if not found
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.catalog">Rust
   *     DataFusion: SessionContext::catalog</a>
   */
  public Optional<CatalogProvider> catalog(String name) {
    checkNotClosed();
    List<String> names = bridge.catalogNames();
    if (names.contains(name)) {
      return Optional.of(new NativeCatalogProvider(bridge, name));
    }
    return Optional.empty();
  }

  /**
   * Creates a snapshot of this session's state.
   *
   * <p>The returned SessionState owns its own Tokio runtime and can outlive this SessionContext.
   *
   * {@snippet :
   * SessionState state = ctx.state();
   * }
   *
   * @return a new SessionState
   * @throws DataFusionError if the state cannot be created
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.state">Rust
   *     DataFusion: SessionContext::state</a>
   */
  public SessionState state() {
    checkNotClosed();
    return new SessionState(bridge.state());
  }

  /**
   * Parses a SQL expression string into an {@link Expr}.
   *
   * <p>The expression is parsed against the given schema, which defines the available column names
   * and types. For example:
   *
   * {@snippet :
   * Schema schema = new Schema(List.of(
   *     Field.nullable("a", new ArrowType.Int(32, true)),
   *     Field.nullable("b", new ArrowType.Int(32, true))));
   * Expr expr = ctx.parseSqlExpr("a + b > 10", schema);
   * }
   *
   * @param sql the SQL expression string to parse
   * @param schema the Arrow schema describing the available columns
   * @return the parsed expression
   * @throws DataFusionError if the expression cannot be parsed
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.parse_sql_expr">Rust
   *     DataFusion: SessionContext::parse_sql_expr</a>
   */
  public Expr parseSqlExpr(String sql, Schema schema) {
    checkNotClosed();
    return bridge.parseSqlExpr(sql, schema);
  }

  /**
   * Returns the unique identifier for this session.
   *
   * {@snippet :
   * String id = ctx.sessionId();
   * }
   *
   * @return the session ID string
   * @throws DataFusionError if the call fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.session_id">Rust
   *     DataFusion: SessionContext::session_id</a>
   */
  public String sessionId() {
    checkNotClosed();
    return bridge.sessionId();
  }

  /**
   * Returns the time this session was created.
   *
   * {@snippet :
   * Instant startTime = ctx.sessionStartTime();
   * }
   *
   * @return the session start time as an {@link Instant}
   * @throws DataFusionError if the call fails
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/execution/context/struct.SessionContext.html#method.session_start_time">Rust
   *     DataFusion: SessionContext::session_start_time</a>
   */
  public Instant sessionStartTime() {
    checkNotClosed();
    return Instant.ofEpochMilli(bridge.sessionStartTimeMillis());
  }

  /** Returns the underlying bridge. Internal use only. */
  public SessionContextBridge bridge() {
    checkNotClosed();
    return bridge;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("SessionContext has been closed");
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      bridge.close();
    }
  }
}
