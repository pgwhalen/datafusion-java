package org.apache.arrow.datafusion;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A session context for executing DataFusion queries using the FFM API.
 *
 * <p>This class wraps a native DataFusion SessionContext and provides methods for registering
 * tables and executing SQL queries.
 */
public class SessionContext implements AutoCloseable {
  private final SessionContextBridge bridge;
  private volatile boolean closed = false;

  /** Creates a new session context with default configuration. */
  public SessionContext() {
    this(SessionConfig.defaults());
  }

  /**
   * Creates a new session context with the specified configuration.
   *
   * @param config the session configuration
   */
  public SessionContext(SessionConfig config) {
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
   * @return a new SessionContext
   */
  public static SessionContext create() {
    return new SessionContext();
  }

  /**
   * Creates a new session context with the specified configuration.
   *
   * <p>Equivalent to Rust's {@code SessionContext::new_with_config(config)}.
   *
   * @param config the session configuration
   * @return a new SessionContext
   */
  public static SessionContext newWithConfig(SessionConfig config) {
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
   * <pre>{@code
   * RuntimeEnv rt = RuntimeEnvBuilder.builder()
   *     .withMemoryLimit(50_000_000, 1.0)
   *     .build();
   * SessionConfig config = SessionConfig.defaults();
   * try (SessionContext ctx = SessionContext.newWithConfigRt(config, rt)) {
   *     DataFrame df = ctx.sql("SELECT 1 + 1");
   *     // ...
   * }
   * rt.close();
   * }</pre>
   *
   * @param config the session configuration
   * @param runtimeEnv the runtime environment (e.g., with memory pool configuration)
   * @return a new SessionContext
   * @throws DataFusionException if context creation fails
   */
  public static SessionContext newWithConfigRt(SessionConfig config, RuntimeEnv runtimeEnv) {
    return new SessionContext(new SessionContextBridge(config, runtimeEnv.bridge.dfRuntimeEnv()));
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context.
   *
   * <p>This is equivalent to Rust's {@code register_batch}: it takes Arrow data directly and wraps
   * it in a MemTable.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerBatch(String name, VectorSchemaRoot root, BufferAllocator allocator) {
    registerBatch(name, root, null, allocator);
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context with dictionary support.
   *
   * <p>This is equivalent to Rust's {@code register_batch}: it takes Arrow data directly and wraps
   * it in a MemTable.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param provider The DictionaryProvider for dictionary-encoded columns (may be null)
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
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
   * @param name The table name
   * @param provider The table provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerTable(String name, TableProvider provider, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerTableProvider(name, provider, allocator);
  }

  /**
   * Deregisters a table by name.
   *
   * @param name The table name
   * @return true if a table was previously registered with this name
   * @throws DataFusionException if deregistration fails
   */
  public boolean deregisterTable(String name) {
    checkNotClosed();
    return bridge.deregisterTable(name);
  }

  /**
   * Executes a SQL query and returns a DataFrame.
   *
   * @param query The SQL query to execute
   * @return A DataFrame representing the query result
   * @throws DataFusionException if query execution fails
   */
  public DataFrame sql(String query) {
    checkNotClosed();
    return new DataFrame(bridge.sql(query));
  }

  /**
   * Registers a catalog provider with the session context.
   *
   * <p>The catalog can be accessed in SQL queries using the catalog name. For example, if you
   * register a catalog named "my_catalog" with a schema "my_schema" containing a table "my_table",
   * you can query it with:
   *
   * <pre>{@code
   * SELECT * FROM my_catalog.my_schema.my_table
   * }</pre>
   *
   * @param name The catalog name
   * @param catalog The catalog provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerCatalog(String name, CatalogProvider catalog, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerCatalog(name, catalog, allocator);
  }

  /**
   * Registers a scalar UDF with the session context.
   *
   * <p>Once registered, the UDF can be used in SQL queries by name. For example:
   *
   * <pre>{@code
   * ScalarUdf pow = ScalarUdf.simple("pow", Volatility.IMMUTABLE,
   *     List.of(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
   *             new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
   *     new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
   *     (args, numRows, allocator) -> { ... });
   * ctx.registerUdf(pow, allocator);
   * DataFrame result = ctx.sql("SELECT pow(a, b) FROM t");
   * }</pre>
   *
   * @param udf The scalar UDF implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerUdf(ScalarUdf udf, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerUdf(udf, allocator);
  }

  /**
   * Registers a listing table with the session context.
   *
   * <p>A listing table scans files in a directory using a user-provided {@link FileFormat}. For
   * example:
   *
   * <pre>{@code
   * ListingTableUrl url = ListingTableUrl.parse("/path/to/data/");
   * ListingOptions options = ListingOptions.builder(myFormat).build();
   * ListingTable table = ListingTable.builder(url)
   *     .withListingOptions(options)
   *     .withSchema(mySchema)
   *     .build();
   * ctx.registerListingTable("my_table", table, allocator);
   * }</pre>
   *
   * @param name The table name
   * @param table The listing table configuration
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerListingTable(name, table, allocator);
  }

  /**
   * Registers a CSV file or directory as a named table.
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options CSV read options
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerCsv(
      String name, String path, CsvReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerCsv(name, path, options, allocator);
  }

  /**
   * Registers a Parquet file or directory as a named table.
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options Parquet read options
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerParquet(
      String name, String path, ParquetReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerParquet(name, path, options, allocator);
  }

  /**
   * Registers a JSON file or directory as a named table.
   *
   * @param name The table name
   * @param path The file or directory path
   * @param options JSON read options
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerJson(
      String name, String path, NdJsonReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    bridge.registerJson(name, path, options, allocator);
  }

  /**
   * Returns whether a table with the given name exists.
   *
   * @param name The table name
   * @return true if the table exists, false otherwise
   * @throws DataFusionException if the existence check fails
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
   * @param name The table name
   * @return An Optional containing a DataFrame for the specified table, or empty if not found
   * @throws DataFusionException if there is an error other than the table not being found
   */
  public Optional<DataFrame> table(String name) {
    checkNotClosed();
    return bridge.table(name).map(DataFrame::new);
  }

  /**
   * Reads a Parquet file/directory into a DataFrame.
   *
   * @param path The file or directory path
   * @return A DataFrame for the Parquet data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readParquet(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readParquet(path));
  }

  /**
   * Reads a Parquet file/directory into a DataFrame with options.
   *
   * @param path The file or directory path
   * @param options Parquet read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the Parquet data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readParquet(String path, ParquetReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readParquet(path, options, allocator));
  }

  /**
   * Reads a CSV file/directory into a DataFrame.
   *
   * @param path The file or directory path
   * @return A DataFrame for the CSV data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readCsv(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readCsv(path));
  }

  /**
   * Reads a CSV file/directory into a DataFrame with options.
   *
   * @param path The file or directory path
   * @param options CSV read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the CSV data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readCsv(String path, CsvReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readCsv(path, options, allocator));
  }

  /**
   * Reads a JSON file/directory into a DataFrame.
   *
   * @param path The file or directory path
   * @return A DataFrame for the JSON data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readJson(String path) {
    checkNotClosed();
    return new DataFrame(bridge.readJson(path));
  }

  /**
   * Reads a JSON file/directory into a DataFrame with options.
   *
   * @param path The file or directory path
   * @param options JSON read options
   * @param allocator The buffer allocator (needed if schema is specified in options)
   * @return A DataFrame for the JSON data
   * @throws DataFusionException if reading fails
   */
  public DataFrame readJson(String path, NdJsonReadOptions options, BufferAllocator allocator) {
    checkNotClosed();
    return new DataFrame(bridge.readJson(path, options, allocator));
  }

  /**
   * Returns the names of all registered catalogs.
   *
   * @return list of catalog names
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
   * @param name The catalog name
   * @return An Optional containing the catalog provider, or empty if not found
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
   * @return a new SessionState
   * @throws DataFusionException if the state cannot be created
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
   * <pre>{@code
   * Schema schema = new Schema(List.of(
   *     Field.nullable("a", new ArrowType.Int(32, true)),
   *     Field.nullable("b", new ArrowType.Int(32, true))));
   * Expr expr = ctx.parseSqlExpr("a + b > 10", schema);
   * }</pre>
   *
   * @param sql the SQL expression string to parse
   * @param schema the Arrow schema describing the available columns
   * @return the parsed expression
   * @throws DataFusionException if the expression cannot be parsed
   */
  public Expr parseSqlExpr(String sql, Schema schema) {
    checkNotClosed();
    return bridge.parseSqlExpr(sql, schema);
  }

  /**
   * Returns the unique identifier for this session.
   *
   * @return the session ID string
   * @throws DataFusionException if the call fails
   */
  public String sessionId() {
    checkNotClosed();
    return bridge.sessionId();
  }

  /**
   * Returns the time this session was created.
   *
   * @return the session start time as an {@link Instant}
   * @throws DataFusionException if the call fails
   */
  public Instant sessionStartTime() {
    checkNotClosed();
    return Instant.ofEpochMilli(bridge.sessionStartTimeMillis());
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
