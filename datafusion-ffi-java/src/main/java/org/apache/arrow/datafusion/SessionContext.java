package org.apache.arrow.datafusion;

import java.time.Instant;
import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * A session context for executing DataFusion queries using the FFM API.
 *
 * <p>This class wraps a native DataFusion SessionContext and provides methods for registering
 * tables and executing SQL queries.
 */
public class SessionContext implements AutoCloseable {
  private final SessionContextFfi ffi;
  private final SessionConfig config;
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
    this.config = config;
    this.ffi = new SessionContextFfi(config);
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerTable(String name, VectorSchemaRoot root, BufferAllocator allocator) {
    registerTable(name, root, null, allocator);
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context with dictionary support.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param provider The DictionaryProvider for dictionary-encoded columns (may be null)
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  public void registerTable(
      String name, VectorSchemaRoot root, DictionaryProvider provider, BufferAllocator allocator) {
    checkNotClosed();
    ffi.registerTable(name, root, provider, allocator);
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
    return new DataFrame(ffi.sql(query));
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
    ffi.registerCatalog(name, catalog, allocator);
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
    ffi.registerUdf(udf, allocator);
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
    ffi.registerListingTable(name, table, allocator);
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
    return new SessionState(ffi.state());
  }

  /**
   * Returns the unique identifier for this session.
   *
   * @return the session ID string
   * @throws DataFusionException if the call fails
   */
  public String sessionId() {
    checkNotClosed();
    return ffi.sessionId();
  }

  /**
   * Returns the time this session was created.
   *
   * @return the session start time as an {@link Instant}
   * @throws DataFusionException if the call fails
   */
  public Instant sessionStartTime() {
    checkNotClosed();
    return Instant.ofEpochMilli(ffi.sessionStartTimeMillis());
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
      ffi.close();
    }
  }
}
