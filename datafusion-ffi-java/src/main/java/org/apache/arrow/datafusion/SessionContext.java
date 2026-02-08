package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.datafusion.ffi.DataFusionBindings;
import org.apache.arrow.datafusion.ffi.NativeUtil;
import org.apache.arrow.datafusion.ffi.SessionContextFfi;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session context for executing DataFusion queries using the FFM API.
 *
 * <p>This class wraps a native DataFusion SessionContext and provides methods for registering
 * tables and executing SQL queries.
 */
public class SessionContext implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionContext.class);

  private final MemorySegment runtime;
  private final MemorySegment context;
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
    try {
      runtime = (MemorySegment) DataFusionBindings.RUNTIME_CREATE.invokeExact();
      if (runtime.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to create Tokio runtime");
      }
      if (config.hasOptions()) {
        context = createWithConfig(config);
      } else {
        context = (MemorySegment) DataFusionBindings.CONTEXT_CREATE.invokeExact();
      }
      if (context.equals(MemorySegment.NULL)) {
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);
        throw new DataFusionException("Failed to create SessionContext");
      }
      ffi = new SessionContextFfi(context, runtime, config);
      logger.debug("Created SessionContext: runtime={}, context={}", runtime, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create SessionContext", e);
    }
  }

  private static MemorySegment createWithConfig(SessionConfig config) {
    Map<String, String> options = config.toOptionsMap();

    try (Arena arena = Arena.ofConfined()) {
      int size = options.size();

      // Allocate parallel arrays of C string pointers
      MemorySegment keys = arena.allocate(ValueLayout.ADDRESS, size);
      MemorySegment values = arena.allocate(ValueLayout.ADDRESS, size);

      int i = 0;
      for (Map.Entry<String, String> entry : options.entrySet()) {
        keys.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(entry.getKey()));
        values.setAtIndex(ValueLayout.ADDRESS, i, arena.allocateFrom(entry.getValue()));
        i++;
      }

      return NativeUtil.callForPointer(
          arena,
          "Create SessionContext with config",
          errorOut ->
              (MemorySegment)
                  DataFusionBindings.CONTEXT_CREATE_WITH_CONFIG.invokeExact(
                      keys, values, (long) size, errorOut));
    }
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

    try (Arena arena = Arena.ofConfined();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

      // Export the VectorSchemaRoot to Arrow C Data Interface
      Data.exportVectorSchemaRoot(allocator, root, provider, ffiArray, ffiSchema);

      // Create null-terminated string for table name
      MemorySegment nameSegment = arena.allocateFrom(name);

      // Get memory addresses from Arrow C Data structures
      MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());
      MemorySegment arrayAddr = MemorySegment.ofAddress(ffiArray.memoryAddress());

      NativeUtil.call(
          arena,
          "Register table '" + name + "'",
          errorOut ->
              (int)
                  DataFusionBindings.CONTEXT_REGISTER_RECORD_BATCH.invokeExact(
                      context, nameSegment, schemaAddr, arrayAddr, errorOut));

      logger.debug("Registered table '{}' with {} rows", name, root.getRowCount());
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register table", e);
    }
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

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment querySegment = arena.allocateFrom(query);

      MemorySegment dataframe =
          NativeUtil.callForPointer(
              arena,
              "SQL execution",
              errorOut ->
                  (MemorySegment)
                      DataFusionBindings.CONTEXT_SQL.invokeExact(
                          runtime, context, querySegment, errorOut));

      logger.debug("Executed SQL query, got DataFrame: {}", dataframe);
      return new DataFrame(runtime, dataframe);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to execute SQL", e);
    }
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

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment statePtr =
          NativeUtil.callForPointer(
              arena,
              "Get session state",
              errorOut ->
                  (MemorySegment) DataFusionBindings.CONTEXT_STATE.invokeExact(context, errorOut));

      return new SessionState(statePtr);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get session state", e);
    }
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
      try {
        // Close handles first (but NOT arenas - upcalls may still be invoked during
        // context destruction)
        ffi.closeCatalogHandles();
        ffi.closeFormatHandles();

        DataFusionBindings.CONTEXT_DESTROY.invokeExact(context);
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);

        // Now it's safe to close arenas (no more upcalls possible)
        ffi.closeArenas();

        logger.debug("Closed SessionContext");
      } catch (Throwable e) {
        logger.error("Error closing SessionContext", e);
      }
    }
  }
}
