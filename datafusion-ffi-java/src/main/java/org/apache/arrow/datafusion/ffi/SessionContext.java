package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
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
  private volatile boolean closed = false;

  // Shared arena for catalog providers (needs to live as long as the context)
  private final Arena catalogArena;
  // Keep references to prevent GC
  private final List<CatalogProviderHandle> catalogHandles = new ArrayList<>();
  private BufferAllocator catalogAllocator;

  /** Creates a new session context with a new Tokio runtime. */
  public SessionContext() {
    this.catalogArena = Arena.ofShared();
    try {
      runtime = (MemorySegment) DataFusionBindings.RUNTIME_CREATE.invokeExact();
      if (runtime.equals(MemorySegment.NULL)) {
        catalogArena.close();
        throw new DataFusionException("Failed to create Tokio runtime");
      }
      context = (MemorySegment) DataFusionBindings.CONTEXT_CREATE.invokeExact();
      if (context.equals(MemorySegment.NULL)) {
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);
        catalogArena.close();
        throw new DataFusionException("Failed to create SessionContext");
      }
      logger.debug("Created SessionContext: runtime={}, context={}", runtime, context);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create SessionContext", e);
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
      MemorySegment nameSegment = arena.allocateUtf8String(name);

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
      MemorySegment querySegment = arena.allocateUtf8String(query);

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

    // Store allocator for use in callbacks
    if (this.catalogAllocator == null) {
      this.catalogAllocator = allocator;
    }

    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the catalog (uses the shared catalog arena)
      CatalogProviderHandle handle = new CatalogProviderHandle(catalog, allocator, catalogArena);
      catalogHandles.add(handle);

      MemorySegment nameSegment = arena.allocateUtf8String(name);
      MemorySegment callbacks = handle.getCallbackStruct();

      NativeUtil.call(
          arena,
          "Register catalog '" + name + "'",
          errorOut ->
              (int)
                  DataFusionBindings.CONTEXT_REGISTER_CATALOG.invokeExact(
                      context, nameSegment, callbacks, errorOut));

      logger.debug("Registered catalog '{}'", name);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register catalog", e);
    }
  }

  /**
   * Gets the native runtime pointer for use by other FFI classes.
   *
   * @return The runtime memory segment
   */
  MemorySegment getRuntime() {
    return runtime;
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
        // Close catalog handles first
        for (CatalogProviderHandle handle : catalogHandles) {
          try {
            handle.close();
          } catch (Exception e) {
            logger.warn("Error closing catalog handle", e);
          }
        }
        catalogHandles.clear();

        DataFusionBindings.CONTEXT_DESTROY.invokeExact(context);
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);

        // Close the catalog arena after context is destroyed
        catalogArena.close();

        logger.debug("Closed SessionContext");
      } catch (Throwable e) {
        logger.error("Error closing SessionContext", e);
      }
    }
  }
}
