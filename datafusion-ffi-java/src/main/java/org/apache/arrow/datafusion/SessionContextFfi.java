package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.config.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for SessionContext.
 *
 * <p>This class manages all FFI-level operations for SessionContext, including runtime/context
 * creation, table registration, SQL execution, and lifecycle management of native resources. It
 * exists in the ffi package to access package-private classes like {@link CatalogProviderHandle}.
 */
final class SessionContextFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionContextFfi.class);

  private static final MethodHandle RUNTIME_CREATE =
      NativeUtil.downcall("datafusion_runtime_create", FunctionDescriptor.of(ValueLayout.ADDRESS));

  private static final MethodHandle RUNTIME_DESTROY =
      NativeUtil.downcall(
          "datafusion_runtime_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle CONTEXT_CREATE =
      NativeUtil.downcall("datafusion_context_create", FunctionDescriptor.of(ValueLayout.ADDRESS));

  private static final MethodHandle CONTEXT_CREATE_WITH_CONFIG =
      NativeUtil.downcall(
          "datafusion_context_create_with_config",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("keys"),
              ValueLayout.ADDRESS.withName("values"),
              ValueLayout.JAVA_LONG.withName("len"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_DESTROY =
      NativeUtil.downcall(
          "datafusion_context_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle CONTEXT_REGISTER_RECORD_BATCH =
      NativeUtil.downcall(
          "datafusion_context_register_record_batch",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("name"),
              ValueLayout.ADDRESS.withName("schema"),
              ValueLayout.ADDRESS.withName("array"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_SQL =
      NativeUtil.downcall(
          "datafusion_context_sql",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("sql"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_STATE =
      NativeUtil.downcall(
          "datafusion_context_state",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_REGISTER_CATALOG =
      NativeUtil.downcall(
          "datafusion_context_register_catalog",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("name"),
              ValueLayout.ADDRESS.withName("callbacks"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_SESSION_ID =
      NativeUtil.downcall(
          "datafusion_context_session_id",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_SESSION_START_TIME =
      NativeUtil.downcall(
          "datafusion_context_session_start_time",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("millis_out"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_REGISTER_UDF =
      NativeUtil.downcall(
          "datafusion_context_register_udf",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("ffi_udf"),
              ValueLayout.ADDRESS.withName("error_out")));

  private static final MethodHandle CONTEXT_REGISTER_LISTING_TABLE =
      NativeUtil.downcall(
          "datafusion_context_register_listing_table",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS.withName("ctx"),
              ValueLayout.ADDRESS.withName("rt"),
              ValueLayout.ADDRESS.withName("name"),
              ValueLayout.ADDRESS.withName("urls"), // pointer to array of C string pointers
              ValueLayout.JAVA_LONG.withName("urls_len"),
              ValueLayout.ADDRESS.withName("file_extension"),
              ValueLayout.ADDRESS.withName("schema"), // FFI_ArrowSchema*
              ValueLayout.ADDRESS.withName("format_callbacks"),
              ValueLayout.JAVA_INT.withName("collect_stat"), // i32 in Rust, 0=false, 1=true
              ValueLayout.JAVA_LONG.withName("target_partitions"),
              ValueLayout.ADDRESS.withName("error_out")));

  private final MemorySegment runtime;
  private final MemorySegment context;
  private final SessionConfig config;

  // Shared arena for all registered providers/UDFs (needs to live as long as the context)
  private final Arena sharedArena;
  // Keep references to prevent GC of upcall stubs while Rust holds pointers
  private final List<TraitHandle> handles = new ArrayList<>();

  /**
   * Creates a new SessionContextFfi, including the native runtime and context.
   *
   * @param config The session configuration
   * @throws DataFusionException if runtime or context creation fails
   */
  SessionContextFfi(SessionConfig config) {
    this.config = config;
    try {
      runtime = (MemorySegment) RUNTIME_CREATE.invokeExact();
      if (runtime.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to create Tokio runtime");
      }
      if (config.hasOptions()) {
        context = createWithConfig(config);
      } else {
        context = (MemorySegment) CONTEXT_CREATE.invokeExact();
      }
      if (context.equals(MemorySegment.NULL)) {
        RUNTIME_DESTROY.invokeExact(runtime);
        throw new DataFusionException("Failed to create SessionContext");
      }
      this.sharedArena = Arena.ofShared();
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
                  CONTEXT_CREATE_WITH_CONFIG.invokeExact(keys, values, (long) size, errorOut));
    }
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param provider The DictionaryProvider for dictionary-encoded columns (may be null)
   * @param allocator The buffer allocator
   * @throws DataFusionException if registration fails
   */
  void registerTable(
      String name, VectorSchemaRoot root, DictionaryProvider provider, BufferAllocator allocator) {
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
                  CONTEXT_REGISTER_RECORD_BATCH.invokeExact(
                      context, nameSegment, schemaAddr, arrayAddr, errorOut));

      logger.debug("Registered table '{}' with {} rows", name, root.getRowCount());
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register table", e);
    }
  }

  /**
   * Executes a SQL query and returns a DataFrameFfi.
   *
   * @param query The SQL query to execute
   * @return A DataFrameFfi wrapping the native runtime and dataframe pointers
   * @throws DataFusionException if query execution fails
   */
  DataFrameFfi sql(String query) {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment querySegment = arena.allocateFrom(query);

      MemorySegment dataframe =
          NativeUtil.callForPointer(
              arena,
              "SQL execution",
              errorOut ->
                  (MemorySegment)
                      CONTEXT_SQL.invokeExact(runtime, context, querySegment, errorOut));

      logger.debug("Executed SQL query, got DataFrame: {}", dataframe);
      return new DataFrameFfi(runtime, dataframe);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to execute SQL", e);
    }
  }

  /**
   * Creates a snapshot of this session's state.
   *
   * @return a SessionStateFfi wrapping the native state pointer
   * @throws DataFusionException if the state cannot be created
   */
  SessionStateFfi state() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment statePtr =
          NativeUtil.callForPointer(
              arena,
              "Get session state",
              errorOut -> (MemorySegment) CONTEXT_STATE.invokeExact(context, errorOut));

      return new SessionStateFfi(statePtr);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get session state", e);
    }
  }

  /**
   * Returns the session ID.
   *
   * @return the session ID string
   * @throws DataFusionException if the call fails
   */
  String sessionId() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment ptr =
          NativeUtil.callForPointer(
              arena,
              "Get session ID",
              errorOut -> (MemorySegment) CONTEXT_SESSION_ID.invokeExact(context, errorOut));
      try {
        return NativeUtil.readNullTerminatedString(ptr);
      } finally {
        NativeUtil.FREE_STRING.invokeExact(ptr);
      }
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get session ID", e);
    }
  }

  /**
   * Returns the session start time as epoch milliseconds.
   *
   * @return epoch milliseconds of when the session was created
   * @throws DataFusionException if the call fails
   */
  long sessionStartTimeMillis() {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment millisOut = arena.allocate(ValueLayout.JAVA_LONG);
      NativeUtil.call(
          arena,
          "Get session start time",
          errorOut -> (int) CONTEXT_SESSION_START_TIME.invokeExact(context, millisOut, errorOut));
      return millisOut.get(ValueLayout.JAVA_LONG, 0);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to get session start time", e);
    }
  }

  /**
   * Registers a catalog provider with the session context.
   *
   * @param name The catalog name
   * @param catalog The catalog provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  void registerCatalog(String name, CatalogProvider catalog, BufferAllocator allocator) {
    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the catalog (uses the shared catalog arena)
      CatalogProviderHandle handle =
          new CatalogProviderHandle(catalog, allocator, sharedArena, config.fullStackTrace());
      handles.add(handle);

      MemorySegment nameSegment = arena.allocateFrom(name);
      MemorySegment callbacks = handle.getTraitStruct();

      NativeUtil.call(
          arena,
          "Register catalog '" + name + "'",
          errorOut ->
              (int)
                  CONTEXT_REGISTER_CATALOG.invokeExact(context, nameSegment, callbacks, errorOut));

      logger.debug("Registered catalog '{}'", name);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register catalog", e);
    }
  }

  /**
   * Registers a listing table with the session context.
   *
   * @param name The table name
   * @param table The listing table configuration
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the format (uses the shared listing table arena)
      FileFormatHandle handle =
          new FileFormatHandle(
              table.options().format(),
              table.schema(),
              allocator,
              sharedArena,
              config.fullStackTrace());
      handles.add(handle);

      MemorySegment nameSegment = arena.allocateFrom(name);
      MemorySegment extSegment = arena.allocateFrom(table.options().fileExtension());

      // Build array of URL string pointers
      List<ListingTableUrl> tablePaths = table.tablePaths();
      MemorySegment urlsArray = arena.allocate(ValueLayout.ADDRESS, tablePaths.size());
      for (int i = 0; i < tablePaths.size(); i++) {
        MemorySegment urlSegment = arena.allocateFrom(tablePaths.get(i).url());
        urlsArray.setAtIndex(ValueLayout.ADDRESS, i, urlSegment);
      }

      // Export schema via Arrow C Data Interface
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, table.schema(), null, ffiSchema);
        MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());

        NativeUtil.call(
            arena,
            "Register listing table '" + name + "'",
            errorOut ->
                (int)
                    CONTEXT_REGISTER_LISTING_TABLE.invokeExact(
                        context,
                        runtime,
                        nameSegment,
                        urlsArray,
                        (long) tablePaths.size(),
                        extSegment,
                        schemaAddr,
                        handle.getTraitStruct(),
                        table.options().collectStat() ? 1 : 0,
                        (long) table.options().targetPartitions(),
                        errorOut));
      }

      logger.debug("Registered listing table '{}'", name);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register listing table", e);
    }
  }

  /**
   * Registers a scalar UDF with the session context.
   *
   * @param udf The scalar UDF implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  void registerUdf(ScalarUdf udf, BufferAllocator allocator) {
    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the UDF (uses the shared UDF arena)
      ScalarUdfHandle handle =
          new ScalarUdfHandle(udf, allocator, sharedArena, config.fullStackTrace());
      handles.add(handle);

      MemorySegment udfStruct = handle.getTraitStruct();

      NativeUtil.call(
          arena,
          "Register UDF '" + udf.name() + "'",
          errorOut -> (int) CONTEXT_REGISTER_UDF.invokeExact(context, udfStruct, errorOut));

      logger.debug("Registered UDF '{}'", udf.name());
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to register UDF", e);
    }
  }

  /**
   * Closes the session context and all associated resources.
   *
   * <p>The close order is important:
   *
   * <ol>
   *   <li>Close handles (but NOT arenas - upcalls may still fire during context destruction)
   *   <li>Destroy native context and runtime
   *   <li>Close arenas (now safe - no more upcalls possible)
   * </ol>
   */
  @Override
  public void close() {
    Throwable firstError = null;

    // Close handles first (but NOT the arena - upcalls may still fire during context destruction)
    for (TraitHandle handle : handles) {
      try {
        handle.close();
      } catch (Exception e) {
        if (firstError == null) firstError = e;
        else firstError.addSuppressed(e);
      }
    }
    handles.clear();

    // Destroy native objects
    try {
      CONTEXT_DESTROY.invokeExact(context);
    } catch (Throwable e) {
      if (firstError == null) firstError = e;
      else firstError.addSuppressed(e);
    }
    try {
      RUNTIME_DESTROY.invokeExact(runtime);
    } catch (Throwable e) {
      if (firstError == null) firstError = e;
      else firstError.addSuppressed(e);
    }

    // Now safe to close arena - no more upcalls possible
    try {
      sharedArena.close();
    } catch (Exception e) {
      if (firstError == null) firstError = e;
      else firstError.addSuppressed(e);
    }

    if (firstError != null) {
      throw new DataFusionException("Error closing SessionContext", firstError);
    }
    logger.debug("Closed SessionContext");
  }
}
