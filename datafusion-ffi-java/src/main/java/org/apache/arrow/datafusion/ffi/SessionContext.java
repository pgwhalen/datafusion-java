package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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

  /** Creates a new session context with a new Tokio runtime. */
  public SessionContext() {
    try {
      runtime = (MemorySegment) DataFusionBindings.RUNTIME_CREATE.invokeExact();
      if (runtime.equals(MemorySegment.NULL)) {
        throw new DataFusionException("Failed to create Tokio runtime");
      }
      context = (MemorySegment) DataFusionBindings.CONTEXT_CREATE.invokeExact();
      if (context.equals(MemorySegment.NULL)) {
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);
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

      MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

      // Create null-terminated string for table name
      MemorySegment nameSegment = arena.allocateUtf8String(name);

      // Get memory addresses from Arrow C Data structures
      MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());
      MemorySegment arrayAddr = MemorySegment.ofAddress(ffiArray.memoryAddress());

      int result =
          (int)
              DataFusionBindings.CONTEXT_REGISTER_RECORD_BATCH.invokeExact(
                  context, nameSegment, schemaAddr, arrayAddr, errorOut);

      NativeUtil.checkResult(result, errorOut, "Register table '" + name + "'");

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
      MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);
      MemorySegment querySegment = arena.allocateUtf8String(query);

      MemorySegment dataframe =
          (MemorySegment)
              DataFusionBindings.CONTEXT_SQL.invokeExact(runtime, context, querySegment, errorOut);

      NativeUtil.checkPointer(dataframe, errorOut, "SQL execution");

      logger.debug("Executed SQL query, got DataFrame: {}", dataframe);
      return new DataFrame(runtime, dataframe);
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to execute SQL", e);
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
        DataFusionBindings.CONTEXT_DESTROY.invokeExact(context);
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);
        logger.debug("Closed SessionContext");
      } catch (Throwable e) {
        logger.error("Error closing SessionContext", e);
      }
    }
  }
}
