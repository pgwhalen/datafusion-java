package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session context for executing DataFusion queries using the FFM API.
 *
 * <p>This class wraps a native DataFusion SessionContext and provides methods for registering
 * tables and executing SQL queries.
 */
public class FfiSessionContext implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(FfiSessionContext.class);

  private final MemorySegment runtime;
  private final MemorySegment context;
  private volatile boolean closed = false;

  /** Creates a new session context with a new Tokio runtime. */
  public FfiSessionContext() {
    try {
      runtime = (MemorySegment) DataFusionBindings.RUNTIME_CREATE.invokeExact();
      if (runtime.equals(MemorySegment.NULL)) {
        throw new RuntimeException("Failed to create Tokio runtime");
      }
      context = (MemorySegment) DataFusionBindings.CONTEXT_CREATE.invokeExact();
      if (context.equals(MemorySegment.NULL)) {
        DataFusionBindings.RUNTIME_DESTROY.invokeExact(runtime);
        throw new RuntimeException("Failed to create SessionContext");
      }
      logger.debug("Created FfiSessionContext: runtime={}, context={}", runtime, context);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to create SessionContext", e);
    }
  }

  /**
   * Registers a VectorSchemaRoot as a table in the session context.
   *
   * @param name The table name
   * @param root The VectorSchemaRoot containing the data
   * @param allocator The buffer allocator
   * @throws RuntimeException if registration fails
   */
  public void registerTable(String name, VectorSchemaRoot root, BufferAllocator allocator) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

      // Export the VectorSchemaRoot to Arrow C Data Interface
      Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

      // Allocate error output pointer
      MemorySegment errorOut = arena.allocate(ValueLayout.ADDRESS);
      errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

      // Create null-terminated string for table name
      MemorySegment nameSegment = arena.allocateUtf8String(name);

      // Get memory addresses from Arrow C Data structures
      MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());
      MemorySegment arrayAddr = MemorySegment.ofAddress(ffiArray.memoryAddress());

      int result =
          (int)
              DataFusionBindings.CONTEXT_REGISTER_RECORD_BATCH.invokeExact(
                  context, nameSegment, schemaAddr, arrayAddr, errorOut);

      if (result != 0) {
        MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
        if (!errorPtr.equals(MemorySegment.NULL)) {
          String errorMessage = errorPtr.reinterpret(1024).getUtf8String(0);
          DataFusionBindings.FREE_STRING.invokeExact(errorPtr);
          throw new RuntimeException("Failed to register table: " + errorMessage);
        }
        throw new RuntimeException("Failed to register table: unknown error");
      }

      logger.debug("Registered table '{}' with {} rows", name, root.getRowCount());
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to register table", e);
    }
  }

  /**
   * Executes a SQL query and returns a DataFrame.
   *
   * @param query The SQL query to execute
   * @return A FfiDataFrame representing the query result
   * @throws RuntimeException if query execution fails
   */
  public FfiDataFrame sql(String query) {
    checkNotClosed();

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment errorOut = arena.allocate(ValueLayout.ADDRESS);
      errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

      MemorySegment querySegment = arena.allocateUtf8String(query);

      MemorySegment dataframe =
          (MemorySegment)
              DataFusionBindings.CONTEXT_SQL.invokeExact(runtime, context, querySegment, errorOut);

      if (dataframe.equals(MemorySegment.NULL)) {
        MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
        if (!errorPtr.equals(MemorySegment.NULL)) {
          String errorMessage = errorPtr.reinterpret(1024).getUtf8String(0);
          DataFusionBindings.FREE_STRING.invokeExact(errorPtr);
          throw new RuntimeException("SQL execution failed: " + errorMessage);
        }
        throw new RuntimeException("SQL execution failed: unknown error");
      }

      logger.debug("Executed SQL query, got DataFrame: {}", dataframe);
      return new FfiDataFrame(runtime, dataframe);
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException("Failed to execute SQL", e);
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
        logger.debug("Closed FfiSessionContext");
      } catch (Throwable e) {
        logger.error("Error closing FfiSessionContext", e);
      }
    }
  }
}
