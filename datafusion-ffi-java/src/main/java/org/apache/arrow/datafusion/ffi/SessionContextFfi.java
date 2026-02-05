package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.CatalogProvider;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.ListingTable;
import org.apache.arrow.datafusion.SessionConfig;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal FFI helper for SessionContext.
 *
 * <p>This class manages the FFI-level operations for SessionContext, including catalog registration
 * and lifecycle management of native resources. It exists in the ffi package to access
 * package-private classes like {@link CatalogProviderHandle}.
 */
public final class SessionContextFfi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionContextFfi.class);

  private final MemorySegment context;
  private final MemorySegment runtime;
  private final SessionConfig config;

  // Shared arena for catalog providers (needs to live as long as the context)
  private final Arena catalogArena;
  // Keep references to prevent GC
  private final List<CatalogProviderHandle> catalogHandles = new ArrayList<>();

  // Shared arena for listing table formats (needs to live as long as the context)
  private final Arena listingTableArena;
  // Keep references to prevent GC
  private final List<FileFormatHandle> formatHandles = new ArrayList<>();

  /**
   * Creates a new SessionContextFfi.
   *
   * @param context The native context pointer
   * @param config The session configuration
   */
  /**
   * Creates a new SessionContextFfi.
   *
   * @param context The native context pointer
   * @param runtime The native runtime pointer
   * @param config The session configuration
   */
  public SessionContextFfi(MemorySegment context, MemorySegment runtime, SessionConfig config) {
    this.context = context;
    this.runtime = runtime;
    this.config = config;
    this.catalogArena = Arena.ofShared();
    this.listingTableArena = Arena.ofShared();
  }

  /**
   * Registers a catalog provider with the session context.
   *
   * @param name The catalog name
   * @param catalog The catalog provider implementation
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerCatalog(String name, CatalogProvider catalog, BufferAllocator allocator) {
    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the catalog (uses the shared catalog arena)
      CatalogProviderHandle handle =
          new CatalogProviderHandle(catalog, allocator, catalogArena, config.fullStackTrace());
      catalogHandles.add(handle);

      MemorySegment nameSegment = arena.allocateFrom(name);
      MemorySegment callbacks = handle.getTraitStruct();

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
   * Registers a listing table with the session context.
   *
   * @param name The table name
   * @param table The listing table configuration
   * @param allocator The buffer allocator to use for Arrow data transfers
   * @throws DataFusionException if registration fails
   */
  public void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    try (Arena arena = Arena.ofConfined()) {
      // Create a handle for the format (uses the shared listing table arena)
      FileFormatHandle handle =
          new FileFormatHandle(
              table.options().format(),
              table.schema(),
              allocator,
              listingTableArena,
              config.fullStackTrace());
      formatHandles.add(handle);

      MemorySegment nameSegment = arena.allocateFrom(name);
      MemorySegment urlSegment = arena.allocateFrom(table.url().url());
      MemorySegment extSegment = arena.allocateFrom(table.options().fileExtension());

      // Export schema via Arrow C Data Interface
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, table.schema(), null, ffiSchema);
        MemorySegment schemaAddr = MemorySegment.ofAddress(ffiSchema.memoryAddress());

        NativeUtil.call(
            arena,
            "Register listing table '" + name + "'",
            errorOut ->
                (int)
                    DataFusionBindings.CONTEXT_REGISTER_LISTING_TABLE.invokeExact(
                        context,
                        runtime,
                        nameSegment,
                        urlSegment,
                        extSegment,
                        schemaAddr,
                        handle.getTraitStruct(),
                        table.options().collectStat(),
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
   * Closes the catalog handles. Call this before destroying the native context.
   *
   * <p>Note: The catalog arena is not closed here because upcall stubs may still be invoked during
   * context destruction. Call {@link #closeArenas()} after the native context is destroyed.
   */
  public void closeCatalogHandles() {
    for (CatalogProviderHandle handle : catalogHandles) {
      try {
        handle.close();
      } catch (Exception e) {
        logger.warn("Error closing catalog handle", e);
      }
    }
    catalogHandles.clear();
  }

  /**
   * Closes the format handles. Call this before destroying the native context.
   *
   * <p>Note: The listing table arena is not closed here because upcall stubs may still be invoked
   * during context destruction. Call {@link #closeArenas()} after the native context is destroyed.
   */
  public void closeFormatHandles() {
    for (FileFormatHandle handle : formatHandles) {
      try {
        handle.close();
      } catch (Exception e) {
        logger.warn("Error closing format handle", e);
      }
    }
    formatHandles.clear();
  }

  /** Closes all arenas. Call this after the native context is destroyed to free upcall stubs. */
  public void closeArenas() {
    catalogArena.close();
    listingTableArena.close();
  }

  @Override
  public void close() {
    closeCatalogHandles();
    closeFormatHandles();
    closeArenas();
  }
}
