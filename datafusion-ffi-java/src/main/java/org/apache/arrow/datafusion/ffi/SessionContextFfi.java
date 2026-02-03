package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.datafusion.CatalogProvider;
import org.apache.arrow.datafusion.DataFusionException;
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
  private final SessionConfig config;

  // Shared arena for catalog providers (needs to live as long as the context)
  private final Arena catalogArena;
  // Keep references to prevent GC
  private final List<CatalogProviderHandle> catalogHandles = new ArrayList<>();

  /**
   * Creates a new SessionContextFfi.
   *
   * @param context The native context pointer
   * @param config The session configuration
   */
  public SessionContextFfi(MemorySegment context, SessionConfig config) {
    this.context = context;
    this.config = config;
    this.catalogArena = Arena.ofShared();
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
   * Closes the catalog handles. Call this before destroying the native context.
   *
   * <p>Note: The catalog arena is not closed here because upcall stubs may still be invoked during
   * context destruction. Call {@link #closeArena()} after the native context is destroyed.
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
   * Closes the catalog arena. Call this after the native context is destroyed to free upcall stubs.
   */
  public void closeArena() {
    catalogArena.close();
  }

  @Override
  public void close() {
    closeCatalogHandles();
    closeArena();
  }
}
