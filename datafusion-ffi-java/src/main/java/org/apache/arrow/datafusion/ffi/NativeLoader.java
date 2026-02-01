package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles loading the native DataFusion FFI library using Java's Foreign Function and Memory API.
 */
public final class NativeLoader {
  private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);
  private static volatile SymbolLookup lookup;

  private NativeLoader() {}

  /**
   * Get the symbol lookup for the native library.
   *
   * @return The symbol lookup instance
   */
  public static synchronized SymbolLookup get() {
    if (lookup == null) {
      String libPath = System.getProperty("java.library.path");
      if (libPath == null || libPath.isEmpty()) {
        throw new RuntimeException(
            "java.library.path not set. Cannot load native DataFusion library.");
      }
      String libName = System.mapLibraryName("datafusion_ffi_native");
      Path path = Path.of(libPath.split(java.io.File.pathSeparator)[0], libName);
      logger.info("Loading native library from: {}", path);
      lookup = SymbolLookup.libraryLookup(path, Arena.global());
    }
    return lookup;
  }
}
