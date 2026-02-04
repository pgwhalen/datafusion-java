package org.apache.arrow.datafusion.ffi;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.SymbolLookup;
import java.nio.file.Files;
import java.nio.file.Path;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLibraryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles loading the native DataFusion FFI library using Java's Foreign Function and Memory API.
 *
 * <p>The loading strategy follows a three-tier approach:
 *
 * <ol>
 *   <li><b>java.library.path</b> - Check system property (development/test mode)
 *   <li><b>JAR extraction</b> - Extract library from JAR resources using native-lib-loader
 *   <li><b>System path fallback</b> - Try System.loadLibrary() + SymbolLookup.loaderLookup()
 * </ol>
 */
public final class NativeLoader {
  private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);
  private static final String LIB_NAME = "datafusion_ffi_native";
  private static volatile SymbolLookup lookup;

  private NativeLoader() {}

  /**
   * Get the symbol lookup for the native library.
   *
   * @return The symbol lookup instance
   */
  public static synchronized SymbolLookup get() {
    if (lookup == null) {
      lookup = loadLibrary();
    }
    return lookup;
  }

  private static SymbolLookup loadLibrary() {
    // Strategy 1: Try java.library.path (development/test mode)
    SymbolLookup result = tryLoadFromLibraryPath();
    if (result != null) {
      return result;
    }

    // Strategy 2: Try extracting from JAR resources
    result = tryLoadFromJar();
    if (result != null) {
      return result;
    }

    // Strategy 3: Try system library path via System.loadLibrary
    result = tryLoadFromSystem();
    if (result != null) {
      return result;
    }

    throw new RuntimeException(
        "Failed to load native DataFusion library. "
            + "Ensure the library is either: "
            + "(1) available via java.library.path, "
            + "(2) embedded in the JAR under natives/{platform}/, or "
            + "(3) installed on the system library path.");
  }

  private static SymbolLookup tryLoadFromLibraryPath() {
    String libPath = System.getProperty("java.library.path");
    if (libPath == null || libPath.isEmpty()) {
      logger.debug("java.library.path not set, skipping");
      return null;
    }

    String libName = System.mapLibraryName(LIB_NAME);
    for (String dir : libPath.split(File.pathSeparator)) {
      Path path = Path.of(dir, libName);
      if (Files.exists(path)) {
        logger.info("Loading native library from java.library.path: {}", path);
        try {
          return SymbolLookup.libraryLookup(path, Arena.global());
        } catch (Exception e) {
          logger.debug("Failed to load from {}: {}", path, e.getMessage());
        }
      }
    }

    logger.debug("Library not found in java.library.path");
    return null;
  }

  private static SymbolLookup tryLoadFromJar() {
    try {
      // Use native-lib-loader to extract the library from JAR resources
      JniExtractor extractor = new DataFusionJniExtractor();
      File extractedLib = extractor.extractJni("natives", LIB_NAME);

      if (extractedLib != null && extractedLib.exists()) {
        Path path = extractedLib.toPath();
        logger.info("Loading native library extracted from JAR: {}", path);
        return SymbolLookup.libraryLookup(path, Arena.global());
      }
    } catch (IOException e) {
      logger.debug("Failed to extract library from JAR: {}", e.getMessage());
    } catch (Exception e) {
      logger.debug("JAR extraction not available: {}", e.getMessage());
    }

    return null;
  }

  private static SymbolLookup tryLoadFromSystem() {
    try {
      logger.info("Attempting to load native library via System.loadLibrary");
      System.loadLibrary(LIB_NAME);
      return SymbolLookup.loaderLookup();
    } catch (UnsatisfiedLinkError e) {
      logger.debug("System.loadLibrary failed: {}", e.getMessage());
      return null;
    }
  }

  /** Custom JNI extractor that uses native-lib-loader's platform detection. */
  private static class DataFusionJniExtractor implements JniExtractor {
    private final File tempDir;

    DataFusionJniExtractor() throws IOException {
      this.tempDir = Files.createTempDirectory("datafusion-native-").toFile();
      this.tempDir.deleteOnExit();
    }

    @Override
    public File extractJni(String libPath, String libName) throws IOException {
      String platformDir = getPlatformDirectory();
      String fullLibName = System.mapLibraryName(libName);

      // Construct the resource path: natives/{platform}/{libname}
      String resourcePath = libPath + "/" + platformDir + "/" + fullLibName;

      try (var inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
        if (inputStream == null) {
          logger.debug("Native library resource not found: {}", resourcePath);
          return null;
        }

        File destFile = new File(tempDir, fullLibName);
        Files.copy(inputStream, destFile.toPath());
        destFile.deleteOnExit();

        logger.debug("Extracted native library to: {}", destFile);
        return destFile;
      }
    }

    @Override
    public void extractRegistered() throws IOException {
      // Not used - we extract on demand
    }

    /** Get the platform directory name matching native-lib-loader convention. */
    private String getPlatformDirectory() {
      NativeLibraryUtil.Architecture arch = NativeLibraryUtil.getArchitecture();

      // Map Architecture enum to our directory naming convention: {os}_{arch}
      // Architecture enum values: OSX_64, OSX_ARM64, LINUX_64, LINUX_ARM64, WINDOWS_64, etc.
      return switch (arch) {
        case OSX_64 -> "osx_64";
        case OSX_ARM64 -> "osx_arm64";
        case LINUX_64 -> "linux_64";
        case LINUX_ARM64 -> "linux_arm64";
        case WINDOWS_64 -> "windows_64";
        default -> throw new UnsupportedOperationException("Unsupported platform: " + arch);
      };
    }
  }
}
