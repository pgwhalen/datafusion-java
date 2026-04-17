package org.apache.arrow.datafusion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.datasource.ArrowReadOptions;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.apache.arrow.datafusion.datasource.ParquetReadOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
class DefaultSessionContext extends AbstractProxy implements SessionContext {

  private static final Logger logger = LoggerFactory.getLogger(DefaultSessionContext.class);

  private final org.apache.arrow.datafusion.execution.SessionContext ffiCtx;
  private final BufferAllocator internalAllocator;

  DefaultSessionContext(ConfigOptions config) {
    super();
    this.internalAllocator = new RootAllocator();
    // Force Utf8 instead of Utf8View for backwards compat with old Arrow vector types
    ConfigOptions.Builder builder = ConfigOptions.builder();
    builder.option("datafusion.execution.parquet.schema_force_view_types", "false");
    // Merge user-provided options on top
    if (config.hasOptions()) {
      for (var entry : config.toOptionsMap().entrySet()) {
        builder.option(entry.getKey(), entry.getValue());
      }
    }
    this.ffiCtx = new org.apache.arrow.datafusion.execution.SessionContext(builder.build());
  }

  private static <T> CompletableFuture<T> runAsync(Supplier<T> action) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      future.complete(action.get());
    } catch (Exception e) {
      future.completeExceptionally(new RuntimeException(e));
    }
    return future;
  }

  private static CompletableFuture<Void> runAsyncVoid(Runnable action) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      action.run();
      future.complete(null);
    } catch (Exception e) {
      future.completeExceptionally(new RuntimeException(e));
    }
    return future;
  }

  @Override
  public CompletableFuture<DataFrame> sql(String sql) {
    return runAsync(
        () -> {
          org.apache.arrow.datafusion.dataframe.DataFrame ffiDf = ffiCtx.sql(sql);
          return new DefaultDataFrame(this, ffiDf);
        });
  }

  private void validatePathExists(Path path) {
    if (!Files.exists(path)) {
      throw new RuntimeException("Path does not exist: " + path);
    }
  }

  @Override
  public CompletableFuture<Void> registerCsv(String name, Path path) {
    return runAsyncVoid(
        () -> {
          Path absPath = path.toAbsolutePath();
          validatePathExists(absPath);
          ffiCtx.registerCsv(
              name, absPath.toString(), CsvReadOptions.builder().build(), internalAllocator);
        });
  }

  @Override
  public CompletableFuture<Void> registerParquet(String name, Path path) {
    return runAsyncVoid(
        () -> {
          Path absPath = path.toAbsolutePath();
          validatePathExists(absPath);
          ffiCtx.registerParquet(
              name, absPath.toString(), ParquetReadOptions.builder().build(), internalAllocator);
        });
  }

  @Override
  public Optional<TableProvider> registerTable(String name, TableProvider tableProvider)
      throws Exception {
    if (ffiCtx.tableExist(name)) {
      // Clean up collected data if it was an intoView() result
      if (tableProvider instanceof CollectedViewTableProvider cvtp) {
        cvtp.releaseData();
      }
      throw new RuntimeException("Table '" + name + "' already exists");
    }

    if (tableProvider instanceof CollectedViewTableProvider cvtp) {
      try {
        ffiCtx.registerBatch(name, cvtp.root, cvtp.dictionaryProvider, cvtp.allocator);
      } finally {
        cvtp.releaseData();
      }
    } else if (tableProvider instanceof ListingTable listingTable) {
      registerListingTable(name, listingTable);
    } else {
      throw new RuntimeException(
          "Unsupported TableProvider type: " + tableProvider.getClass().getName());
    }
    return Optional.empty();
  }

  private void registerListingTable(String name, ListingTable listingTable) {
    ListingTableConfig config = listingTable.getConfig();
    ListingOptions options = config.getListingOptions();
    FileFormat format = options.getFormat();
    String[] paths = config.getTablePaths();

    // Resolve the path to register - use the first path for single/multi
    String registrationPath;
    if (paths.length == 1) {
      registrationPath = paths[0];
    } else {
      // For multiple paths, use the parent directory of the first path
      Path firstPath = Path.of(paths[0]);
      Path parent = firstPath.getParent();
      registrationPath = (parent != null) ? parent.toString() : paths[0];
    }

    if (format instanceof CsvFormat) {
      ffiCtx.registerCsv(
          name, registrationPath, CsvReadOptions.builder().build(), internalAllocator);
    } else if (format instanceof ParquetFormat) {
      ffiCtx.registerParquet(
          name, registrationPath, ParquetReadOptions.builder().build(), internalAllocator);
    } else if (format instanceof ArrowFormat) {
      ffiCtx.registerArrow(
          name, registrationPath, ArrowReadOptions.builder().build(), internalAllocator);
    } else {
      throw new RuntimeException("Unsupported file format: " + format.getClass().getName());
    }
  }

  @Override
  public Runtime getRuntime() {
    return NoOpRuntime.INSTANCE;
  }

  @Override
  void doClose(long pointer) throws Exception {
    ffiCtx.close();
    internalAllocator.close();
  }
}
