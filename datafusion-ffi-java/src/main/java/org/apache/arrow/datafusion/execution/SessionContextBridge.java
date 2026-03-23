package org.apache.arrow.datafusion.execution;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.DfCatalogAdapter;
import org.apache.arrow.datafusion.DfFileFormatAdapter;
import org.apache.arrow.datafusion.DfScalarUDFAdapter;
import org.apache.arrow.datafusion.DfTableAdapter;
import org.apache.arrow.datafusion.ExprProtoConverter;
import org.apache.arrow.datafusion.catalog.CatalogProvider;
import org.apache.arrow.datafusion.catalog.TableProvider;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.common.NativeDataFusionError;
import org.apache.arrow.datafusion.config.ConfigOptions;
import org.apache.arrow.datafusion.dataframe.DataFrameBridge;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.apache.arrow.datafusion.datasource.ListingTable;
import org.apache.arrow.datafusion.datasource.ListingTableUrl;
import org.apache.arrow.datafusion.datasource.NdJsonReadOptions;
import org.apache.arrow.datafusion.datasource.ParquetReadOptions;
import org.apache.arrow.datafusion.generated.DfArrowBatch;
import org.apache.arrow.datafusion.generated.DfArrowSchema;
import org.apache.arrow.datafusion.generated.DfDataFrame;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfExprBytes;
import org.apache.arrow.datafusion.generated.DfRuntimeEnv;
import org.apache.arrow.datafusion.generated.DfSessionContext;
import org.apache.arrow.datafusion.generated.DfSessionState;
import org.apache.arrow.datafusion.logical_expr.Expr;
import org.apache.arrow.datafusion.logical_expr.ScalarUDF;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge between public SessionContext API and Diplomat-generated DfSessionContext.
 *
 * <p>This replaces SessionContextFfi, delegating to the Diplomat-generated class for all native
 * calls.
 */
public final class SessionContextBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionContextBridge.class);

  private final DfSessionContext dfCtx;
  private final ConfigOptions config;

  // Shared arena for all registered providers/UDFs (needs to live as long as the context)
  private final Arena sharedArena;
  // Keep references to prevent GC of Diplomat trait impls while Rust holds pointers
  private final List<Object> traitImpls = new ArrayList<>();

  SessionContextBridge(ConfigOptions config) {
    this.config = config;
    this.sharedArena = Arena.ofShared();
    try {
      if (config.hasOptions()) {
        this.dfCtx = createWithConfig(config);
      } else {
        this.dfCtx = new DfSessionContext();
      }
      logger.debug("Created SessionContext via Diplomat bridge");
    } catch (DfError e) {
      sharedArena.close();
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      sharedArena.close();
      throw new DataFusionError("Failed to create SessionContext", e);
    }
  }

  SessionContextBridge(ConfigOptions config, DfRuntimeEnv rtEnv) {
    this.config = config;
    this.sharedArena = Arena.ofShared();
    try {
      this.dfCtx = createWithConfigRt(config, rtEnv);
      logger.debug("Created SessionContext with RuntimeEnv via Diplomat bridge");
    } catch (DfError e) {
      sharedArena.close();
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      sharedArena.close();
      throw new DataFusionError("Failed to create SessionContext", e);
    }
  }

  private static DfSessionContext createWithConfig(ConfigOptions config) {
    return createFromOptions(config.toOptionsMap(), null);
  }

  private static DfSessionContext createWithConfigRt(ConfigOptions config, DfRuntimeEnv rtEnv) {
    Map<String, String> options = config.hasOptions() ? config.toOptionsMap() : Map.of();
    return createFromOptions(options, rtEnv);
  }

  private static DfSessionContext createFromOptions(
      Map<String, String> options, DfRuntimeEnv rtEnv) {
    // Encode as interleaved null-separated: key\0value\0key\0value...
    byte[] optBytes = encodeConfigOptions(options);
    if (rtEnv != null) {
      return DfSessionContext.newWithConfigRt(optBytes, rtEnv);
    } else {
      return DfSessionContext.newWithConfig(optBytes);
    }
  }

  private static byte[] encodeNullSeparated(String[] strs) {
    if (strs.length == 0) return new byte[0];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strs.length; i++) {
      if (i > 0) sb.append('\0');
      sb.append(strs[i]);
    }
    return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  private static byte[] encodeConfigOptions(Map<String, String> options) {
    if (options.isEmpty()) return new byte[0];
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (!first) sb.append('\0');
      sb.append(entry.getKey()).append('\0').append(entry.getValue());
      first = false;
    }
    return sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  void registerBatch(
      String name, VectorSchemaRoot root, DictionaryProvider provider, BufferAllocator allocator) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, provider, ffiArray, ffiSchema);

      // Create a DfArrowBatch from the exported data (schema first, array second)
      try (DfArrowBatch batch =
          DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
        dfCtx.registerTable(name, batch);
      }
      logger.debug("Registered batch as table '{}' with {} rows", name, root.getRowCount());
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to register batch", e);
    }
  }

  void registerTableProvider(String name, TableProvider provider, BufferAllocator allocator) {
    try {
      DfTableAdapter adapter = new DfTableAdapter(provider, allocator, config.fullStackTrace());
      traitImpls.add(adapter);
      dfCtx.registerTableProvider(name, adapter);
      // Rust has imported the schema from the FFI address; release the exported data
      adapter.closeFfiSchema();
      logger.debug("Registered table provider '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to register table provider", e);
    }
  }

  boolean deregisterTable(String name) {
    try {
      return dfCtx.deregisterTable(name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to deregister table", e);
    }
  }

  DataFrameBridge sql(String query) {
    try {
      DfDataFrame df = dfCtx.sql(query);
      logger.debug("Executed SQL query via Diplomat bridge");
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to execute SQL", e);
    }
  }

  SessionStateBridge state() {
    try {
      DfSessionState dfState = dfCtx.state();
      return new SessionStateBridge(dfState);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get session state", e);
    }
  }

  Expr parseSqlExpr(String sql, org.apache.arrow.vector.types.pojo.Schema schema) {
    try (org.apache.arrow.memory.RootAllocator tempAllocator =
            new org.apache.arrow.memory.RootAllocator();
        ArrowSchema ffiSchema = ArrowSchema.allocateNew(tempAllocator)) {
      Data.exportSchema(tempAllocator, schema, null, ffiSchema);

      try (DfArrowSchema dfSchema = DfArrowSchema.fromAddress(ffiSchema.memoryAddress())) {
        try (DfExprBytes exprBytes = dfCtx.parseSqlExpr(sql, dfSchema)) {
          long len = exprBytes.len();
          try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(len);
            exprBytes.copyTo(buf.address(), len);
            byte[] protoBytes = buf.toArray(ValueLayout.JAVA_BYTE);
            return ExprProtoConverter.fromProtoBytes(protoBytes).get(0);
          }
        }
      }
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to parse SQL expression", e);
    }
  }

  String sessionId() {
    return dfCtx.sessionId();
  }

  long sessionStartTimeMillis() {
    return dfCtx.sessionStartTimeMillis();
  }

  void registerCatalog(String name, CatalogProvider catalog, BufferAllocator allocator) {
    try {
      DfCatalogAdapter adapter = new DfCatalogAdapter(catalog, allocator, config.fullStackTrace());
      traitImpls.add(adapter);
      dfCtx.registerCatalog(name, adapter);
      logger.debug("Registered catalog '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to register catalog", e);
    }
  }

  void registerUdf(ScalarUDF udf, BufferAllocator allocator) {
    try {
      DfScalarUDFAdapter adapter = new DfScalarUDFAdapter(udf, allocator, config.fullStackTrace());
      traitImpls.add(adapter);
      dfCtx.registerUdf(adapter);
      logger.debug("Registered UDF '{}'", udf.name());
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to register UDF", e);
    }
  }

  void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    try {
      DfFileFormatAdapter adapter =
          new DfFileFormatAdapter(table.options().format(), allocator, config.fullStackTrace());
      traitImpls.add(adapter);

      // Build URL bytes (null-separated)
      List<ListingTableUrl> tablePaths = table.tablePaths();
      String[] urlStrs = tablePaths.stream().map(ListingTableUrl::getUrl).toArray(String[]::new);
      byte[] urlBytes = encodeNullSeparated(urlStrs);
      String extension = table.options().fileExtension();

      // Export schema to FFI_ArrowSchema
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, table.schema(), null, ffiSchema);

        dfCtx.registerListingTable(
            name,
            adapter,
            urlBytes,
            extension,
            ffiSchema.memoryAddress(),
            table.options().collectStat() ? 1 : 0,
            table.options().targetPartitions());

        // Rust reads the schema by reference (doesn't consume it), so we must
        // explicitly release the exported data to avoid a memory leak.
        // ArrowSchema.close() only frees the struct memory, not the exported buffers.
        ffiSchema.release();
      }
      logger.debug("Registered listing table '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to register listing table", e);
    }
  }

  boolean tableExist(String name) {
    try {
      return dfCtx.tableExist(name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to check table existence", e);
    }
  }

  Optional<DataFrameBridge> table(String name) {
    try {
      if (!dfCtx.tableExist(name)) {
        return Optional.empty();
      }
      DfDataFrame df = dfCtx.table(name);
      return Optional.of(new DataFrameBridge(df));
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get table", e);
    }
  }

  DataFrameBridge readParquet(String path) {
    try {
      DfDataFrame df = dfCtx.readParquet(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to read Parquet file", e);
    }
  }

  DataFrameBridge readCsv(String path) {
    try {
      DfDataFrame df = dfCtx.readCsv(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to read CSV file", e);
    }
  }

  DataFrameBridge readJson(String path) {
    try {
      DfDataFrame df = dfCtx.readJson(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to read JSON file", e);
    }
  }

  void registerCsv(String name, String path, CsvReadOptions options, BufferAllocator allocator) {
    try {
      withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            dfCtx.registerCsv(name, path, options.encodeOptions(), schemaAddr);
            return null;
          });
      logger.debug("Registered CSV table '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to register CSV", e);
    }
  }

  void registerParquet(
      String name, String path, ParquetReadOptions options, BufferAllocator allocator) {
    try {
      withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            dfCtx.registerParquet(name, path, options.encodeOptions(), schemaAddr);
            return null;
          });
      logger.debug("Registered Parquet table '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to register Parquet", e);
    }
  }

  void registerJson(
      String name, String path, NdJsonReadOptions options, BufferAllocator allocator) {
    try {
      withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            dfCtx.registerJson(name, path, options.encodeOptions(), schemaAddr);
            return null;
          });
      logger.debug("Registered JSON table '{}'", name);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to register JSON", e);
    }
  }

  DataFrameBridge readCsv(String path, CsvReadOptions options, BufferAllocator allocator) {
    try {
      return withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            DfDataFrame df = dfCtx.readCsvWithOptions(path, options.encodeOptions(), schemaAddr);
            return new DataFrameBridge(df);
          });
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to read CSV with options", e);
    }
  }

  DataFrameBridge readParquet(String path, ParquetReadOptions options, BufferAllocator allocator) {
    try {
      return withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            DfDataFrame df =
                dfCtx.readParquetWithOptions(path, options.encodeOptions(), schemaAddr);
            return new DataFrameBridge(df);
          });
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to read Parquet with options", e);
    }
  }

  DataFrameBridge readJson(String path, NdJsonReadOptions options, BufferAllocator allocator) {
    try {
      return withOptionalSchema(
          options.schema(),
          allocator,
          schemaAddr -> {
            DfDataFrame df = dfCtx.readJsonWithOptions(path, options.encodeOptions(), schemaAddr);
            return new DataFrameBridge(df);
          });
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError("Failed to read JSON with options", e);
    }
  }

  List<String> catalogNames() {
    try (DfExprBytes bytes = dfCtx.catalogNames()) {
      return decodeNullSeparatedBytes(bytes);
    }
  }

  public List<String> catalogSchemaNames(String catalogName) {
    try {
      try (DfExprBytes bytes = dfCtx.catalogSchemaNames(catalogName)) {
        return decodeNullSeparatedBytes(bytes);
      }
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get schema names", e);
    }
  }

  public List<String> catalogTableNames(String catalogName, String schemaName) {
    try {
      try (DfExprBytes bytes = dfCtx.catalogTableNames(catalogName, schemaName)) {
        return decodeNullSeparatedBytes(bytes);
      }
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to get table names", e);
    }
  }

  public boolean catalogTableExists(String catalogName, String schemaName, String tableName) {
    try {
      return dfCtx.catalogTableExists(catalogName, schemaName, tableName);
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (Exception e) {
      throw new DataFusionError("Failed to check table existence in catalog", e);
    }
  }

  @FunctionalInterface
  interface SchemaAction<T> {
    T apply(long schemaAddr) throws Exception;
  }

  /**
   * Export a schema via FFI (if non-null), execute the action with the schema address, then release
   * the exported schema. If schema is null, the action is called with address 0.
   */
  private <T> T withOptionalSchema(
      org.apache.arrow.vector.types.pojo.Schema schema,
      BufferAllocator allocator,
      SchemaAction<T> action)
      throws Exception {
    if (schema == null) {
      return action.apply(0);
    }
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, schema, null, ffiSchema);
      try {
        return action.apply(ffiSchema.memoryAddress());
      } finally {
        ffiSchema.release();
      }
    }
  }

  private static List<String> decodeNullSeparatedBytes(DfExprBytes bytes) {
    long len = bytes.len();
    if (len == 0) {
      return List.of();
    }
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment buf = arena.allocate(len);
      bytes.copyTo(buf.address(), len);
      byte[] raw = buf.toArray(ValueLayout.JAVA_BYTE);
      String joined = new String(raw, java.nio.charset.StandardCharsets.UTF_8);
      return List.of(joined.split("\0"));
    }
  }

  @Override
  public void close() {
    Throwable firstError = null;
    try {
      dfCtx.close();
    } catch (Throwable e) {
      firstError = e;
    }
    try {
      sharedArena.close();
    } catch (Exception e) {
      if (firstError == null) firstError = e;
      else firstError.addSuppressed(e);
    }
    traitImpls.clear();
    if (firstError != null) {
      throw new DataFusionError("Error closing SessionContext", firstError);
    }
    logger.debug("Closed SessionContext");
  }
}
