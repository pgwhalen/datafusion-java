package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
 * Bridge between public SessionContext API and Diplomat-generated DfSessionContext.
 *
 * <p>This replaces SessionContextFfi, delegating to the Diplomat-generated class for all native
 * calls.
 */
final class SessionContextBridge implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(SessionContextBridge.class);

  private final DfSessionContext dfCtx;
  private final SessionConfig config;

  // Shared arena for all registered providers/UDFs (needs to live as long as the context)
  private final Arena sharedArena;
  // Keep references to prevent GC of Diplomat trait impls while Rust holds pointers
  private final List<Object> traitImpls = new ArrayList<>();

  SessionContextBridge(SessionConfig config) {
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
      throw new DataFusionException(e.getMessage());
    } catch (Exception e) {
      sharedArena.close();
      throw new DataFusionException("Failed to create SessionContext", e);
    }
  }

  SessionContextBridge(SessionConfig config, DfRuntimeEnv rtEnv) {
    this.config = config;
    this.sharedArena = Arena.ofShared();
    try {
      this.dfCtx = createWithConfigRt(config, rtEnv);
      logger.debug("Created SessionContext with RuntimeEnv via Diplomat bridge");
    } catch (DfError e) {
      sharedArena.close();
      throw new DataFusionException(e.getMessage());
    } catch (Exception e) {
      sharedArena.close();
      throw new DataFusionException("Failed to create SessionContext", e);
    }
  }

  private static DfSessionContext createWithConfig(SessionConfig config) {
    return createFromOptions(config.toOptionsMap(), null);
  }

  private static DfSessionContext createWithConfigRt(SessionConfig config, DfRuntimeEnv rtEnv) {
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

  void registerTable(
      String name, VectorSchemaRoot root, DictionaryProvider provider, BufferAllocator allocator) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, provider, ffiArray, ffiSchema);

      // Create a DfArrowBatch from the exported data (schema first, array second)
      try (DfArrowBatch batch =
          DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
        dfCtx.registerTable(name, batch);
      }
      logger.debug("Registered table '{}' with {} rows", name, root.getRowCount());
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (DataFusionException e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionException("Failed to register table", e);
    }
  }

  DataFrameBridge sql(String query) {
    try {
      DfDataFrame df = dfCtx.sql(query);
      logger.debug("Executed SQL query via Diplomat bridge");
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to execute SQL", e);
    }
  }

  SessionStateBridge state() {
    try {
      DfSessionState dfState = dfCtx.state();
      return new SessionStateBridge(dfState);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to get session state", e);
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
      throw new DataFusionException(dfErrorMessage(e));
    } catch (DataFusionException e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionException("Failed to parse SQL expression", e);
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
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to register catalog", e);
    }
  }

  void registerUdf(ScalarUdf udf, BufferAllocator allocator) {
    try {
      DfScalarUdfAdapter adapter = new DfScalarUdfAdapter(udf, allocator, config.fullStackTrace());
      traitImpls.add(adapter);
      dfCtx.registerUdf(adapter);
      logger.debug("Registered UDF '{}'", udf.name());
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to register UDF", e);
    }
  }

  void registerListingTable(String name, ListingTable table, BufferAllocator allocator) {
    try {
      DfFileFormatAdapter adapter =
          new DfFileFormatAdapter(table.options().format(), allocator, config.fullStackTrace());
      traitImpls.add(adapter);

      // Build URL bytes (null-separated)
      List<ListingTableUrl> tablePaths = table.tablePaths();
      String[] urlStrs = tablePaths.stream().map(ListingTableUrl::url).toArray(String[]::new);
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
      throw new DataFusionException(dfErrorMessage(e));
    } catch (DataFusionException e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionException("Failed to register listing table", e);
    }
  }

  DataFrameBridge table(String name) {
    try {
      DfDataFrame df = dfCtx.table(name);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to get table", e);
    }
  }

  DataFrameBridge readParquet(String path) {
    try {
      DfDataFrame df = dfCtx.readParquet(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to read Parquet file", e);
    }
  }

  DataFrameBridge readCsv(String path) {
    try {
      DfDataFrame df = dfCtx.readCsv(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to read CSV file", e);
    }
  }

  DataFrameBridge readJson(String path) {
    try {
      DfDataFrame df = dfCtx.readJson(path);
      return new DataFrameBridge(df);
    } catch (DfError e) {
      throw new DataFusionException(dfErrorMessage(e));
    } catch (Exception e) {
      throw new DataFusionException("Failed to read JSON file", e);
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
      throw new DataFusionException("Error closing SessionContext", firstError);
    }
    logger.debug("Closed SessionContext");
  }

  private static String dfErrorMessage(DfError e) {
    try (e) {
      return e.toDisplay();
    }
  }
}
