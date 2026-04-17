# Migration Guide: datafusion-java to datafusion-ffi-java

## Overview

The `datafusion-java` library has been reimplemented as a thin compatibility layer on top of
`datafusion-ffi-java`. All public classes and interfaces in `datafusion-java` are now **deprecated**
and delegate to the new FFI-based library under the hood.

**Key changes:**
- **DataFusion upgraded** from version 25 to 52
- **JDK requirement raised** from 8+ to **22+** (required by Java's Foreign Function & Memory API)
- **JNI eliminated** &mdash; the Rust JNI crate (`datafusion-jni`) is no longer used
- **Zero-copy Arrow data transfer** via the Arrow C Data Interface

Existing code using `datafusion-java` should continue to work without changes, but you are
encouraged to migrate directly to `datafusion-ffi-java` for access to the full DataFusion API.

---

## Minimum Requirements

| Requirement | Old (`datafusion-java`) | New (via `datafusion-ffi-java`) |
|---|---|---|
| JDK | 8+ | **22+** |
| DataFusion | 25 | 52 |
| Arrow Java | 18.1.0 | 18.1.0 (unchanged) |
| Rust toolchain | Required (for JNI build) | Required (for FFI build) |

---

## API Mapping

### SessionContext

| Old API | New API |
|---|---|
| `SessionContexts.create()` | `new SessionContext()` |
| `SessionContexts.withConfig(SessionConfig)` | `new SessionContext(ConfigOptions)` |
| `context.sql(sql)` returns `CompletableFuture<DataFrame>` | `context.sql(sql)` returns `DataFrame` (synchronous) |
| `context.registerCsv(name, path)` | `context.registerCsv(name, path, CsvReadOptions.builder().build(), allocator)` |
| `context.registerParquet(name, path)` | `context.registerParquet(name, path, ParquetReadOptions.builder().build(), allocator)` |
| `context.registerTable(name, provider)` | `context.registerTable(name, provider, allocator)` or `context.registerBatch(name, root, allocator)` |

### DataFrame

| Old API | New API |
|---|---|
| `df.collect(allocator)` returns `CompletableFuture<ArrowReader>` | `df.collect(allocator)` returns `SendableRecordBatchStream` |
| `df.executeStream(allocator)` returns `CompletableFuture<RecordBatchStream>` | `df.executeStream(allocator)` returns `SendableRecordBatchStream` |
| `df.show()` returns `CompletableFuture<Void>` | `df.show()` (synchronous) |
| `df.writeParquet(path)` | `df.writeParquet(path.toString())` |
| `df.writeCsv(path)` | `df.writeCsv(path.toString())` |
| `df.intoView()` | Use `context.registerBatch()` directly |

### Configuration

| Old API | New API |
|---|---|
| `new SessionConfig()` | `ConfigOptions.builder()` |
| `config.executionOptions().withBatchSize(n)` | `ConfigOptions.builder().execution(ExecutionOptions.builder().batchSize(n).build()).build()` |
| `config.executionOptions().parquet().withPruning(b)` | Include `ParquetOptions` in `ExecutionOptions` builder |
| `config.sqlParserOptions().withDialect(s)` | `SqlParserOptions.builder().dialect(Dialect.POSTGRESQL).build()` |

### RecordBatchStream

| Old API | New API |
|---|---|
| `RecordBatchStream` interface | `SendableRecordBatchStream` class |
| `stream.loadNextBatch()` returns `CompletableFuture<Boolean>` | `stream.loadNextBatch()` returns `boolean` (synchronous) |
| `stream.getVectorSchemaRoot()` | Same |
| `stream.lookup(id)` / `getDictionaryIds()` | Same (implements `DictionaryProvider`) |

### ListingTable

| Old API | New API |
|---|---|
| `new CsvFormat()` | `CsvReadOptions.builder().build()` or implement `FileFormat` interface |
| `new ParquetFormat()` | `ParquetReadOptions.builder().build()` or implement `FileFormat` interface |
| `new ArrowFormat()` | SQL: `CREATE EXTERNAL TABLE ... STORED AS ARROW LOCATION '...'` |
| `ListingTableConfig.builder(path).withListingOptions(opts).build(ctx)` | `new ListingTableConfig(ListingTableUrl.parse(path)).withListingOptions(opts).withSchema(schema)` |
| `context.registerTable(name, listingTable)` | `context.registerListingTable(name, listingTable, allocator)` |

### Async to Sync

The old library used `CompletableFuture<T>` for all operations. The new library is **synchronous**.
If you need async behavior, wrap calls in your own executor:

```java
// Old
CompletableFuture<DataFrame> df = context.sql("SELECT 1");
df.thenComposeAsync(d -> d.collect(allocator)).join();

// New
DataFrame df = context.sql("SELECT 1");
SendableRecordBatchStream stream = df.collect(allocator);
```

---

## New Features Available After Migration

The new `datafusion-ffi-java` library provides significantly more functionality:

- **DataFrame transformations**: `filter()`, `select()`, `sort()`, `limit()`, `aggregate()`, `join()`, `union()`, `distinct()`, `withColumn()`, `dropColumns()`, etc.
- **Expression builder**: `col()`, `lit()`, `avg()`, `sum()`, `count()`, type casts, CASE/WHEN, etc.
- **LogicalPlanBuilder**: Build query plans programmatically
- **Custom TableProvider/CatalogProvider**: Implement Java-side data sources that DataFusion can query
- **Scalar UDFs**: Register Java functions callable from SQL
- **JSON support**: `registerJson()`, `readJson()`, `writeJson()`
- **Session state**: `parseSqlExpr()`, `sessionId()`, `sessionStartTime()`
- **Memory management**: `RuntimeEnvBuilder` with memory pool limits

---

## Behavioral Differences (DataFusion 25 vs 52)

- **String columns** may use `Utf8View` vectors instead of `Utf8` by default. The compatibility layer forces `Utf8` via `datafusion.execution.parquet.schema_force_view_types=false`.
- **Path validation**: The old library validated file paths at registration time. The compatibility layer preserves this behavior with explicit `Files.exists()` checks.
- **Default configuration values** are preserved from the old library's defaults for backwards compatibility. When using the new library directly, DataFusion 52 defaults apply.
