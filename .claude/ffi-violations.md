# FFI Rule Violations & Duplication

Audit of the codebase against `.claude/rules/ffi.md`. Most items in this file have been resolved;
remaining work and deferred items are flagged below.

## Rule Violations

### Integer-encoded enums across the FFI boundary — **DONE**
The rule "NEVER return an integer value to represent an enumerated type across the FFI boundary"
was violated in several Diplomat trait signatures. Fixed by introducing Diplomat enums
(`DfVolatility`, `DfTableType`, `DfEmissionType`, `DfBoundedness`, `DfFilterPushdown`) plus the
already-existing `DfInsertOp`, and reworking `supports_filters_pushdown` to return a raw pointer
to a new `DfFilterPushdownList` opaque (Java pushes typed enum values; Rust takes ownership and
reads them back).

Trait signatures updated:
- `DfScalarUdfTrait::volatility() -> DfVolatility` (was `i32`)
- `DfAggregateUdfTrait::volatility() -> DfVolatility` (was `i32`)
- `DfTableTrait::table_type() -> DfTableType` (was `i32`)
- `DfTableTrait::supports_filters_pushdown(...) -> usize` returning a `DfFilterPushdownList` raw
  ptr (was an `(addr, cap) -> count` buffer of `i32` discriminants)
- `DfTableTrait::insert_into(..., insert_op: DfInsertOp, ...)` (was `i32`)
- `DfExecutionPlanTrait::emission_type() -> DfEmissionType` (was `i32`)
- `DfExecutionPlanTrait::boundedness() -> DfBoundedness` (was `i32`)

Helpers `volatility_from_i32` (in `udf_common.rs`) and the inline `match` in `execution_plan.rs`
became Diplomat-enum match statements.

### Public methods exposing Diplomat-generated types — **DEFERRED**
The cross-package layout (Bridge classes in different packages need to share Diplomat handles)
makes a clean fix impossible without a larger restructure:

- `SessionContextBridge.dfContext()` is called from `LogicalPlanBuilder` in the `logical_expr`
  package, so it can't be made package-private without moving code.
- `RustCatalogProvider.handle()` / `RustTableProvider.handle()` are called from
  `SessionContextBridge` in the `execution` package — same constraint.

The new ArchUnit rule (`publicApiMethodsShouldNotReturnGeneratedTypes`) carves out
`RustCatalogProvider` and `RustTableProvider` as documented exceptions; any new offender will fail
the test.

### Public constructors taking internal types — **DEFERRED**
Same cross-package constraint as above. `DataFrame(DataFrameBridge)`,
`PhysicalExpr(PhysicalExprBridge)`, `LogicalPlanBuilder(LogicalPlanBuilderBridge)`, and the 40+
`LogicalPlan$*` records all have public constructors taking Bridge parameters because they're
constructed from packages other than their own. The ArchUnit rule for this was scoped out (see
task #14 in the todo list).

### Open-coded `LogicalExprList` encoding — **DONE**
- `bridge.rs:680` — `DfExprBytes::from_exprs` now delegates to `crate::table::encode_exprs` (which
  was promoted from `pub(crate)`). The empty-slice short-circuit in `encode_exprs` now applies
  uniformly.

### Open-coded `FFI_ArrowSchema` export — **NOT A VIOLATION**
- `file_format.rs:87` and `file_source.rs:74` were flagged as needing `export_schema_to`.
  Re-evaluation: both of those sites construct an `FFI_ArrowSchema` on the Rust stack and pass the
  address to Java for *Java to read from*. There is no `std::ptr::write` and no Java-allocated
  address; `export_schema_to` is for the opposite direction. Left as-is.

### Manual upcall plumbing where helpers exist — **PARTIAL**
- `udaf.rs:134-138` (`accumulator()`) still builds an `ErrorBuffer` directly. This is acceptable
  per the rules doc (similar to `stream.rs`); not changed in this pass.

### Inconsistent string conversion — **DONE**
- `flight_sql_test_server.rs:178` now calls `crate::diplomat_util::diplomat_str(name)` instead of
  `std::str::from_utf8(name)`.

## Duplication

### 1. Arrow C Data Interface size constants — **DONE**
Hoisted to `FfiArrowConstants` (`ARROW_SCHEMA_SIZE`, `ARROW_ARRAY_SIZE`, `WRAPPED_ARRAY_SIZE`).
The five adapter classes now reference the constants rather than redeclaring them.

### 2. Field import/export helpers — **DONE**
Extracted `importFieldFromSegment`, `importFieldArray`, `importFieldFromAddress`,
`exportFieldToAddress` (and friends) to a new package-private `FfiSchemaUtil` class. Removed the
duplicate copies from `DfScalarUDFAdapter` and `DfAggregateUDFAdapter`.

### 3. Release-callback nullification idiom — **DONE**
`FfiSchemaUtil` now exposes:
- `importSchemaFromSegment(BufferAllocator, MemorySegment)` and `…FromAddress`
- `importArrayFromSegment(BufferAllocator, MemorySegment)`
- `copySchemaToAddress(MemorySegment, long)` / `copyArrayToAddress(...)` for the export
  direction
- `importFieldArray` / `exportFieldToAddress` / `importFieldFromAddress` for the wrapped-Field
  pattern

Adapter classes (`DfFileSourceAdapter`, `DfVarProviderAdapter`, `DfScalarUDFAdapter`,
`DfAggregateUDFAdapter`, `DfRecordBatchReaderAdapter`) all use the helpers.

### 4. `Linker` + `lib.find().orElseThrow()` + `downcallHandle()` boilerplate — **DONE**
Added `NativeUtil.createDowncallHandle(String, FunctionDescriptor)`; both `NativeUtil`'s own
static block and `DfTableAdapter` use it (was 4 lines each, now 1 line per handle).

### 5. Bridge `DfError` → `DataFusionException` conversion — **PARTIAL**
Added `BridgeUtil.unwrap(String, Supplier<T>)` and the `Runnable` overload. Refactored
`LogicalPlanBuilderBridge` end-to-end as the exemplar; the other 5–6 bridge classes still use the
old inline `try { ... } catch (DfError e) { ... }` pattern. Migrating those is straightforward
follow-up work.

### 6. `DfStringArray` try-with-resources iteration — **DONE**
Added `BridgeUtil.toList(DfStringArray)`. `SessionContextBridge.toStringList` was removed in favor
of the helper. `SessionConfigBridge.options()` (key/value zip) and `TaskContextBridge.toHandleMap`
(name→handle map) have different shapes and stay as-is.

### 7. `DfExprBytes` byte-extraction idiom — **DONE**
Added `BridgeUtil.readBytes(DfExprBytes)`. `LogicalPlanBridge.readRawBytes`,
`LiteralGuaranteeBridge.readExprBytes`, and `SessionContextBridge.parseSqlExpr` all delegate to it
now.

## Things that look like violations but aren't

- **`DfScalarUDFAdapter.java:153`** — clearing offset 64 (private_data) on an `ArrowSchema` during
  *export to Rust*. The auto-memory entry "Arrow C Data Interface: Release vs Private_data
  Offsets" explicitly notes this is acceptable in the export direction because
  `ArrowSchema.close()` does not call the release callback. Encapsulated in
  `FfiSchemaUtil.copySchemaToAddress` with a comment; not a bug.
- **`duckdb_factory.rs::RustDuckDbMode`** — one agent flagged this as missing
  `#[diplomat::enum_convert]`, but the attribute is present at line 17.
- **`table.rs:131` `result_buf = vec![0i32; result_cap]`** — gone, replaced by the
  `DfFilterPushdownList` opaque.

## ArchUnit additions

`DiplomatEncapsulationTest` previously enforced rules 2/3/4 of the encapsulation section. Added:

- **Rule 5** — `publicApiMethodsShouldNotReturnGeneratedTypes`: public API classes (i.e., not
  `*Bridge` / `*Adapter` / `*Ffi` / `generated.*`) must not return Diplomat-generated `Df*` types.
  `RustCatalogProvider` / `RustTableProvider` are listed as known carve-outs.

The "constructors taking internal types must be package-private" rule was prototyped but rejected
— it flagged ~50 cases (most of them auto-generated record canonical constructors for
`LogicalPlan`'s sealed-interface variants), virtually all of which require cross-package
restructuring to fix. Tracked in the deferred task #3.

`UTILITY_CLASS_NAMES` (the set of classes considered internal helpers) was extended with
`BridgeUtil`, `FfiSchemaUtil`, and `FfiArrowConstants`.
