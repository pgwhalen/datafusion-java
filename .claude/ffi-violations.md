# FFI Rule Violations & Duplication

Audit of the codebase against `.claude/rules/ffi.md`, performed by 10 parallel agents and spot-verified. Findings are prioritized by impact. Line numbers are at the time of writing — re-grep before fixing.

## Rule Violations

### Integer-encoded enums across the FFI boundary
The rule "NEVER return an integer value to represent an enumerated type across the FFI boundary" is violated in several Diplomat trait signatures. Every one of these has a corresponding Java adapter that switches on the int and a Rust caller that switches it back. All should be Diplomat enums.

- `bridge.rs:245` — `DfScalarUdfTrait::volatility() -> i32` (0/1/2)
- `bridge.rs:290` — `DfAggregateUdfTrait::volatility() -> i32` (0/1/2)
- `bridge.rs:403` — `DfTableTrait::table_type() -> i32` (0/1/2)
- `bridge.rs:419` — `DfTableTrait::supports_filters_pushdown(...)` writes `i32` discriminants into the result buffer (3-state filter pushdown). The fixed-len buffer here is sized to `filters.len()` so it is not a cap violation, but the discriminant encoding is.
- `bridge.rs:433` — `DfTableTrait::insert_into(..., insert_op: i32, ...)` — there is already a `DfInsertOp` Diplomat enum at `bridge.rs:102`; the trait should take that.
- `execution_plan.rs:11` — `DfExecutionPlanTrait::emission_type() -> i32` (0/1/2)
- `execution_plan.rs:13` — `DfExecutionPlanTrait::boundedness() -> i32` (0/1)

The Java adapters (`DfScalarUDFAdapter`, `DfAggregateUDFAdapter`, `DfTableAdapter`, `DfExecutionPlanAdapter`) all switch on the matching Java enum to produce these ints; converting to Diplomat enums would let the generated code do the mapping.

### Bridge classes are public when they should be package-private
The rule says bridge classes "Are package-private (`final class *Bridge`)". 8 are `public`:

- `dataframe/DataFrameBridge.java:38`
- `physical_expr/LiteralGuaranteeBridge.java:31`
- `physical_plan/PhysicalExprBridge.java:19`
- `physical_plan/SendableRecordBatchStreamBridge.java:24`
- `logical_expr/LogicalPlanBridge.java:33`
- `logical_expr/LogicalPlanBuilderBridge.java:21`
- `execution/RuntimeEnvBridge.java:15`
- `execution/SessionContextBridge.java:59`

These are public because the public API classes that wrap them (e.g. `DataFrame`) live in different packages and need a way to construct them. Fixing this requires either co-locating the wrapper into the bridge's package or introducing package-private factory hops. Worth doing — the leak lets external code construct or downcast bridges.

### Public methods exposing Diplomat-generated types
- `SessionContextBridge.java:234` — `dfContext()` is `public` and returns `DfSessionContext` (a `generated.*` class). The bridge layer should encapsulate `Df*`; this hands the raw Diplomat handle out.
- `providers/RustCatalogProvider.java:48` — `handle()` returns `DfRustCatalogProvider`.
- `providers/RustTableProvider.java:46` — `handle()` returns `DfRustTableProvider`.

### Public constructors taking internal types
Rule 5 of the encapsulation section ("Constructors taking internal types are package-private"). Verify these are package-private even though the parameter type is:

- `DataFrame.java:56` — public ctor takes `DataFrameBridge`.
- `PhysicalExpr.java:36` — public ctor takes `PhysicalExprBridge`.
- `LogicalPlanBuilder.java:33` — public ctor takes `LogicalPlanBuilderBridge`.

The bridges are currently `public` (see above), so external code can in fact reach these. Fixing the bridge visibility would close the loophole; making the ctor package-private is the orthogonal fix.

### Open-coded `LogicalExprList` encoding
- `bridge.rs:681` — `DfExprBytes::from_exprs()` constructs `DefaultLogicalExtensionCodec` + `serialize_exprs` + `LogicalExprList { ... }.encode_to_vec()` directly. There is now a `encode_exprs()` helper in `table.rs` for exactly this — bridge should call it (and `from_exprs` in turn becomes a one-liner). Also note `from_exprs` is missing the empty-slice short-circuit that `encode_exprs` has.

### Open-coded `FFI_ArrowSchema` export
- `file_format.rs:87` — `FFI_ArrowSchema::try_from(self.schema.as_ref())` + manual `std::ptr::write` instead of `export_schema_to`.
- `file_source.rs:74` — `FFI_ArrowSchema::try_from(schema)` + manual `std::ptr::write` instead of `export_schema_to`.

The rule explicitly says: **"Always use this helper [`export_schema_to`] in any bridge method that exports a schema to a Java-allocated address — do not open-code the pattern."** `udf_common.rs::export_field_as_ffi_schema` is OK because it returns the `FFI_ArrowSchema` rather than writing it to a Java-allocated address — different use case. The earlier draft also flagged `var_provider.rs:75`; that site allocates an empty `FFI_ArrowSchema` for *Java to write into* (Java→Rust import direction), not an export, so it is not a violation.

### Manual upcall plumbing where helpers exist
- `udaf.rs:134-138` — `accumulator()` builds an `ErrorBuffer` directly, calls `accumulator_create`, branches on `acc_id < 0`. This is the shape of `do_counted_upcall` (returns `usize`, ID semantics aside). Could either widen the helper or wrap it. Low impact since stream.rs is similarly excused.

### Inconsistent string conversion
- `flight_sql_test_server.rs:178` — `std::str::from_utf8(name)?` directly instead of `diplomat_str(name)?`. Minor; one-line change.

## Duplication

Patterns repeated across many files that should be extracted into shared helpers. Listed by impact.

### 1. Arrow C Data Interface size constants (5 redeclarations)
`ARROW_SCHEMA_SIZE = 72`, `ARROW_ARRAY_SIZE = 80`, `WRAPPED_ARRAY_SIZE = 152` are private static constants in:
- `DfScalarUDFAdapter.java:23-26`
- `DfAggregateUDFAdapter.java:30-32`
- `DfRecordBatchReaderAdapter.java:104-106`
- `DfFileSourceAdapter.java:23`
- `DfVarProviderAdapter.java:24`

Hoist into `NativeUtil` (or a new `FfiArrowConstants`) as package-private constants.

### 2. Field import/export helpers (full duplication across two adapters, ~120 lines)
`importFieldFromSegment`, `importFieldArray`, `importFieldFromAddress`, `exportFieldToAddress` are defined privately and **identically** in both `DfScalarUDFAdapter.java` (lines 205-263) and `DfAggregateUDFAdapter.java` (lines 285-324). They wrap a `Field` in a single-field `Schema`, copy `FFI_ArrowSchema` 72 bytes, and clear the source release callback. Extract to a package-private helper class (e.g. `FfiSchemaUtil`) or to `NativeUtil`.

### 3. Release-callback nullification idiom (~13 sites)
The `copyFrom(...)` + `segment.set(ValueLayout.ADDRESS, 56|64, MemorySegment.NULL)` pattern appears in:
- `DfFileSourceAdapter.java:53`
- `DfScalarUDFAdapter.java:108-109, 152-153, 238, 261`
- `DfVarProviderAdapter.java:70`
- `DfAggregateUDFAdapter.java:274-275, 308, 322`
- `DfRecordBatchReaderAdapter.java:71, 81`

Two helpers — `importSchemaFromSegment(srcSegment)` and `importArrayFromSegment(srcSegment)` — would encapsulate the offset constants (56 vs 64) and the copy+nullify pattern. The auto-memory note about offset 56 vs 64 confusion (`CLAUDE.md` was wrong about offset 64 for ArrowSchema) is exactly the kind of footgun a helper would prevent.

### 4. UDF/UDAF trait method overlap
`DfScalarUdfTrait` and `DfAggregateUdfTrait` in `bridge.rs` share four identical method signatures: `name_raw`, `volatility`, `return_field`, `coerce_types`. If Diplomat supports trait composition this could be a shared base; otherwise at minimum the docstrings should be deduplicated or kept in sync via comments.

### 5. Boilerplate `Linker` + `lib.find().orElseThrow()` + `downcallHandle()` setup
Repeated in `NativeUtil.java:31-60` (8 handles) and `DfTableAdapter.java:52-68` (3 handles, for the `DfLazyRecordBatchStream_*` symbols). A small helper `createDowncallHandle(String name, FunctionDescriptor)` would compress each from 4 lines to 1.

### 6. Bridge `DfError` → `DataFusionException` conversion
The documented pattern (`try (e) { throw new DataFusionException(e.toDisplay()); }`) is implemented inline in 50+ methods across all bridge classes, often via a helper called `NativeDataFusionError` rather than `DataFusionException` (which deviates from the rule's documented snippet). Worth either:
- updating the rule to reflect the actual `NativeDataFusionError` wrapper, OR
- introducing a single `BridgeUtil.unwrap(Supplier<T>)` helper that all sites use.

### 7. `DfStringArray` try-with-resources iteration in bridges
Inline `try (DfStringArray names = ...) { for (...) names.get(i, ...); }` blocks in `SessionConfigBridge.java:43`, `SessionContextBridge.java:613/620/632`, `TaskContextBridge.java:60/67`. Could be a `BridgeUtil.toList(DfStringArray)` helper analogous to `NativeUtil.fromRawStringArray` for the upcall side.

### 8. `DfExprBytes` byte-extraction idiom
Repeated `Arena.ofConfined()` + `arena.allocate(len)` + `exprBytes.copyTo(addr, len)` + `toArray(JAVA_BYTE)` blocks in `LiteralGuaranteeBridge.java:107` (private `readExprBytes`), `LogicalPlanBridge.java:426` (private `readRawBytes`, used at 133/172/408/418), and `SessionContextBridge.java:256-263` (inline). Promote `readRawBytes` to a shared package-private helper.

## Things that look like violations but aren't

- **`DfScalarUDFAdapter.java:153`** — clearing offset 64 (private_data) on an `ArrowSchema` during *export to Rust*. The auto-memory entry "Arrow C Data Interface: Release vs Private_data Offsets" explicitly notes this is acceptable in the export direction because `ArrowSchema.close()` does not call the release callback. Not a bug.
- **`duckdb_factory.rs::RustDuckDbMode`** — one agent flagged this as missing `#[diplomat::enum_convert]`, but the attribute is present at line 17.
- **`table.rs:131` `result_buf = vec![0i32; result_cap]`** — sized to `filters.len()`, not a hardcoded cap. The discriminant *encoding* is the violation, not the buffer size.

## ArchUnit gaps

`DiplomatEncapsulationTest` enforces rules 2/3/4 of the encapsulation section (no `java.lang.foreign` in public API, no `MemorySegment` in signatures, `NativeLoader` confined). It does **not** enforce:
- Rule 5 (constructors taking internal types must be package-private).
- "Bridge classes are package-private."
- "Public API classes do not return Diplomat-generated `Df*` types."

Adding ArchUnit rules for these would have caught several of the violations above.
