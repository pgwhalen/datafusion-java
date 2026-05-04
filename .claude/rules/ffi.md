---
paths:
  - "datafusion-ffi-java/**/*.java"
  - "datafusion-ffi-native/**/*.rs"
---

# FFI Architecture (Diplomat-Based)

This document describes the FFI patterns used in `datafusion-ffi-java` and `datafusion-ffi-native`. All native calls go through Diplomat-generated bindings.

## Architecture Overview

```
User Java Code
    ↓
Public API (SessionContext, DataFrame, PhysicalExpr, LiteralGuarantee, ...)
    ↓
*Bridge classes (SessionContextBridge, DataFrameBridge, PhysicalExprBridge, ...)
    ↓
Diplomat-generated classes (DfSessionContext, DfDataFrame, DfPhysicalExpr, ...)
    ↓  (FFM downcalls)
Rust bridge.rs (#[diplomat::bridge] opaques)
    ↓
DataFusion Rust library
```

For callbacks (Java-implemented providers):

```
DataFusion Rust library
    ↓
Rust Foreign* structs (catalog.rs, table.rs, plan.rs, etc.)
    ↓  (Diplomat trait upcalls)
Diplomat-generated trait interfaces (DfCatalogTrait, DfTableTrait, ...)
    ↓
Df*Adapter classes (DfCatalogAdapter, DfTableAdapter, ...)
    ↓
User Java interfaces (CatalogProvider, TableProvider, ...)
```

## Diplomat Rules

### Naming Conventions

| Pattern                        | Naming                                  |
|--------------------------------|-----------------------------------------|
| Diplomat opaque (Rust)         | `DfFoo` in `bridge.rs`                 |
| Diplomat trait (Rust)          | `DfFooTrait` in `bridge.rs`            |
| Diplomat-generated Java class  | `DfFoo` (public, in `generated` subpackage) |
| Bridge class                   | `FooBridge` (package-private)          |
| Adapter class                  | `DfFooAdapter` (package-private)       |
| Converter class                | `FooConverter` (package-private)       |
| Public API class               | `Foo` (public)                         |

### Enum Types

**NEVER return an integer value to represent an enumerated type across the FFI boundary.** Diplomat supports enums natively — always define a Diplomat enum in `bridge.rs` and let Diplomat generate the corresponding Java enum. This produces self-documenting generated code instead of opaque integer values whose meaning is only conveyed by comments.

#### Bad: integer discriminant

```rust
// bridge.rs — DON'T do this
impl DfFoo {
    // Returns 0=Bounded, 1=Unbounded, 2=Unknown
    pub fn get_kind(&self) -> i32 { ... }
}
```

#### Good: Diplomat enum

```rust
// bridge.rs — DO this
#[diplomat::enum_convert(datafusion::prelude::FooKind)]
pub enum DfFooKind {
    Bounded,
    Unbounded,
    Unknown,
}

impl DfFoo {
    pub fn get_kind(&self) -> DfFooKind { ... }
}
```

The same rule applies to trait callbacks (upcalls): use a Diplomat enum parameter instead of an integer discriminant.

### Bridge Class Pattern

Every public API class that wraps a native resource delegates to a package-private `*Bridge` class, which wraps a Diplomat-generated `Df*` opaque:

```
SessionContext → SessionContextBridge → DfSessionContext
DataFrame → DataFrameBridge → DfDataFrame
PhysicalExpr → PhysicalExprBridge → DfPhysicalExpr
```

Bridge classes:
- Are package-private (`final class *Bridge`)
- Implement `AutoCloseable` (delegating to the Diplomat opaque's `close()`)
- Handle error conversion: catch `DfError`, extract message via `e.toDisplay()`, throw `DataFusionException`
- May contain factory methods (e.g., `PhysicalExprBridge.fromProtoFilters()`)

### Adapter Class Pattern

For callback interfaces (Java → Rust upcalls), adapter classes translate between user-facing Java interfaces and Diplomat-generated trait interfaces:

```
TableProvider → DfTableAdapter → DfTableTrait
CatalogProvider → DfCatalogAdapter → DfCatalogTrait
SchemaProvider → DfSchemaAdapter → DfSchemaTrait
```

Adapter classes:
- Are package-private (`final class Df*Adapter implements Df*Trait`)
- Read raw memory parameters using `NativeUtil.readBytes()`, `readU32s()`, etc.
- Write errors using `Errors.writeException()`
- Convert between Java enums and FFI integer discriminants

### Error Handling

#### Downcalls (Java → Rust)

Diplomat-generated methods throw `DfError` (an opaque that implements `AutoCloseable`). Bridge classes route every downcall through `BridgeUtil.unwrap(context, () -> ...)` (see the BridgeUtil section below), which catches `DfError`, closes it, and wraps it in `NativeDataFusionError`. Do not open-code the `try { ... } catch (DfError e) { try (e) { throw ...; } }` pattern.

#### Upcalls (Rust → Java callbacks)

Adapter methods write error strings to Rust-provided buffers using `Errors.writeException()`:

```java
try {
    // ... trait method body ...
    return 0;
} catch (Exception e) {
    Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
    return -1;
}
```

### No Fixed-Capacity Buffers

**NEVER bake an arbitrary fixed capacity into an FFI signature** (e.g. `(buf_addr, buf_cap) -> bytes_written`). The cap is an internal binding detail that silently truncates user data, and no value is defensible.

For Java→Rust strings via a trait callback, return a `DfStringArray` raw pointer (`NativeUtil.toRawStringArray` on the Java side, `DfStringArray::take_from_raw` on the Rust side). The container owns its own allocation, so there is no cap to pick.

`ErrorBuffer` (32 KiB) is the one exception: it is a diagnostic channel, and truncated stack traces are tolerable. Anything carrying user-visible payload must size itself dynamically.

### No Custom Byte Encoding

**NEVER invent ad-hoc binary wire formats** (length-prefixed lists, hand-rolled tag/value layouts, custom struct packing) to move data across the FFI boundary. This produces fragile parsers on both sides, bypasses Diplomat's type system, and the meaning of the bytes is only conveyed by comments.

#### Bad: hand-rolled length-prefixed encoding

```rust
// DON'T do this — wire format documented only in a comment
// Wire format: `[count:4 LE][len:4 LE][proto_bytes]...`
fn decode_scalar_value_list(bytes: &[u8]) -> DFResult<Vec<ScalarValue>> {
    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let mut offset = 4;
    // ... manual offset arithmetic, truncation checks, etc.
}
```

#### Good: use existing typed mechanisms

Pick the right tool for the data:
- **Lists of Diplomat opaques** — return a `Df*Array` opaque (see `DfStringArray` pattern) or have Java call back to fetch by index
- **Structured values (single or list)** — serialize with the existing protobuf codec (`encode_exprs`, `ScalarValueProtoConverter`, etc.) and pass the bytes as a single `&[u8]` slice
- **Repeated callbacks** — define a Diplomat trait method that Rust calls once per element

If none of those fit, add a new Diplomat opaque or trait rather than inventing a byte layout.

### Proto Byte Passing

Expressions are serialized as protobuf bytes (`LogicalExprList`) and passed as `&[u8]` Diplomat slices. The `DfExprBytes` opaque wraps `Vec<u8>` for returning bytes from Rust. To extract those bytes on the Java side, use `BridgeUtil.toBytes(DfExprBytes)` (see the BridgeUtil section below) — do not open-code the `Arena` + `copyTo` + `toArray` dance.

### Encapsulation Rules

Enforced by `DiplomatEncapsulationTest` (ArchUnit):

1. **Diplomat-generated classes live in the `generated` subpackage** — All `Df*`, `DiplomatLib`, `OwnedSlice` classes are in `org.apache.arrow.datafusion.generated`
2. **No `java.lang.foreign` in public API classes** — FFM types only in internal/generated classes
3. **No `MemorySegment` in public API signatures** — No public method/constructor/field uses `MemorySegment`
4. **`NativeLoader` confined to internal classes** — Only Diplomat-generated and bridge classes may reference it
5. **Constructors taking internal types are package-private** — `*Ffi`, `*Bridge` parameters require package-private constructors

### Rust Helpers

A handful of Rust modules centralize patterns that show up across every `Foreign*` wrapper. Reach for them rather than open-coding the equivalent — the doc comment on each item describes its precise contract.

#### `arrow_ffi_util.rs` — Arrow C Data Interface (Rust side)

See @datafusion-ffi-native/src/arrow_ffi_util.rs.

The Rust mirror of the Java-side `ArrowFfiUtil`. The single home for code that constructs, exports, or imports `FFI_ArrowSchema` / `FFI_ArrowArray` structs across the boundary — `export_schema_to` (write a schema into a Java-allocated address), `export_field_as_ffi_schema` and friends (single-field-Schema convention used for passing `Field`s), `export_arrays_as_ffi`, `import_field_from_ffi_schema`. Reach for this any time you are touching an Arrow C Data Interface struct so the `try_from` / `to_ffi` / `unsafe { std::ptr::write }` mechanics stay confined here.

#### `upcall_utils.rs` — Rust→Java trait upcalls

Every upcall does the same dance: allocate an `ErrorBuffer`, hand its address + capacity to Java, invoke the trait method, then translate the result back into a `Result`. `do_returning_upcall`, `do_option_returning_upcall`, `do_option_upcall`, `do_upcall`, `do_counted_upcall`, and `do_optional_upcall` cover the standard return-code conventions; pick the one whose contract matches the trait method you're calling (raw pointer, count, tri-state, etc.). For unusual conventions (e.g., the `stream.rs` tri-state path), use `ErrorBuffer` directly. **Always go through these helpers** so the `unsafe` for `Box::from_raw` and the error-buffer plumbing stay in one place.

#### `udf_common.rs` — UDF / UDAF upcall plumbing

UDF-shaped upcall helpers built on top of `arrow_ffi_util` and `upcall_utils`: `upcall_return_field`, `upcall_coerce_types`, plus small conversions like `volatility_from_df` and `decode_scalar_value`. Reach for these from `udf.rs` / `udaf.rs`; reach for the underlying modules elsewhere.

#### `encode_exprs` (`table.rs`) — outgoing expression serialization

The single canonical way to turn `&[Expr]` into protobuf `LogicalExprList` bytes for handing to Java. Use this instead of open-coding `DefaultLogicalExtensionCodec` + `serialize_exprs` + `LogicalExprList::encode_to_vec` — it keeps the codec choice consistent and handles the empty-slice short-circuit.

### Java FFI Util Classes

There are three Java helper classes that sit on three distinct sides of the FFI boundary. They do not overlap; pick the right one based on what you are holding.

#### `NativeUtil` (root package, package-private) — adapter-side raw FFM

See @datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/NativeUtil.java.

The home for **adapter-side FFM primitives**. `Df*Adapter` classes implementing Diplomat trait upcalls receive raw `long` addresses + lengths from Rust and need to (a) read those, (b) bind their own downcall handles for opaques whose generated APIs are out of reach, and (c) hand a `DfStringArray` back as a raw pointer. Reach for `NativeUtil` whenever you are working with raw memory addresses inside an upcall implementation.

#### `BridgeUtil` (`common` package, public) — bridge-side Diplomat helpers

See @datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/common/BridgeUtil.java.

The home for **bridge-side Diplomat-opaque handling**. `*Bridge` classes wrapping Diplomat downcalls hold typed Diplomat opaques (`DfError`, `DfExprBytes`, `DfStringArray`) and need to (a) translate `DfError` into the project's `DataFusionError` hierarchy with a context message, and (b) materialize Diplomat opaques into ordinary Java values. Public because bridges live across packages (`execution`, `logical_expr`, `physical_expr`, ...). Reach for `BridgeUtil` from any `*Bridge` class.

#### `ArrowFfiUtil` (root package, package-private) — Arrow C Data Interface

See @datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/ArrowFfiUtil.java.

The home for **the Arrow C Data Interface spec**. `FFI_ArrowSchema` and `FFI_ArrowArray` have well-defined layouts (sizes, release-callback offsets) and a release-callback ownership protocol that is easy to get wrong. `ArrowFfiUtil` keeps that knowledge in one place: copy + clear-release on import, copy + null-out-private-data on export, and the wrapped-single-field-Schema convention used for passing `Field`s across the boundary. Reach for `ArrowFfiUtil` any time you are touching a `FFI_ArrowSchema` or `FFI_ArrowArray` struct.

#### Choosing between them

- Holding a raw `long` address from an upcall → `NativeUtil`.
- Holding a Diplomat opaque (`Df*`) returned by a downcall → `BridgeUtil`.
- Holding (or producing) an Arrow C Data Interface struct → `ArrowFfiUtil`.

### Build Process

```bash
# 1. Build Rust library (includes bridge.rs → Diplomat proc-macros, generates Java bindings)
./gradlew :datafusion-ffi-java:cargoDevBuild

# 2. Compile Java (includes hand-written + generated code)
./gradlew :datafusion-ffi-java:compileJava

# 3. Run tests
./gradlew :datafusion-ffi-java:test
```

NativeLoader wiring is handled by `clib_initializer` in `diplomat-config.toml`.

## Arrow Data Passing

Arrow schemas and arrays are passed across FFI as raw `usize` addresses pointing to `FFI_ArrowSchema` or `FFI_ArrowArray` structs. Java exports via `Data.exportSchema()` / `Data.exportVector()` and passes the `memoryAddress()`.

**Important:** After Rust reads the schema by reference, Java must call `ffiSchema.release()` then `ffiSchema.close()` to avoid memory leaks.

## Converter Classes

Pure Java proto converters with no FFI dependencies:

- `TableReferenceConverter` — Converts between Java `TableReference` and protobuf
- `ExprProtoConverter` — Converts between Java `Expr` and protobuf `LogicalExprList`
- `ScalarValueProtoConverter` — Converts between Java `ScalarValue` and protobuf
- `ArrowTypeProtoConverter` — Converts between Arrow types and protobuf
