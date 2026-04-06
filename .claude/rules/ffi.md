---
paths:
  - "datafusion-ffi-java/src/main/java/**/*.java"
  - "datafusion-ffi-native/src/**/*.rs"
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

Diplomat-generated methods throw `DfError` (an opaque that implements `AutoCloseable`). Bridge classes catch and convert:

```java
try {
    DfPhysicalExpr dfExpr = DfPhysicalExpr.fromProtoFilters(filterBytes, schemaAddr);
    return new PhysicalExprBridge(dfExpr);
} catch (DfError e) {
    try (e) {
        throw new DataFusionException(e.toDisplay());
    }
}
```

#### Upcalls (Rust → Java callbacks)

Adapter methods write error strings to Rust-provided buffers using `Errors.writeException()`:

```java
} catch (Exception e) {
    Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
    return 0;
}
```

### Proto Byte Passing

Expressions are serialized as protobuf bytes (`LogicalExprList`) and passed as `&[u8]` Diplomat slices. The `DfExprBytes` opaque wraps `Vec<u8>` for returning bytes from Rust.

To extract bytes from `DfExprBytes`:
```java
try (DfExprBytes exprBytes = ...) {
    long len = exprBytes.len();
    try (Arena arena = Arena.ofConfined()) {
        MemorySegment buf = arena.allocate(len);
        exprBytes.copyTo(buf.address(), len);
        byte[] bytes = buf.toArray(ValueLayout.JAVA_BYTE);
    }
}
```

### Encapsulation Rules

Enforced by `DiplomatEncapsulationTest` (ArchUnit):

1. **Diplomat-generated classes live in the `generated` subpackage** — All `Df*`, `DiplomatLib`, `OwnedSlice` classes are in `org.apache.arrow.datafusion.generated`
2. **No `java.lang.foreign` in public API classes** — FFM types only in internal/generated classes
3. **No `MemorySegment` in public API signatures** — No public method/constructor/field uses `MemorySegment`
4. **`NativeLoader` confined to internal classes** — Only Diplomat-generated and bridge classes may reference it
5. **Constructors taking internal types are package-private** — `*Ffi`, `*Bridge` parameters require package-private constructors

### Rust Helpers

#### `do_returning_upcall` (`upcall_utils.rs`)

Most Rust→Java upcalls follow the same pattern: allocate an error buffer, call Java, check for null pointer, reconstruct `Box<T>` on success. **Use `do_returning_upcall`** for any upcall that returns a raw pointer (0 on error):

```rust
use crate::upcall_utils::do_returning_upcall;

let boxed = do_returning_upcall::<DfExecutionPlan>(
    "Java scan callback failed",
    Box::new(|ea, ec| self.inner.scan(session_addr, ..., ea, ec)),
)?;
```

The `Upcall` type alias documents the closure signature: `(error_addr: usize, error_cap: usize) -> usize`. The function creates the `ErrorBuffer` internally, checks for null, and returns `Result<Box<T>, DataFusionError>`. The `unsafe` for `Box::from_raw` is contained within the function since the null check is the only meaningful validation.

#### `do_upcall` (`upcall_utils.rs`)

For upcalls that return 0 on success, non-zero on error (no pointer reconstruction needed). Returns `Result<(), DataFusionError>`:

```rust
use crate::upcall_utils::do_upcall;

do_upcall("Java return_field failed", |ea, ec| {
    self.inner.return_field(arg1, arg2, ea, ec)
})?;
```

#### `do_counted_upcall` (`upcall_utils.rs`)

For upcalls that return a non-negative count on success, or negative on error. Returns `Result<usize, DataFusionError>`:

```rust
use crate::upcall_utils::do_counted_upcall;

let count = do_counted_upcall("Java coerce_types failed", |ea, ec| {
    self.inner.coerce_types(arg1, arg2, result_addr, result_cap, ea, ec)
})?;
```

#### `do_option_returning_upcall` (`upcall_utils.rs`)

For upcalls that return a raw pointer where null is valid (means "not found"), and only a populated error buffer indicates failure. Returns `Result<Option<Box<T>>, DataFusionError>`:

```rust
use crate::upcall_utils::do_option_returning_upcall;

let boxed = do_option_returning_upcall::<DfTableProvider>(
    "Java SchemaProvider.table() failed",
    Box::new(|ea, ec| self.inner.table(name_addr, name_len, ea, ec)),
)?;
// boxed is Option<Box<DfTableProvider>>
```

For upcalls with non-standard error handling (e.g., `stream.rs` with tri-state returns), use `ErrorBuffer` directly:

```rust
use crate::upcall_utils::ErrorBuffer;

let err = ErrorBuffer::new();
let ptr = self.inner.some_callback(arg1, arg2, err.addr(), err.cap());
if ptr == 0 {
    let msg = err.read();
    if !msg.is_empty() { return Err(...); }
    return Ok(None);
}
```

#### `encode_exprs` (`table.rs`)

Serializes `&[Expr]` to protobuf `LogicalExprList` bytes for passing to Java. Returns empty `Vec` for empty input. Use this instead of manually creating a `DefaultLogicalExtensionCodec` + `serialize_exprs` + `LogicalExprList` + `encode_to_vec`:

```rust
let filter_bytes = encode_exprs(filters)?;
// Pass to Java as (addr, len) pair:
self.inner.callback(filter_bytes.as_ptr() as usize, filter_bytes.len(), ...);
```

### NativeUtil (Java)

Contains adapter helpers used by `Df*Adapter` classes:

| Method                                    | Purpose                                  |
|-------------------------------------------|------------------------------------------|
| `readString(long, long)`                  | Read UTF-8 string from raw address       |
| `readU32s(long, long)`                    | Read u32 array from raw address          |
| `readBytes(long, long)`                   | Read byte array from raw address         |
| `toRawStringArray(List<String>)`          | Java → Rust: string list via DfStringArray |
| `fromRawStringArray(long)`                | Rust → Java: string list via DfStringArray |

#### `toRawStringArray` — Java → Rust string list

Creates a `DfStringArray`, pushes strings, and returns a raw pointer. Rust consumes it via `DfStringArray::take_from_raw()`:

```java
// In a Df*Adapter method returning strings to Rust:
return NativeUtil.toRawStringArray(provider.schemaNames());
```

#### `fromRawStringArray` — Rust → Java string list

Reads strings from a raw `DfStringArray` pointer passed by Rust, then destroys it (takes ownership). Uses native method handles directly since the generated `DfStringArray` constructor is package-private to the `generated` package:

```java
// In a Df*Adapter method receiving strings from Rust:
List<String> colNames = NativeUtil.fromRawStringArray(colNamesPtr);
```

Rust side to create the pointer:

```rust
use crate::bridge::ffi::DfStringArray;
let arr = Box::new(DfStringArray { strings: col_names });
let ptr = Box::into_raw(arr) as usize;
// Pass ptr to Java — Java takes ownership and destroys it
```

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
