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
Rust providers/*.rs (ForeignCatalog, ForeignTable, etc.)
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

### NativeUtil

Contains only adapter helpers and Diplomat slice readers:

| Method                                    | Used By                                  |
|-------------------------------------------|------------------------------------------|
| `writeStrings(long, long, List<String>)`  | DfCatalogAdapter, DfSchemaAdapter        |
| `readString(long, long)`                  | Adapter classes                          |
| `readU32s(long, long)`                    | DfTableAdapter                           |
| `readBytes(long, long)`                   | DfTableAdapter, DfFileSourceAdapter      |
| `readDiplomatStr(Object)`                 | Diplomat-generated trait code            |
| `readDiplomatBytes(Object)`               | Diplomat-generated trait code            |
| `readDiplomatInts(Object)`                | Diplomat-generated trait code            |

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
