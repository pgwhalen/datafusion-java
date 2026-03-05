# Proposed Diplomat Java Backend Features

This document describes features that, if added to the Diplomat Java backend, would allow
further simplification of `bridge.rs`. The improvements in this section cannot be implemented
with the current Diplomat toolchain.

---

## Feature 1: `Result<T, E>` Returns from Trait Methods (Highest Priority)

**Problem**: Trait methods that can fail currently use `(error_addr: usize, error_cap: usize)`
out-parameters to report errors back to Rust. This is verbose, error-prone, and forces Rust
to manage error buffer lifetimes manually.

**Affected methods** (8 methods across 4 traits):

```rust
// DfSchemaTrait
fn table(&self, name: &DiplomatStr, error_addr: usize, error_cap: usize) -> usize;

// DfTableTrait
fn scan(&self, session_addr: usize, filters: &[u8], projection: &[u32],
        limit: i64, error_addr: usize, error_cap: usize) -> usize;
fn supports_filters_pushdown(&self, filters: &[u8],
                             result_addr: usize, result_cap: usize,
                             error_addr: usize, error_cap: usize) -> i32;

// DfFileSourceTrait
fn create_file_opener(&self, schema_addr: usize, projection: &[u32],
                      limit: i64, batch_size: i64,
                      error_addr: usize, error_cap: usize) -> usize;

// DfFileOpenerTrait
fn open(&self, path: &DiplomatStr, file_size: i64, range_start: i64, range_end: i64,
        error_addr: usize, error_cap: usize) -> usize;

// DfExecutionPlanTrait (hypothetical future trait)
fn execute(&self, partition: i32, error_addr: usize, error_cap: usize) -> usize;
```

**Desired signature**:

```rust
fn table(&self, name: &DiplomatStr) -> Result<usize, Box<DfError>>;
fn scan(&self, session_addr: usize, filters: &[u8], projection: &[u32],
        limit: i64) -> Result<usize, Box<DfError>>;
fn open(&self, path: &DiplomatStr, file_size: i64, range_start: i64,
        range_end: i64) -> Result<usize, Box<DfError>>;
```

**Diplomat status**: Trait methods explicitly cannot return `Result<T, E>`. The Java backend
already handles `Result` for regular opaque methods — the gap is only in the trait vtable
code-generation path.

**Implementation notes**: The vtable runner functions would need to handle the Result
discriminant. On success, extract the `T` value; on error, convert the error to a Rust
`DataFusionError` and return it. This requires a defined ABI for the vtable entry return type
when the logical return is `Result<T, E>`.

**Impact**: Eliminates all `error_addr: usize, error_cap: usize` parameters from trait methods
and all `Errors.writeException(errorAddr, errorCap, e, fullStackTrace)` calls in adapter classes.
Approximately 8 parameter pairs and 6 error-writing call sites removed.

---

## Feature 2: `DiplomatWrite` in Trait Method Return Types (High Priority)

**Problem**: Trait methods that return strings to Rust currently use the
`(buf_addr: usize, buf_cap: usize) -> i64` workaround: Rust allocates a buffer, passes its
address and capacity to Java, Java writes the string into it, and returns the byte count (or -1
on error).

**Affected methods** (5 methods across 5 traits):

```rust
pub trait DfScalarUdfTrait {
    fn name_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
}
pub trait DfFileFormatTrait {
    fn extension_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
}
pub trait DfFileSourceTrait {
    fn file_type_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
}
pub trait DfCatalogTrait {
    fn schema_names_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
}
pub trait DfSchemaTrait {
    fn table_names_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
}
```

**Desired signature** (using `DiplomatWrite`):

```rust
pub trait DfCatalogTrait {
    fn schema_names(&self, write: &mut DiplomatWrite);
}
pub trait DfScalarUdfTrait {
    fn name(&self, write: &mut DiplomatWrite);
}
```

**Diplomat status**: `DiplomatWrite` is supported as a return mechanism for regular opaque
methods (via `write: &mut DiplomatWrite` parameter). It is not currently supported as a
parameter in trait methods.

**Implementation notes**: In the Java interface generator, `&mut DiplomatWrite` parameters in
trait methods would generate a `String` return type in the Java interface (similar to how regular
methods already handle `DiplomatWrite` returns). The generated Java trait runner would allocate
a `DiplomatWrite`, call the Java method, and write the resulting string into it.

**Impact**: Eliminates 5 `*_to(buf_addr, buf_cap) -> i64` trait methods and their corresponding
manual buffer-write logic in 5 Java adapter classes. Also removes `NativeUtil.writeStrings()`.

---

## Feature 3: `Option<T>` Returns from Trait Methods (Medium Priority)

**Problem**: Trait methods that return "found or not found" use `usize` with 0 meaning
"not found":

```rust
pub trait DfCatalogTrait {
    fn schema(&self, name: &DiplomatStr) -> usize;  // 0 = not found
}
pub trait DfSchemaTrait {
    fn table(&self, name: &DiplomatStr, error_addr: usize, error_cap: usize) -> usize;  // 0 = not found
}
```

**Desired signature**:

```rust
pub trait DfCatalogTrait {
    fn schema(&self, name: &DiplomatStr) -> Option<usize>;
}
pub trait DfSchemaTrait {
    fn table(&self, name: &DiplomatStr) -> Result<Option<usize>, Box<DfError>>;
}
```

**Diplomat status**: `Option<T>` returns are not currently supported for trait methods.

**Impact**: Makes the "not found" semantic explicit. Currently the 0-means-null convention must
be documented and manually maintained in both Rust and Java. With `Option`, it becomes
type-safe and self-documenting.

---

## Feature 4: Slices of Opaque Types as Method Parameters (Lower Priority)

**Problem**: Methods that pass multiple expressions currently serialize them to protobuf bytes
(`&[u8]`). If expressions could be passed as `&[&DfExpr]`, the protobuf serialization layer
could be eliminated entirely.

**Example**:

```rust
// Current: protobuf bytes
pub fn filter_bytes(&self, bytes: &[u8]) -> Result<Box<DfDataFrame>, Box<DfError>>

// Desired: direct expression references
pub fn filter(&self, expr: &DfExpr) -> Result<Box<DfDataFrame>, Box<DfError>>
pub fn select(&self, exprs: &[&DfExpr]) -> Result<Box<DfDataFrame>, Box<DfError>>
```

**Diplomat status**: Slices of opaque types (`&[&DfExpr]`) are not currently supported in any
Diplomat backend.

**Impact**: Would eliminate the entire protobuf dependency from the FFI layer. This is a
significant design change, not just a code-generation improvement. Requires exposing `Expr` as
a Diplomat opaque and supporting `&[&T]` slice parameters.

---

## Summary

| Feature | Priority | Methods Affected | Unsafe/Boilerplate Eliminated |
|---------|----------|-----------------|-------------------------------|
| `Result` in trait returns | Highest | 8 methods | ~16 error params, 6 error-write sites |
| `DiplomatWrite` in trait params | High | 5 methods | 5 buffer-write helpers |
| `Option` in trait returns | Medium | 2 methods | Null-check conventions |
| `&[&T]` slice params | Low | 10+ methods | Entire protobuf layer |
