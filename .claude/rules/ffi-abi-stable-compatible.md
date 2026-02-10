# FFI: Direct Construction of `datafusion-ffi` Structs

This document describes the strategy for Java to directly construct the `FFI_*` structs defined in the upstream `datafusion-ffi` crate, rather than going through custom intermediate Rust callback structs. This approach was proven with `FFI_RecordBatchStream` and `FFI_ExecutionPlan` and should be applied across the entire codebase.

## Motivation

The original FFI design used a two-layer bridge:

```
Java interface impl
  → Java Handle (creates upcall stubs, populates JavaXxxCallbacks)
    → Rust JavaXxxCallbacks struct (custom, defined in java_provider.rs)
      → Rust JavaBackedXxx wrapper (implements DataFusion trait by calling callbacks)
        → constructs FFI_Xxx (from datafusion-ffi)
          → ForeignXxx (datafusion-ffi's consumer of FFI_Xxx)
```

Every DataFusion trait required three custom Rust types: a callbacks struct, a wrapper struct, and glue to convert between them. This duplicated logic already present in `datafusion-ffi` and diverged from the upstream interfaces that `ForeignExecutionPlan`, `FFI_RecordBatchStream`, etc. are designed to consume.

The new approach eliminates the middle layers:

```
Java interface impl
  → Java Handle (creates upcall stubs, populates FFI_Xxx directly)
    → ForeignXxx (datafusion-ffi's consumer of FFI_Xxx)
```

### Goals

1. **Reuse upstream interfaces** -- `datafusion-ffi` already defines stable FFI structs (`FFI_ExecutionPlan`, `FFI_RecordBatchStream`, `FFI_PlanProperties`, etc.) and their consumer impls (`ForeignExecutionPlan`, etc.). Java should target these directly so that changes in `datafusion-ffi` are automatically reflected.

2. **Reduce bridge code** -- Eliminating custom Rust callback structs, wrapper structs, and conversion functions cuts the per-trait Rust code from ~200 lines to ~0 lines (plus a few shared helpers in `ffi_adapters.rs`).

3. **Stay aligned with DataFusion upstream** -- When DataFusion adds fields to `FFI_ExecutionPlan` or changes `FFI_PlanProperties`, we update Java's struct layout rather than maintaining a parallel Rust adaptation layer.

## Architecture

### What Java constructs directly

Java allocates the `FFI_*` struct in arena memory and writes function pointers (upcall stubs) into each field. The struct is then passed to Rust by copying its bytes into an output buffer (`copyStructTo`).

| Struct | Size | Fields Java fills |
|--------|------|-------------------|
| `FFI_RecordBatchStream` | 32 bytes | `poll_next`, `schema`, `release` fn ptrs + `private_data` |
| `FFI_ExecutionPlan` | 64 bytes | `properties`, `children`, `name`, `execute`, `clone`, `release` fn ptrs + `private_data` + `library_marker_id` |

### What requires Rust helpers

Some `datafusion-ffi` and `abi_stable` types have internal invariants that Java cannot satisfy by writing raw bytes. For these, thin Rust helpers in `ffi_adapters.rs` construct the type and write it into a Java-provided buffer:

| Type | Why Java can't construct it | Rust helper |
|------|----------------------------|-------------|
| `RString` | Internal heap pointer + vtable from `abi_stable` | `datafusion_create_rstring(ptr, len, out)` |
| `RVec<T>` | Internal heap pointer + vtable from `abi_stable` | `datafusion_create_empty_rvec_plan(out)` |
| `FFI_PlanProperties` | Function-pointer-based struct backed by `Arc<PlanProperties>` | `datafusion_create_ffi_plan_properties(partitioning, emission, boundedness, schema, out)` |

### What Java writes as raw bytes

For types that are `#[repr(C)]` or `#[repr(u8)]` with no hidden invariants, Java writes discriminants and payloads directly:

| Type | How Java constructs it |
|------|----------------------|
| `FfiPoll<ROption<FFIResult<WrappedArray>>>` (176 bytes) | Write discriminant bytes at offsets 0/8/16, then Arrow C Data structs at offset 24 |
| `FFIResult<FFI_RecordBatchStream>` (40 bytes) | Write RResult discriminant at offset 0, then stream bytes or RString at offset 8 |
| `WrappedSchema` (72 bytes) | Identical to `FFI_ArrowSchema` -- Java already constructs these |
| `FFI_ArrowArray` / `FFI_ArrowSchema` | Standard Arrow C Data Interface, exported via `arrow-c-data` Java library |

## Pattern: copyStructTo

The Handle class allocates the FFI struct in its arena and populates it with upcall stub pointers. When a caller needs to hand the struct to Rust (e.g., the `scan` callback writes an `FFI_ExecutionPlan` into Rust stack memory), it copies the raw bytes:

```java
void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_EXECUTION_PLAN_SIZE).copyFrom(ffiPlan);
}
```

This replaces the old `setToPointer` pattern that wrote a pointer to a Rust-allocated callback struct. With `copyStructTo`, no Rust allocation is needed -- the struct lives entirely in Java arena memory.

## Safety Mechanisms

### Runtime size validation

Java hardcodes struct sizes as constants and validates them against Rust at first use:

```java
NativeUtil.validateSize(
    FFI_EXECUTION_PLAN_SIZE,
    DataFusionBindings.FFI_EXECUTION_PLAN_SIZE,  // calls datafusion_ffi_execution_plan_size()
    "FFI_ExecutionPlan");
```

Each `datafusion-ffi` struct has a corresponding Rust `#[no_mangle]` size helper. If a `datafusion-ffi` upgrade changes a struct size, the mismatch is caught immediately at runtime.

### Discriminant offset tests

Rust unit tests in `ffi_adapters.rs` verify discriminant offsets by constructing each variant and inspecting raw bytes:

```rust
let end_of_stream: FfiPoll<ROption<FFIResult<WrappedArray>>> =
    FfiPoll::Ready(ROption::RNone);
let bytes = unsafe { std::slice::from_raw_parts(&end_of_stream as *const _ as *const u8, ...) };
assert_eq!(bytes[0], 0, "FfiPoll::Ready discriminant should be 0");
assert_eq!(bytes[8], 1, "ROption::RNone discriminant should be 1");
```

### Library marker ID

Java-backed `FFI_ExecutionPlan` structs use a dedicated library marker (`datafusion_java_marker_id`) that differs from the one used by `datafusion-ffi` itself. This prevents `TryFrom<&FFI_ExecutionPlan>` from taking the same-library fast-path, which would try to downcast `private_data` to an internal Rust type.

### Eager construction of function-pointer-based return values

When a callback return type contains function pointers (e.g., `FFI_PlanProperties`), the Handle must build it eagerly in the constructor rather than lazily in the callback. If construction fails (e.g., `plan.schema()` throws), the error propagates immediately. Returning zeroed bytes would leave null function pointers that cause SIGSEGV when DataFusion calls them later.

## Remaining Work

The current `RecordBatchReaderHandle` and `ExecutionPlanHandle` implementations are proof-of-concept. Significant refactoring is needed before applying this pattern across the entire codebase:

### Java FFM utilities for abi_stable types

The current code manually writes discriminant bytes at hardcoded offsets. This should be extracted into reusable Java utilities:

- **`RResult<T, E>` builder** -- Encapsulate the discriminant-at-0, padding, payload-at-8 pattern for constructing `FFIResult` and other `RResult` instances.
- **`FfiPoll<T>` builder** -- Encapsulate the three-level discriminant layout (FfiPoll/ROption/RResult) used by `poll_next`.
- **`RString` wrapper** -- The `NativeUtil.writeRString` helper works but should be part of a more complete set of abi_stable type utilities.
- **`RVec<T>` helpers** -- Currently only supports empty vecs. Will need parameterized construction if Java-backed plans ever have children.

### StructLayout definitions

The current code defines struct layouts as sequences of `JAVA_LONG` to satisfy alignment. These should be centralized, possibly auto-generated from the Rust size helpers, so that adding a new FFI struct type is mechanical rather than requiring manual offset calculations.

### Reduce per-Handle boilerplate

Each Handle class currently has ~15 static fields for MethodHandles and FunctionDescriptors. The init pattern (`initXxxMethodHandle()` with try/catch wrapping `MethodHandles.lookup().findVirtual(...)`) is identical across all handles. This should be extracted into a shared utility.

### Apply to remaining callback chains

The callback chains that still use the old `JavaXxxCallbacks` + Rust wrapper pattern should be migrated:

| Chain | Current | Target |
|-------|---------|--------|
| `FileOpener` | `JavaFileOpenerCallbacks` + Rust `JavaBackedFileOpener` | Java constructs... what? FileOpener returns `FFI_RecordBatchStream` which Java already constructs directly. The callback struct itself (`open_fn` + `release_fn`) is still Rust-allocated. |
| `FileSource` | `JavaFileSourceCallbacks` + Rust `JavaBackedFileSource` | Similar -- evaluate whether the callback struct can be eliminated. |
| `CatalogProvider` / `SchemaProvider` / `TableProvider` | `JavaXxxCallbacks` + Rust wrappers | These callback chains return pointers to other callback structs (e.g., `schema_fn` returns `*mut JavaSchemaProviderCallbacks`). Migrating these requires determining whether `datafusion-ffi` provides equivalent FFI structs for catalogs/schemas/tables, or whether the current callback-struct pattern is the right fit for these. |

Not all callback chains will benefit equally from this migration. The biggest wins are for types where `datafusion-ffi` already defines a consumer (like `ForeignExecutionPlan` for `FFI_ExecutionPlan`). For types where no upstream FFI struct exists, the current callback pattern may remain appropriate.
