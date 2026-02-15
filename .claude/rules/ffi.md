# FFI Code Patterns

This document describes the patterns used in `datafusion-ffi-java` and `datafusion-ffi-native` for Java-Rust interop via the Foreign Function & Memory (FFM) API.

## Error Handling

### Rust Side: Error Output Pattern (Downcalls)

All Rust FFI functions that can fail should:
1. Accept an `error_out: *mut *mut c_char` parameter as the last argument
2. Return an `i32` status code (0 = success, non-zero = error) OR a pointer (null = error)
3. On error, use the utility functions from `crate::error` to set the error and return

**For i32-returning functions**, use `set_error_return`:
```rust
use crate::error::{clear_error, set_error_return};

#[no_mangle]
pub unsafe extern "C" fn datafusion_do_something(
    ctx: *mut c_void,
    error_out: *mut *mut c_char,  // Always last parameter
) -> i32 {
    clear_error(error_out);  // Always clear first

    if ctx.is_null() {
        return set_error_return(error_out, "Context is null");
    }

    match do_something_fallible() {
        Ok(_) => 0,
        Err(e) => set_error_return(error_out, &format!("Operation failed: {}", e)),
    }
}
```

**For pointer-returning functions**, use `set_error_return_null`:
```rust
use crate::error::{clear_error, set_error_return_null};

#[no_mangle]
pub unsafe extern "C" fn datafusion_create_thing(
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    match create_thing() {
        Ok(thing) => Box::into_raw(Box::new(thing)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Create failed: {}", e)),
    }
}
```

### Rust Side: Callback Error Handling (Upcalls)

When Rust code calls back into Java and receives an error, use the callback error utilities:

**For checking callback results**, use `check_callback_result`:
```rust
use crate::error::check_callback_result;

// In a method that returns Result<T, DataFusionError>
let result = (callbacks.some_fn)(callbacks.java_object, &mut out, &mut error_out);
check_callback_result(result, error_out, "get data from Java")?;
```

**For creating errors directly** (e.g., in match arms), use `callback_error`:
```rust
use crate::error::callback_error;

match result {
    1 => { /* success */ }
    0 => { /* end of stream */ }
    _ => return Poll::Ready(Some(Err(callback_error(
        error_out,
        "load next batch from Java",
    )))),
}
```

Note: Java-allocated error strings (from callbacks) should NOT be freed by Rust - they are managed by Java's arena.

### Java Side: High-Level Error Handling

Use `NativeUtil` high-level methods to combine allocation, invocation, and error checking:

```java
// For void operations (int result, 0 = success):
NativeUtil.call(arena, "Operation name", errorOut ->
    (int) SOME_FUNCTION.invokeExact(arg1, arg2, errorOut));

// For pointer-returning operations:
MemorySegment ptr = NativeUtil.callForPointer(arena, "Operation name", errorOut ->
    (MemorySegment) CREATE_THING.invokeExact(arg, errorOut));

// For stream operations (positive=data, 0=end, negative=error):
int result = NativeUtil.callForStreamResult(arena, "Stream next", errorOut ->
    (int) STREAM_NEXT.invokeExact(stream, out, errorOut));
```

### Java Side: Low-Level Error Handling

When the high-level API doesn't fit (e.g., complex setup in the same arena scope):

```java
try (Arena arena = Arena.ofConfined()) {
    MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);
    // ... other setup ...
    int result = (int) SomeBinding.METHOD.invokeExact(..., errorOut);
    NativeUtil.checkResult(result, errorOut, "operation name");
}
```

## MemorySegment Wrapper Records

Use typed wrapper records to reduce duplication and clarify intent when working with raw MemorySegments in callbacks.

### Input Wrappers

**NativeString** - for C string input parameters:
```java
// Before: name.reinterpret(1024).getUtf8String(0)
// After:
String tableName = new NativeString(name).value();
```

### Output Wrappers

**PointerOut** - for returning pointer values:
```java
// Before: tableOut.reinterpret(8).set(ValueLayout.ADDRESS, 0, ptr)
// After:
new PointerOut(tableOut).set(ptr);
new PointerOut(tableOut).setNull();  // for null case
```

**LongOut** - for returning long values:
```java
// Before: lenOut.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, len)
// After:
new LongOut(lenOut).set(len);
```

**Errors** - for callback return values:
```java
// For the success case:
return Errors.SUCCESS;

// For the error case (preferred pattern):
} catch (Exception e) {
  return Errors.fromException(errorOut, e, arena, fullStackTrace);
}
```

The `fullStackTrace` parameter controls whether error messages include the full Java stack trace. This is configured via `SessionConfig.builder().fullStackTrace(true).build()` or the `FULL_JAVA_STACK_TRACE` environment variable. Full stack traces help trace exactly where exceptions originated in Java callback code.

## Upcall Stub Pattern (Java Callbacks)

When creating Java callbacks that Rust can invoke, follow these patterns:

### Static MethodHandles and FunctionDescriptors

Make `MethodHandle` and `FunctionDescriptor` objects static to enable JIT optimization:

```java
final class SomeHandle implements TraitHandle {
    // Static descriptors - define FFI signatures once at class load
    private static final FunctionDescriptor CALLBACK_DESC =
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);

    // Static method handles - looked up once at class load
    private static final MethodHandle CALLBACK_MH = initCallbackMethodHandle();

    private static MethodHandle initCallbackMethodHandle() {
        try {
            return MethodHandles.lookup().findVirtual(
                SomeHandle.class,
                "callback",
                MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // Instance fields - only upcall stubs are per-instance
    private final MemorySegment callbackStub;

    SomeHandle(Arena arena) {
        // Create upcall stubs bound to this instance
        Linker linker = NativeUtil.getLinker();
        this.callbackStub = linker.upcallStub(CALLBACK_MH.bindTo(this), CALLBACK_DESC, arena);
    }

    @SuppressWarnings("unused")  // Called via upcall stub
    int callback(MemorySegment javaObject, MemorySegment errorOut) {
        try {
            // ... implementation
            return Errors.SUCCESS;
        } catch (Exception e) {
            return Errors.fromException(errorOut, e, arena, fullStackTrace);
        }
    }
}
```

### Rust Callback Structs (Callback Pattern)

When using the callback pattern (see "Handle Class Pattern" below), the Rust side defines callback structs that Java populates:

```rust
#[repr(C)]
pub struct JavaCallbacks {
    pub java_object: *mut c_void,
    pub callback_fn: Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_char) -> i32>,
    pub release_fn: Option<unsafe extern "C" fn(*mut c_void)>,
}
```

### 1:1 Callback-to-Trait-Method Rule (Callback Pattern)

Each function pointer in a `Java*Callbacks` struct must correspond 1:1 to a method on the DataFusion trait **and** the Java interface it mirrors. Do not split one trait method into multiple callbacks, and do not merge multiple trait methods into one callback.

When a trait method returns a compound type (e.g., `properties() -> &PlanProperties`), create an `FFI_*` bridge struct in `java_provider.rs` to carry the data across FFI, and use a single callback that writes to that struct:

```rust
// Bridge struct in java_provider.rs
#[repr(C)]
pub struct FFI_PlanProperties {
    pub output_partitioning: i32,
    pub emission_type: i32,
    pub boundedness: i32,
}

// Single callback in JavaExecutionPlanCallbacks — maps 1:1 to ExecutionPlan::properties()
pub properties_fn: unsafe extern "C" fn(
    java_object: *mut c_void,
    properties_out: *mut FFI_PlanProperties,
),
```

On the Java side, the Handle callback writes to the struct:

```java
void getProperties(MemorySegment javaObject, MemorySegment propertiesOut) {
    PlanProperties props = plan.properties();
    MemorySegment out = propertiesOut.reinterpret(12);
    out.set(ValueLayout.JAVA_INT, 0, props.outputPartitioning());
    out.set(ValueLayout.JAVA_INT, 4, /* enum switch */);
    out.set(ValueLayout.JAVA_INT, 8, /* enum switch */);
}
```

This keeps the callback struct aligned with the trait hierarchy and makes it easy to verify correctness by comparing `Java*Callbacks` fields against DataFusion trait methods.

## Direct FFI Struct Construction

Handle classes that implement upstream `datafusion-ffi` traits (e.g., `ExecutionPlan`, `RecordBatchStream`) use a different pattern from the callback structs above. Instead of defining intermediate Rust callback structs and wrapper types, Java constructs the upstream FFI struct directly in Java arena memory and populates it with upcall stub function pointers. Rust's `TryFrom<&FFI_Xxx>` conversion then produces the final DataFusion trait object.

### Architecture

The callback pattern uses intermediate Rust layers:

```
Java Handle → Rust Java*Callbacks → Rust JavaBacked* wrapper → DataFusion trait
```

Direct struct construction eliminates them:

```
Java Handle → FFI_Xxx (constructed in Java arena) → ForeignXxx (via TryFrom)
```

### When to Use Each Pattern

**Direct struct construction is always preferred** when the upstream `datafusion-ffi` crate defines a corresponding `FFI_*` struct. This eliminates intermediate Rust layers (`Java*Callbacks` + `JavaBacked*` wrapper) and lets Rust's existing `TryFrom<&FFI_Xxx>` do the conversion.

The **callback pattern** is a legacy approach still used by some Handle classes (see "Existing violations" under Handle Class Pattern). It should not be used for new Handle classes unless there is no upstream `FFI_*` struct available in `datafusion-ffi`.

### Struct Layouts, VarHandles, and Runtime Validation

Define the struct as a named `StructLayout` and derive `VarHandle` accessors from it. Validate the layout's size at first construction against Rust size helpers:

```java
private static final StructLayout FFI_STRUCT_LAYOUT =
    MemoryLayout.structLayout(
        ValueLayout.ADDRESS.withName("field_a"),
        ValueLayout.ADDRESS.withName("field_b"),
        ValueLayout.ADDRESS.withName("release"),
        ValueLayout.ADDRESS.withName("private_data"));

private static final VarHandle VH_FIELD_A =
    FFI_STRUCT_LAYOUT.varHandle(PathElement.groupElement("field_a"));
private static final VarHandle VH_FIELD_B =
    FFI_STRUCT_LAYOUT.varHandle(PathElement.groupElement("field_b"));

private static final MethodHandle FFI_STRUCT_SIZE_MH =
    NativeUtil.downcall("datafusion_ffi_struct_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

private static void validateSizes() {
    NativeUtil.validateSize(FFI_STRUCT_LAYOUT.byteSize(), FFI_STRUCT_SIZE_MH, "FFI_Struct");
}
```

Field access uses the `VarHandle` with a `0L` base offset (required by the FFM API in all JDK versions):

```java
VH_FIELD_A.set(ffiStruct, 0L, upcallStubSegment);
```

### Struct-Returning Callbacks

When callbacks must return `abi_stable` or `async-ffi` structs (not simple `int`/`void`), use `StructLayout` descriptors:

```java
private static final StructLayout RESULT_LAYOUT =
    MemoryLayout.structLayout(
        MemoryLayout.sequenceLayout(SIZE / 8, ValueLayout.JAVA_LONG));

private static final FunctionDescriptor CALLBACK_DESC =
    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
```

The callback Java method returns `MemorySegment` (pointing to a pre-allocated buffer).

### Pre-allocated Return Buffers

Allocate return buffers once in the constructor to avoid per-call allocation:

```java
private final MemorySegment resultBuffer;

Handle(Arena arena) {
    this.resultBuffer = arena.allocate(RESULT_SIZE, 8);
}

MemorySegment callback(MemorySegment arg) {
    resultBuffer.fill((byte) 0);
    // ... populate buffer ...
    return resultBuffer;
}
```

### copyStructTo

Since the struct is constructed in Java arena memory (not Rust heap), callers copy the struct bytes into the Rust-side output buffer using `copyStructTo` rather than `setToPointer`:

```java
void copyStructTo(MemorySegment out) {
    out.reinterpret(FFI_STRUCT_SIZE).copyFrom(ffiStruct);
}
```

### Library Marker

`FFI_ExecutionPlan` and `FFI_PlanProperties` have `library_marker_id` fields. Java sets these to a Rust-provided marker function so that `TryFrom` takes the "foreign library" conversion path (not the same-library fast-path):

```java
private static final MemorySegment JAVA_MARKER_ID_FN =
    NativeLoader.get().find("datafusion_java_marker_id").orElseThrow();
```

The marker function is defined in `execution_plan.rs`.

### Discriminant-Based Enum Returns

`abi_stable` enums like `ROption`, `RResult`, and `FfiPoll` use byte discriminants at fixed offsets:

```java
// FfiPoll::Ready(RSome(ROk(payload)))
buffer.set(ValueLayout.JAVA_BYTE, 0, (byte) 0);  // FfiPoll: Ready
buffer.set(ValueLayout.JAVA_BYTE, 8, (byte) 0);  // ROption: RSome
buffer.set(ValueLayout.JAVA_BYTE, 16, (byte) 0); // RResult: ROk
// payload starts at offset 24
```

### `repr(C)` Enum Returns

`datafusion-ffi` enums like `FFI_Partitioning`, `FFI_EmissionType`, and `FFI_Boundedness` use `#[repr(C)]` layout with `i32` discriminants:

```java
// FFI_Partitioning::UnknownPartitioning(count): disc=2(i32) at 0, pad, count(long) at 8
buffer.set(ValueLayout.JAVA_INT, 0, 2);     // UnknownPartitioning discriminant
buffer.set(ValueLayout.JAVA_LONG, 8, count); // payload

// FFI_EmissionType: returned as scalar JAVA_INT (0=Incremental, 1=Final, 2=Both)

// FFI_Boundedness: disc(i32) at 0, requires_infinite_memory(bool) at 4
buffer.set(ValueLayout.JAVA_INT, 0, boundedness); // 0=Bounded, 1=Unbounded
```

### RString Construction

Error messages and string returns use `NativeUtil.writeRString()` which delegates to a Rust helper (`datafusion_create_rstring` in `native_util.rs`) since `RString` has internal structure managed by `abi_stable`.

### Rust-Side Support

Size validation and construction helpers live alongside their corresponding FFI functions:

- **`execution_plan.rs`** -- Size helpers for `FFI_ExecutionPlan`, `FFI_PlanProperties`, `FFI_Partitioning`, `FFI_EmissionType`, `FFI_Boundedness`, and related types. Also provides `datafusion_java_marker_id()` and `datafusion_create_empty_rvec_plan()`.
- **`record_batch_reader.rs`** -- Size helpers for `FFI_RecordBatchStream`, poll result, and wrapped schema types.
- **`native_util.rs`** -- `datafusion_rstring_size()` and `datafusion_create_rstring()` for `abi_stable` string construction.
- **`scalar_value.rs`** -- `datafusion_ffi_scalar_value_size()` for scalar value struct validation.

## Arrow C Data Interface

### Ownership Transfer

When copying Arrow FFI structs across the boundary, the **destination owns the release callback**. Clear the source's release callback after copying to prevent double-free:

```java
// Copy schema to destination
destSchema.copyFrom(srcSchema);
// Clear release in source - dest now owns it
srcSchema.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);  // offset 64 = release callback
```

### Arena Lifetime

Catalogs and providers that Rust holds references to must use shared arenas:

```java
// Use ofShared() so callbacks can be invoked from any thread
Arena catalogArena = Arena.ofShared();
// Store to prevent GC while Rust holds pointers
catalogArenas.put(name, catalogArena);
```

## Java Interface to Rust Trait Mapping

Java interfaces that correspond directly to DataFusion Rust traits MUST mirror the trait's method names, converted to Java naming conventions (e.g., `create_file_opener` becomes `createFileOpener`). The interfaces may differ in the number of methods (some Rust trait methods may be omitted or combined where Java's type system requires it), but every method that is included must preserve the semantics of its Rust counterpart as closely as possible. Deviations are only acceptable where fundamental differences between Java and Rust make an exact match impossible (e.g., ownership, lifetime parameters, async vs sync).

### Rules

1. **Method names must match** -- Apply camelCase conversion to the Rust snake_case name and nothing else. Do not rename, abbreviate, or "improve" the name.
2. **Semantics must match** -- A Java method should do the same thing as the Rust trait method it maps to. Return types and parameter intent should correspond directly.
3. **Subset is OK, invention is not** -- An interface may omit Rust trait methods that are not yet needed, but it must not add methods that have no counterpart in the Rust trait.
4. **Differences only where languages require it** -- For example, Rust's `async fn scan(...)` may become a synchronous `scan(...)` in Java because the FFI boundary is synchronous. These are acceptable because they are forced by the language boundary, not design choices.

### Enum FFI Encoding

Public Java enums that mirror Rust enums (e.g., `TableType`, `EmissionType`, `Boundedness`) must **not** carry FFI integer values. The enum variants are plain (`BASE`, `VIEW`, `TEMPORARY` — no constructor argument). All integer encoding/decoding lives in the `*Handle` callback that crosses the FFI boundary, using a switch expression:

```java
// In TableProviderHandle (FFI layer)
int getTableType(MemorySegment javaObject) {
    return switch (provider.tableType()) {
      case BASE -> 0;
      case VIEW -> 1;
      case TEMPORARY -> 2;
    };
}
```

On the Rust side, the reverse mapping uses `match` with a `_ =>` default for safety:

```rust
let emission_type = match emission_type_value {
    1 => EmissionType::Final,
    2 => EmissionType::Both,
    _ => EmissionType::Incremental,
};
```

This keeps FFI encoding details out of the public API. Library users see clean enums; Handle classes own the serialization.

### Handle Class Pattern

Every Java interface that maps to a DataFusion Rust trait MUST have a corresponding package-private `*Handle` class (e.g., `TableProvider` -> `TableProviderHandle`, `FileSource` -> `FileSourceHandle`). The Handle is the internal FFI bridge that creates upcall stubs so Rust can call back into the Java interface implementation. Users never see Handle classes; they only implement the public interface.

#### Handle class rules

1. **Implements `TraitHandle`** -- Declared as `final class XxxHandle implements TraitHandle`. The `TraitHandle` interface extends `AutoCloseable` and provides `getTraitStruct()` plus a default `setToPointer(MemorySegment)` method. Never public.

2. **Wraps the interface instance** -- The constructor takes the corresponding interface as its first parameter and stores it in a field. Callback methods delegate to this interface instance.

3. **Struct layout documented in comments** -- The struct layout is documented as a comment block near the top, with matching constants for each field. **Callback pattern**: `OFFSET_*` constants for the Rust `Java*Callbacks` struct. **Direct struct pattern**: `StructLayout` with `VarHandle` accessors for the upstream `FFI_*` struct, plus separate size constants for nested and returned struct types.

4. **Static FunctionDescriptors and MethodHandles** -- Each callback has a static `*_DESC` (FunctionDescriptor) and a static `*_MH` (MethodHandle looked up via `init*MethodHandle()`). These are class-level constants, never per-instance, to enable JIT optimization. Direct-struct-pattern Handles additionally use `StructLayout`-based descriptors for callbacks that return `abi_stable`/`async-ffi` structs.

5. **Constructor signature** -- Always takes the interface instance, `BufferAllocator`, `Arena`, and `boolean fullStackTrace`. Some Handles take additional context (e.g., `Schema`). **Callback pattern**: the constructor allocates the Rust callback struct via a local `ALLOC_*_CALLBACKS` MethodHandle, creates `UpcallStub` instances, and populates the struct fields. **Direct struct pattern**: the constructor allocates the `FFI_*` struct in the Java arena, validates sizes against Rust helpers, pre-allocates return buffers, and populates the struct with upcall stub function pointers.

6. **UpcallStub fields stored to prevent GC** -- Each upcall stub is stored in an instance field (named `*Stub`) with a comment noting this prevents garbage collection.

7. **Struct delivery** -- **Callback pattern**: `getTraitStruct()` returns the `MemorySegment` pointing to the Rust-allocated callback struct; `setToPointer()` writes it to a Rust output buffer. **Direct struct pattern**: `copyStructTo(MemorySegment out)` copies the Java-arena struct bytes into a Rust-side output buffer.

8. **Callback methods** -- Each callback method:
   - Is annotated `@SuppressWarnings("unused")` with a comment that it is called via upcall stub
   - Takes `MemorySegment javaObject` as its first parameter (unused on the Java side but required by the Rust struct calling convention)
   - Delegates to the wrapped interface instance for business logic
   - **Callback pattern**: Returns `int` using `Errors.SUCCESS` on success and `Errors.fromException(errorOut, e, arena, fullStackTrace)` on failure
   - **Direct struct pattern**: Callbacks returning `abi_stable` types return `MemorySegment` pointing to a pre-allocated buffer populated with discriminants and payload

9. **`release` callback** -- Every Handle has a `release(MemorySegment javaObject)` callback. This is usually a no-op (cleanup happens when the arena closes), except for `RecordBatchReaderHandle` which must close the reader to prevent resource leaks.

10. **`close()` is a no-op** -- The `close()` method from `AutoCloseable` is typically empty because the callback struct is freed by Rust when it drops the wrapper.

11. **Child Handle creation** -- When a callback returns a sub-object, the callback method creates the next Handle in the chain and writes its struct to the output. **Callback pattern**: uses `setToPointer` (e.g., `SchemaProviderHandle.getTable()` creates a `TableProviderHandle` and calls `tableHandle.setToPointer(tableOut)`). **Direct struct pattern**: uses `copyStructTo` (e.g., `TableProviderHandle.scan()` creates an `ExecutionPlanHandle` and calls `planHandle.copyStructTo(planOut)`).

#### Existing violations (callback pattern)

The following Handle classes still use the callback pattern with intermediate `Java*Callbacks` structs and `JavaBacked*` Rust wrappers. New Handle classes should use direct struct construction. These should be migrated when feasible.

| Handle | Rust Callback Struct | Rust Wrapper | Rust Files |
|--------|---------------------|--------------|------------|
| `CatalogProviderHandle` | `JavaCatalogProviderCallbacks` | `JavaBackedCatalogProvider` | `java_provider.rs`, `catalog_provider.rs` |
| `SchemaProviderHandle` | `JavaSchemaProviderCallbacks` | `JavaBackedSchemaProvider` | `java_provider.rs`, `schema_provider.rs` |
| `TableProviderHandle` | `JavaTableProviderCallbacks` | `JavaBackedTableProvider` | `java_provider.rs`, `table_provider.rs` |
| `FileFormatHandle` | `JavaFileFormatCallbacks` | `JavaBackedFileFormat` | `file_format.rs` |
| `FileSourceHandle` | `JavaFileSourceCallbacks` | `JavaBackedFileSource` | `file_source.rs` |
| `FileOpenerHandle` | `JavaFileOpenerCallbacks` | `JavaBackedFileOpener` | `file_opener.rs` |

Migration involves:
1. Defining the struct layout in Java (field offsets, `StructLayout` for complex returns)
2. Constructing the `FFI_*` struct in Java arena memory with upcall stub function pointers
3. Adding Rust-side size helpers for runtime validation
4. Replacing `setToPointer`/`getTraitStruct` with `copyStructTo`
5. Removing the `Java*Callbacks` struct, `JavaBacked*` wrapper, and `ALLOC_*_CALLBACKS` downcall from Rust

## Naming Conventions

| Pattern | Naming |
|---------|--------|
| Rust FFI function | `datafusion_<module>_<action>` |
| Java MethodHandle constant | `SCREAMING_SNAKE_CASE` |
| Java FunctionDescriptor constant | `*_DESC` suffix |
| Java callback method handle | `*_MH` suffix |
| Java upcall stub field | `*Stub` suffix |
| Java Handle class | `*Handle` suffix |

## Cross-Language File Naming

Every Java `*Ffi` or `*Handle` class that makes downcalls into Rust must have a matching Rust source file. The Rust file name is derived from the Java class name:

1. Strip the suffix (`Ffi` or `Handle`) from the Java class name
2. Convert CamelCase to snake_case
3. Append `.rs`

For example: `SessionContextFfi` → `session_context.rs`, `TableProviderHandle` → `table_provider.rs`.

### Rule

All `#[no_mangle] pub extern "C" fn` entry points called by a given Java class must live in the matching Rust file. Internal Rust helpers (non-FFI functions, trait impls, wrapper structs) may live in other modules; only the `#[no_mangle]` entry points are constrained.

Classes with no downcalls (e.g., `ExprFfi`, `TableReferenceFfi`) do not need a corresponding Rust file.

### Enforcement

`CrossLanguageNamingTest.java` validates this rule at build time by scanning Java source files for downcall function names and verifying they appear in the expected Rust file.

## FFI Encapsulation Rules

All FFI implementation classes live in `org.apache.arrow.datafusion` but are **package-private** (no `public` modifier), making them invisible to library consumers.

Rules 1--4, 6, and the Handle class structural rules (implements `TraitHandle`, `final`) are enforced at build time by `FfiEncapsulationTest` (ArchUnit). When adding or changing a rule here, update the corresponding ArchUnit test, and vice versa.

### Rules

1. **FFI classes are package-private** -- `NativeUtil`, `NativeLoader`, `Errors`,
   `UpcallStub`, all `*Ffi` classes, all `*Handle` classes, `TraitHandle`, and utility
   records (`NativeString`, `PointerOut`, `LongOut`) must have no `public` modifier on their class
   declaration.

2. **No `java.lang.foreign` in public classes** -- Public API classes (e.g., `SessionContext`, `DataFrame`)
   must NOT import `java.lang.foreign.MemorySegment`, `Arena`, `ValueLayout`, or any other FFM type.
   These may only appear in package-private FFI classes.

3. **No `MemorySegment` in public API signatures** -- No public constructor, method, or field may use
   `MemorySegment` as a parameter type, return type, or field type.

4. **`NativeUtil` confined to FFI classes** -- Only `*Ffi` and `*Handle` classes
   may reference `NativeUtil`.

5. **Delegation pattern** -- Public API classes that wrap native pointers delegate to a package-private
   `*Ffi` class. The public class holds a `*Ffi` instance field and forwards method calls to it.
   Example: `SessionContext` delegates to `SessionContextFfi`.

6. **Constructors taking FFI types are package-private** -- If a public class has a constructor that
   accepts a `*Ffi` parameter, that constructor must be package-private. Example: `Expr(ExprFfi ffi)`
   is package-private.


## Common Gotchas

1. **Debug implementations**: Rust wrapper structs implementing DataFusion traits need manual `Debug` impl since they contain raw pointers

2. **Handle references prevent GC**: Store `*Handle` objects in the parent to prevent garbage collection while Rust holds pointers

3. **RecordBatchReader must close**: The release callback must close the reader to prevent memory leaks

4. **String length before read**: When reading error strings from Rust, query the length first with `datafusion_string_len` rather than assuming a max length
