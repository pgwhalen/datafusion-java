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
    (int) DataFusionBindings.SOME_FUNCTION.invokeExact(arg1, arg2, errorOut));

// For pointer-returning operations:
MemorySegment ptr = NativeUtil.callForPointer(arena, "Operation name", errorOut ->
    (MemorySegment) DataFusionBindings.CREATE_THING.invokeExact(arg, errorOut));

// For stream operations (positive=data, 0=end, negative=error):
int result = NativeUtil.callForStreamResult(arena, "Stream next", errorOut ->
    (int) DataFusionBindings.STREAM_NEXT.invokeExact(stream, out, errorOut));
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
        Linker linker = DataFusionBindings.getLinker();
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

### Rust Callback Structs

The Rust side defines callback structs that Java populates:

```rust
#[repr(C)]
pub struct JavaCallbacks {
    pub java_object: *mut c_void,
    pub callback_fn: Option<unsafe extern "C" fn(*mut c_void, *mut *mut c_char) -> i32>,
    pub release_fn: Option<unsafe extern "C" fn(*mut c_void)>,
}
```

### 1:1 Callback-to-Trait-Method Rule

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

Current pairs:

| Interface | Handle |
|-----------|--------|
| `CatalogProvider` | `CatalogProviderHandle` |
| `SchemaProvider` | `SchemaProviderHandle` |
| `TableProvider` | `TableProviderHandle` |
| `ExecutionPlan` | `ExecutionPlanHandle` |
| `RecordBatchReader` | `RecordBatchReaderHandle` |
| `FileFormat` | `FileFormatHandle` |
| `FileSource` | `FileSourceHandle` |
| `FileOpener` | `FileOpenerHandle` |

#### Handle class rules

1. **Implements `TraitHandle`** -- Declared as `final class XxxHandle implements TraitHandle`. The `TraitHandle` interface extends `AutoCloseable` and provides `getTraitStruct()` plus a default `setToPointer(MemorySegment)` method. Never public.

2. **Wraps the interface instance** -- The constructor takes the corresponding interface as its first parameter and stores it in a field. Callback methods delegate to this interface instance.

3. **Struct layout documented in comments** -- The Rust `#[repr(C)]` callback struct layout is documented as a comment block near the top, with matching `OFFSET_*` constants for each field.

4. **Static FunctionDescriptors and MethodHandles** -- Each callback has a static `*_DESC` (FunctionDescriptor) and a static `*_MH` (MethodHandle looked up via `init*MethodHandle()`). These are class-level constants, never per-instance, to enable JIT optimization.

5. **Constructor signature** -- Always takes the interface instance, `BufferAllocator`, `Arena`, and `boolean fullStackTrace`. Some Handles take additional context (e.g., `Schema`). The constructor:
   - Allocates the Rust callback struct via `DataFusionBindings.ALLOC_*_CALLBACKS`
   - Creates `UpcallStub` instances bound to `this` for each callback
   - Populates the struct fields with the upcall stub segments

6. **UpcallStub fields stored to prevent GC** -- Each upcall stub is stored in an instance field (named `*Stub`) with a comment noting this prevents garbage collection.

7. **`getTraitStruct()` method** -- Returns the `MemorySegment` pointing to the populated Rust callback struct. This is what gets passed across the FFI boundary.

8. **Callback methods** -- Each callback method:
   - Is annotated `@SuppressWarnings("unused")` with a comment that it is called via upcall stub
   - Takes `MemorySegment javaObject` as its first parameter (unused on the Java side but required by the Rust struct calling convention)
   - Returns `int` using `Errors.SUCCESS` on success and `Errors.fromException(errorOut, e, arena, fullStackTrace)` on failure
   - Delegates to the wrapped interface instance for business logic

9. **`release` callback** -- Every Handle has a `release(MemorySegment javaObject)` callback. This is usually a no-op (cleanup happens when the arena closes), except for `RecordBatchReaderHandle` which must close the reader to prevent resource leaks.

10. **`close()` is a no-op** -- The `close()` method from `AutoCloseable` is typically empty because the callback struct is freed by Rust when it drops the wrapper.

11. **Child Handle creation** -- When a callback returns a sub-object, the callback method creates the next Handle in the chain and writes its callback struct pointer to the output using `setToPointer`. For example, `SchemaProviderHandle.getTable()` creates a `TableProviderHandle` and calls `tableHandle.setToPointer(tableOut)`.

## Naming Conventions

| Pattern | Naming |
|---------|--------|
| Rust FFI function | `datafusion_<module>_<action>` |
| Java MethodHandle constant | `SCREAMING_SNAKE_CASE` |
| Java FunctionDescriptor constant | `*_DESC` suffix |
| Java callback method handle | `*_MH` suffix |
| Java upcall stub field | `*Stub` suffix |
| Java Handle class | `*Handle` suffix |

## FFI Encapsulation Rules

All FFI implementation classes live in `org.apache.arrow.datafusion` but are **package-private** (no `public` modifier), making them invisible to library consumers.

Rules 1--4 are enforced at build time by `FfiEncapsulationTest` (ArchUnit). When adding or changing a rule here, update the corresponding ArchUnit test, and vice versa.

### Rules

1. **FFI classes are package-private** -- `DataFusionBindings`, `NativeUtil`, `NativeLoader`, `Errors`,
   `UpcallStub`, `ArrowExporter`, all `*Ffi` classes, all `*Handle` classes, `TraitHandle`, and utility
   records (`NativeString`, `PointerOut`, `LongOut`) must have no `public` modifier on their class
   declaration.

2. **No `java.lang.foreign` in public classes** -- Public API classes (e.g., `SessionContext`, `DataFrame`)
   must NOT import `java.lang.foreign.MemorySegment`, `Arena`, `ValueLayout`, or any other FFM type.
   These may only appear in package-private FFI classes.

3. **No `MemorySegment` in public API signatures** -- No public constructor, method, or field may use
   `MemorySegment` as a parameter type, return type, or field type.

4. **`DataFusionBindings` and `NativeUtil` confined to FFI classes** -- Only `*Ffi` and `*Handle` classes
   may reference `DataFusionBindings` or `NativeUtil`.

5. **Delegation pattern** -- Public API classes that wrap native pointers delegate to a package-private
   `*Ffi` class. The public class holds a `*Ffi` instance field and forwards method calls to it.
   Example: `SessionContext` delegates to `SessionContextFfi`.

6. **Constructors taking FFI types are package-private** -- If a public class has a constructor that
   accepts a `*Ffi` parameter, that constructor must be package-private. Example: `Expr(ExprFfi ffi)`
   is package-private.

### Existing violations

There are currently no known violations. Do NOT add new violations.

## Common Gotchas

1. **Debug implementations**: Rust wrapper structs implementing DataFusion traits need manual `Debug` impl since they contain raw pointers

2. **Handle references prevent GC**: Store `*Handle` objects in the parent to prevent garbage collection while Rust holds pointers

3. **RecordBatchReader must close**: The release callback must close the reader to prevent memory leaks

4. **String length before read**: When reading error strings from Rust, query the length first with `datafusion_string_len` rather than assuming a max length
