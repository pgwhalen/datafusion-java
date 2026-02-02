# FFI Code Patterns

This document describes the patterns used in `datafusion-ffi-java` and `datafusion-ffi-native` for Java-Rust interop via the Foreign Function & Memory (FFM) API.

## Error Handling

### Rust Side: Error Output Pattern

All Rust FFI functions that can fail should:
1. Accept an `error_out: *mut *mut c_char` parameter as the last argument
2. Return an `i32` status code (0 = success, non-zero = error) OR a pointer (null = error)
3. On error, allocate an error string and write it to `error_out`

```rust
#[no_mangle]
pub unsafe extern "C" fn datafusion_do_something(
    ctx: *mut c_void,
    error_out: *mut *mut c_char,  // Always last parameter
) -> i32 {
    clear_error(error_out);  // Always clear first

    if ctx.is_null() {
        set_error(error_out, "Context is null");
        return -1;
    }

    match do_something_fallible() {
        Ok(_) => 0,
        Err(e) => {
            set_error(error_out, &e.to_string());
            -1
        }
    }
}
```

For pointer-returning functions:
```rust
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_thing(
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    match create_thing() {
        Ok(thing) => Box::into_raw(Box::new(thing)) as *mut c_void,
        Err(e) => {
            set_error(error_out, &e.to_string());
            std::ptr::null_mut()
        }
    }
}
```

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

**ErrorOut** - for returning error messages from callbacks:
```java
// Before: manual null check, allocate string, set pointer
// After:
new ErrorOut(errorOut).set(e.getMessage(), arena);
```

## Upcall Stub Pattern (Java Callbacks)

When creating Java callbacks that Rust can invoke, follow these patterns:

### Static MethodHandles and FunctionDescriptors

Make `MethodHandle` and `FunctionDescriptor` objects static to enable JIT optimization:

```java
final class SomeHandle implements AutoCloseable {
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
            return 0;
        } catch (Exception e) {
            new ErrorOut(errorOut).set(e.getMessage(), arena);
            return -1;
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

## Naming Conventions

| Pattern | Naming |
|---------|--------|
| Rust FFI function | `datafusion_<module>_<action>` |
| Java MethodHandle constant | `SCREAMING_SNAKE_CASE` |
| Java FunctionDescriptor constant | `*_DESC` suffix |
| Java callback method handle | `*_MH` suffix |
| Java upcall stub field | `*Stub` suffix |
| Java Handle class | `*Handle` suffix |

## Common Gotchas

1. **Debug implementations**: Rust wrapper structs implementing DataFusion traits need manual `Debug` impl since they contain raw pointers

2. **Handle references prevent GC**: Store `*Handle` objects in the parent to prevent garbage collection while Rust holds pointers

3. **RecordBatchReader must close**: The release callback must close the reader to prevent memory leaks

4. **String length before read**: When reading error strings from Rust, query the length first with `datafusion_string_len` rather than assuming a max length
