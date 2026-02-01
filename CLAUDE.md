# CLAUDE.md - Project Guide for AI Assistants

## Project Overview

This repository contains Java bindings for Apache DataFusion. There are two separate binding implementations:

1. **`datafusion-java` / `datafusion-jni`** (LEGACY) - Original JNI-based bindings
2. **`datafusion-ffi-java` / `datafusion-ffi-native`** (NEW) - FFM-based bindings using Java's Foreign Function & Memory API

## Goals

The new FFM-based bindings (`datafusion-ffi-java`) were created to:

1. **Enable zero-copy data transfer** via the Arrow C Data Interface between Java and Rust
2. **Use modern Java FFM API** (Project Panama) instead of JNI for better performance and safety
3. **Create C-compatible Rust wrapper** since DataFusion's existing `datafusion-ffi` module uses `abi_stable` (Rust-to-Rust only)
4. **Simplify the architecture** with synchronous FFI calls bridged by a Rust-side Tokio runtime

## Architecture

```
┌─────────────────────┐     ┌───────────────────────┐     ┌─────────────┐
│  Java (FFM API)     │ ──► │  Rust C-FFI Library   │ ──► │  DataFusion │
│  datafusion-ffi-java│     │  datafusion-ffi-native│     │             │
└─────────────────────┘     └───────────────────────┘     └─────────────┘
         │                            │
         └────── Arrow C Data Interface ──────┘
                   (zero-copy)
```

### Key Components

| Component | Purpose |
|-----------|---------|
| `datafusion-ffi-native/` | Rust crate exposing C-compatible FFI functions |
| `datafusion-ffi-java/` | Java module using FFM API to call native functions |
| `NativeLoader.java` | Loads native library via `SymbolLookup` |
| `DataFusionBindings.java` | Method handles for all native functions |
| `SessionContext.java` | Main entry point - create sessions, register tables, execute SQL |
| `DataFrame.java` | Query result wrapper |
| `RecordBatchStream.java` | Zero-copy Arrow data streaming |

## Important: Legacy Code Policy

### DO NOT modify or depend on:
- `datafusion-java/` - Legacy Java JNI bindings
- `datafusion-jni/` - Legacy Rust JNI library

### Reasons:
1. The legacy code uses outdated dependencies (Arrow 39, DataFusion 25) with known build issues
2. The JNI approach is being replaced by the FFM approach
3. Changes to legacy code may break existing users

### When working on this project:
- All new features should go in `datafusion-ffi-java` and `datafusion-ffi-native`
- Do not add dependencies between the new and legacy modules
- The legacy modules may be removed in a future release

## Build Instructions

### Prerequisites
- Java 21+ (for FFM preview features)
- Rust toolchain (cargo)

### Build and Test the FFM Module

```bash
# Build and test (recommended)
./gradlew :datafusion-ffi-java:test

# Just build the Rust library
./gradlew :datafusion-ffi-java:cargoDevBuild

# Just compile Java
./gradlew :datafusion-ffi-java:compileJava

# Format code
./gradlew :datafusion-ffi-java:spotlessApply
```

### Running Tests

Tests require the native library to be built first. The Gradle build handles this automatically via task dependencies.

```bash
# Run all FFI tests
./gradlew :datafusion-ffi-java:test

# Run with verbose output
./gradlew :datafusion-ffi-java:test --info
```

### DO NOT run `./gradlew test` (without module qualifier)
This will try to build the legacy `datafusion-jni` module which has dependency issues with newer Rust/Arrow versions.

## Adding New Features

### Adding a new native function:

1. **Rust side** (`datafusion-ffi-native/src/`):
   - Add the function with `#[no_mangle]` and `extern "C"`
   - Use `*mut c_void` for opaque pointers
   - Use `*mut *mut c_char` for error output
   - Return `i32` for status codes (0 = success, -1 = error)

2. **Java side** (`datafusion-ffi-java/`):
   - Add method handle in `DataFusionBindings.java`
   - Create or update wrapper class to call the method handle
   - Use `Arena.ofConfined()` for temporary allocations
   - Always check for null pointers and handle errors

### Example pattern for new FFI function:

**Rust:**
```rust
#[no_mangle]
pub unsafe extern "C" fn datafusion_new_function(
    ctx: *mut c_void,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);
    if ctx.is_null() {
        set_error(error_out, "Null pointer");
        return -1;
    }
    // ... implementation
    0
}
```

**Java:**
```java
// In DataFusionBindings.java
public static final MethodHandle NEW_FUNCTION =
    downcall("datafusion_new_function",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS,  // ctx
            ValueLayout.ADDRESS   // error_out
        ));

// In wrapper class
public void newFunction() {
    try (Arena arena = Arena.ofConfined()) {
        MemorySegment errorOut = arena.allocate(ValueLayout.ADDRESS);
        errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

        int result = (int) DataFusionBindings.NEW_FUNCTION.invokeExact(context, errorOut);

        if (result != 0) {
            MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
            if (!errorPtr.equals(MemorySegment.NULL)) {
                String msg = errorPtr.reinterpret(1024).getUtf8String(0);
                DataFusionBindings.FREE_STRING.invokeExact(errorPtr);
                throw new RuntimeException(msg);
            }
        }
    } catch (Throwable e) {
        throw new RuntimeException("Failed", e);
    }
}
```

## Dependencies

### Java (datafusion-ffi-java)
- Java 21+ with `--enable-preview`
- arrow-vector 18.1.0
- arrow-c-data 18.1.0
- JUnit 5 for testing

### Rust (datafusion-ffi-native)
- datafusion 45
- arrow 54 (with ffi feature)
- tokio 1.x (for async runtime)
- futures 0.3

## Known Issues

1. **Java 21 preview features**: The FFM API is preview in Java 21, stable in Java 22+. Code uses `allocateUtf8String` and `getUtf8String` methods.

2. **Legacy module build failures**: The `datafusion-jni` crate has dependency conflicts with newer chrono versions. This is a known issue - use module-specific Gradle commands.

## Testing Checklist

When making changes, ensure:

- [ ] `./gradlew :datafusion-ffi-java:compileJava` succeeds
- [ ] `./gradlew :datafusion-ffi-java:test` passes all tests
- [ ] `./gradlew :datafusion-ffi-java:spotlessCheck` passes (or run `spotlessApply`)
- [ ] New functionality has corresponding tests in `IntegrationTest.java`
