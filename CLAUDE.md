# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is datafusion-java, a Java binding for Apache DataFusion. The project provides Java APIs to execute SQL queries using the Apache DataFusion query engine written in Rust. It consists of two main modules:

- `datafusion-java`: The main Java library containing public APIs and JNI bindings
- `datafusion-jni`: A JNI wrapper library written in Rust that bridges Java to the native DataFusion library
- `datafusion-examples`: Example applications demonstrating usage

## Build System & Commands

This project uses Gradle as the build system with a multi-module setup.

### Essential Commands

```bash
# Build the project (includes compiling Rust JNI library)
./gradlew build

# Run tests (requires Rust toolchain for JNI compilation)
./gradlew test

# Run the example application
./gradlew run

# Run tests for a specific module
./gradlew :datafusion-java:test

# Format Java code (uses Google Java Format via Spotless)
./gradlew spotlessApply

# Check code formatting
./gradlew spotlessCheck

# Build release version with all native libraries
./gradlew cargoReleaseBuild

# Build dev version of native library
./gradlew cargoDevBuild
```

### Dependencies
- Requires Rust toolchain (cargo) to build JNI native library
- Uses Apache Arrow 18.x for vector operations
- Built with Java 21 toolchain but targets Java 8 compatibility
- Uses JUnit 5 for testing

## Architecture

### Core Components

1. **SessionContext** (`SessionContext.java`): The main entry point for creating SQL queries and managing DataFusion sessions. Acts as a factory for DataFrame objects.

2. **DataFrame** (`DataFrame.java`): Represents query results that can be collected into Arrow batches, written to files, or converted to table providers.

3. **JNI Bridge** (`datafusion-jni/`): Rust library that exposes DataFusion functionality to Java through JNI calls. Contains modules for:
   - `context.rs`: Session context operations
   - `dataframe.rs`: DataFrame operations  
   - `runtime.rs`: Tokio runtime management
   - `stream.rs`: Streaming result handling

4. **Native Library Management**: The build system handles cross-platform native library compilation and packaging for Linux, macOS, and Windows.

### Key Patterns

- All operations return `CompletableFuture` for async execution
- Memory management uses Apache Arrow's `BufferAllocator`
- Results are streamed through Arrow's `ArrowReader` interface
- Native resources implement `AutoCloseable` for proper cleanup

### Testing

Tests are located in `datafusion-java/src/test/java/` and use:
- JUnit 5 for test framework
- Requires JNI library path configuration: `JNI_PATH` or defaults to dev build location
- Uses `--add-opens` JVM arguments for Arrow vector access

### File Formats

The library supports:
- CSV files via `registerCsv()`
- Parquet files via `registerParquet()`
- Custom table providers via `TableProvider` interface

## Development Notes

- The project maintains version compatibility with specific DataFusion and Apache Arrow versions (currently DataFusion 25.0, Arrow 18.x)
- Native library compilation happens automatically during build but requires Rust toolchain
- Cross-platform JAR artifacts include native libraries for all supported platforms
- Uses Spotless for code formatting with Google Java Format