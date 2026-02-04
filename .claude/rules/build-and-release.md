# Build and Release

This document describes the build system, JAR packaging, and release process for `datafusion-ffi-java`.

## Native Library Loading Strategy

`NativeLoader.java` implements a three-tier loading strategy:

1. **java.library.path** - Check system property first (development/test mode)
2. **JAR extraction** - Extract library from JAR resources using native-lib-loader
3. **System path fallback** - Try `System.loadLibrary()` + `SymbolLookup.loaderLookup()`

This preserves backward compatibility with existing tests while enabling JAR-embedded distribution.

## Platform Configuration

Supported platforms are defined in `scripts/platforms.conf`, which is the single source of truth. This file is a Java properties file mapping osdetector keys to native-lib-loader directory names:

```properties
osx-x86_64=osx_64
osx-aarch_64=osx_arm64
linux-x86_64=linux_64
linux-aarch_64=linux_arm64
windows-x86_64=windows_64
```

This file is read by:
- **`build.gradle`** — loaded as a `Properties` object for `platformMapping`
- **Shell scripts** — loaded via `scripts/platforms.sh` into the `PLATFORMS` bash array

When adding a new platform, update `scripts/platforms.conf` and also:
- `.github/workflows/publish-ffi.yml` (build-native matrix — ties platform to runner OS and lib filename)
- `NativeLoader.java` (`getPlatformDirectory` switch — maps from Java `Architecture` enum)

## Native Library Directory Structure

Native libraries are packaged in the JAR under `natives/{platform}/`:

```
natives/
  osx_64/libdatafusion_ffi_native.dylib
  osx_arm64/libdatafusion_ffi_native.dylib
  linux_64/libdatafusion_ffi_native.so
  linux_arm64/libdatafusion_ffi_native.so
  windows_64/datafusion_ffi_native.dll
```

Platform directory names follow native-lib-loader convention: `{os}_{arch}` where:
- OS: `osx`, `linux`, `windows`
- Arch: `64` (x86_64), `arm64` (aarch64)

## Gradle Tasks

### Development Tasks

| Task | Description |
|------|-------------|
| `cargoDevBuild` | Build Rust library in debug mode |
| `cargoReleaseBuild` | Build Rust library in release mode |
| `copyDevLibrary` | Copy debug library to build dir for testing |
| `test` | Run tests (depends on `copyDevLibrary`) |

### JAR Packaging Tasks

| Task | Description |
|------|-------------|
| `copyReleaseLibraryForJar` | Copy release library to JAR resources for current platform |
| `copyAllNativeLibraries` | Copy all platform artifacts from CI build directory |
| `jarWithLocalLib` | Create JAR with only local platform's native library |
| `jarWithAllLibs` | Create JAR with all platform native libraries |

### Publishing Tasks

| Task | Description |
|------|-------------|
| `publish` | Publish to GitHub Packages |
| `publishToMavenLocal` | Publish to local Maven repository |

## Development Workflow

### Running Tests

```bash
./gradlew :datafusion-ffi-java:test
```

This automatically:
1. Builds the Rust library in debug mode
2. Copies it to `build/ffi_libs/dev/`
3. Sets `java.library.path` for the test JVM

### Building JAR with Local Library

```bash
./gradlew :datafusion-ffi-java:jarWithLocalLib
```

Creates `build/libs/datafusion-ffi-java-{version}-local.jar` with only the current platform's native library. Useful for local development and testing.

### Validating JAR

```bash
./scripts/validate-jar.sh
```

This script:
1. Verifies JAR exists and lists native libraries
2. Checks for expected platform directories
3. Downloads Arrow/SLF4J dependencies
4. Compiles and runs a test that creates SessionContext and executes SQL

Requires Java 22+ (searches `JAVA_HOME`, common JDK locations, or falls back to system Java).

## CI/CD Workflow

The `.github/workflows/publish-ffi.yml` workflow handles cross-platform builds and publishing:

### Job 1: build-native (matrix)

Builds native libraries on 5 platforms:
- `ubuntu-latest` (linux_64)
- `ubuntu-24.04-arm` (linux_arm64)
- `macos-13` (osx_64 - Intel)
- `macos-14` (osx_arm64 - Apple Silicon)
- `windows-latest` (windows_64)

Each platform uploads its native library as an artifact.

### Job 2: build-jar

1. Downloads all 5 native artifacts
2. Organizes them into `build/native-artifacts/{platform}/`
3. Runs `copyAllNativeLibraries` and `jarWithAllLibs`
4. Uploads the combined JAR

### Job 3: test-jar

Tests the packaged JAR on 3 platforms:
- `ubuntu-latest`
- `macos-14`
- `windows-latest`

Runs without `java.library.path` to validate JAR extraction works.

### Job 4: publish

Publishes to GitHub Packages when:
- Pushing to `main` branch
- Creating a tag starting with `v`

## Publishing Configuration

### GitHub Packages

The `publishing` block in `build.gradle` configures GitHub Packages:

```groovy
repositories {
    maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/pgwhalen/datafusion-java")
        credentials {
            username = System.getenv("GITHUB_ACTOR")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
}
```

GitHub Actions automatically provides `GITHUB_ACTOR` and `GITHUB_TOKEN`.

### POM Metadata

The published artifact includes:
- Name: DataFusion FFI Java
- License: Apache 2.0
- SCM: GitHub repository URLs
- Javadoc and sources JARs

## Consumer Usage

After publishing, users can add the dependency:

```gradle
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/pgwhalen/datafusion-java")
        credentials {
            username = project.findProperty("gpr.user") ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.key") ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation 'io.github.datafusion-contrib:datafusion-ffi-java:0.17.0'
}
```

Then use directly without native library installation:

```java
try (SessionContext ctx = new SessionContext()) {
    DataFrame df = ctx.sql("SELECT 1 + 1");
    // ...
}
```

## Dependencies

### Runtime Dependencies

| Dependency | Purpose |
|------------|---------|
| `org.scijava:native-lib-loader:2.5.0` | Platform detection and library extraction |
| `org.apache.arrow:arrow-vector:18.1.0` | Arrow Java vectors |
| `org.apache.arrow:arrow-c-data:18.1.0` | Arrow C Data Interface |
| `org.slf4j:slf4j-api:2.0.16` | Logging API |

### Build Requirements

- Java 22+ (for FFM API)
- Rust toolchain (for native library)
- Gradle 8.7+
