---
paths:
  - "datafusion-ffi-java/build.gradle"
  - "build.gradle"
  - "settings.gradle"
  - ".github/workflows/*.yml"
  - "scripts/*"
---

# Build and Release

## Versioning

Version is in `datafusion-ffi-java/build.gradle`. Local dev uses a `-SNAPSHOT` suffix; CI strips it for releases.

**Lifecycle:** `0.17.1-SNAPSHOT` → workflow_dispatch → publish `0.17.1` → tag `v0.17.1` → auto-bump to `0.17.2-SNAPSHOT`.

To bump the major or minor version, manually update `build.gradle` (e.g., `0.18.0-SNAPSHOT`).

## Key Commands

```bash
# Build and test
./gradlew :datafusion-ffi-java:test

# Build Rust library only
./gradlew :datafusion-ffi-java:cargoDevBuild

# Format code
./gradlew :datafusion-ffi-java:spotlessApply

# Build JAR with local platform's native library
./gradlew :datafusion-ffi-java:jarWithLocalLib

# Validate a built JAR end-to-end
./scripts/validate-jar.sh
```

**DO NOT** run `./gradlew test` without the module qualifier — it will try to build the legacy `datafusion-jni` module which has dependency issues.

## Gradle Properties

| Property | Purpose |
|----------|---------|
| `-PreleaseVersion=X.Y.Z` | Override the SNAPSHOT version with a release version |
| `-PpublishPrebuiltJars` | Publish exact pre-built JARs instead of rebuilding (CI mode) |
| `-PskipDiplomatGeneration` | Skip Diplomat code generation (use pre-generated sources) |

## Native Library Loading

`NativeLoader.java` loads the native library via a three-tier strategy: (1) `java.library.path` system property, (2) JAR extraction via native-lib-loader, (3) `System.loadLibrary()` fallback.

## Platform Configuration

Supported platforms are defined in `scripts/platforms.conf` (single source of truth). When adding a new platform, also update:
- `.github/workflows/publish-ffi.yml` (build-native matrix)
- `NativeLoader.java` (`getPlatformDirectory` switch)

## Releasing
The CI workflow is `.github/workflows/publish-ffi.yml`. It builds native libraries on 5 platforms, packages a fat JAR, tests it on 3 platforms, publishes to GitHub Packages, tags the release, and bumps the version.

**To release**, trigger `workflow_dispatch` on the current development branch. **Do NOT merge to `main`.**

```bash
gh workflow run publish-ffi.yml --ref datafusion-ffi-java -R pgwhalen/datafusion-java
```

The `-R pgwhalen/datafusion-java` flag is required because the workflow exists on the fork, not the upstream `datafusion-contrib` repo.

When asked to release, trigger the workflow directly without asking for confirmation.
