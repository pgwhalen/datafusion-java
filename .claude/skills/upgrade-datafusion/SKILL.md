---
name: upgrade-datafusion
description: Upgrade DataFusion and related Rust/Java dependencies to a new version, fixing breaking changes and updating config records.
argument-hint: "[target-version]"
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Edit, Write, Bash, WebFetch, WebSearch, Task
---

# Upgrade DataFusion

Upgrade the DataFusion dependency in `datafusion-ffi-native` and propagate any breaking changes to the Java side.

## Step 1: Determine Current and Target Versions

Read `datafusion-ffi-native/Cargo.toml` to find the current DataFusion, Arrow, and object_store versions.

If a target version was provided (`$ARGUMENTS`), use that. Otherwise, search crates.io for the latest DataFusion release version.

## Step 2: Research Breaking Changes

Fetch and review these sources for changes between the current and target versions:

1. **DataFusion Upgrade Guide**: https://datafusion.apache.org/library-user-guide/upgrading.html
2. **DataFusion Blog** (look for release posts): https://datafusion.apache.org/blog/
3. **DataFusion CHANGELOG**: https://github.com/apache/datafusion/blob/main/CHANGELOG.md

Also check DataFusion's `Cargo.toml` on the target version's tag to find the exact Arrow and object_store versions required:
- https://github.com/apache/datafusion/blob/main/datafusion/Cargo.toml

Summarize the breaking changes relevant to this project before proceeding.

## Step 3: Update Cargo.toml

Update `datafusion-ffi-native/Cargo.toml`. All DataFusion crates must use the same version. Arrow and object_store must match what DataFusion requires:

```toml
datafusion = "NEW_VERSION"
datafusion-ffi = "NEW_VERSION"
datafusion-datasource = "NEW_VERSION"
arrow = { version = "MATCHING_ARROW", features = ["ffi"] }
object_store = "MATCHING_VERSION"
```

## Step 4: Build and Fix Compilation Errors

Run `cargo build` in `datafusion-ffi-native/`. Fix errors iteratively. Common breaking changes:

- **Renamed/moved types**: Search DataFusion source for the type name to find its new location
- **Changed trait methods**: Compare trait signatures with the new version
- **New required trait methods**: Implement any new required methods
- **Removed types/enums**: Check the upgrade guide for replacements
- **Changed function signatures**: Update call sites

When stuck, search the DataFusion source in `~/.cargo/registry/src/`:
```bash
grep -rn "error_text" ~/.cargo/registry/src/*/datafusion-*NEW_VERSION/
```

Repeat until it compiles cleanly.

## Step 5: Run Tests

```bash
./gradlew :datafusion-ffi-java:test
```

If tests fail:
1. Read the error message — it often indicates which trait method or callback failed
2. Search DataFusion source for the error message to find context
3. Compare with built-in implementations (e.g., `CsvSource`, `MemTable`) for correct patterns
4. Fix and re-run until all tests pass

## Step 6: Update Java SessionConfig Option Records

The Java option records mirror DataFusion's `ConfigOptions` from `datafusion-common/src/config.rs`. Fetch that file from the new version's tag and diff against the current Java records.

For detailed instructions on which files to update and how, see [config-options.md](config-options.md).

## Step 7: Format and Final Validation

```bash
./gradlew :datafusion-ffi-java:spotlessApply
./gradlew :datafusion-ffi-java:test
```

## Step 8: Summary

Provide a summary including:
- Previous version -> new version for each dependency
- Breaking changes encountered and how they were resolved
- New Java config options added/removed
- Any changes to the FFI interface
- Whether all tests pass
