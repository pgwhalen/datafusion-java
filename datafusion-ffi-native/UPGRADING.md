# Upgrading DataFusion in datafusion-ffi-native

This guide documents how to upgrade the DataFusion dependency in this crate.

## Overview

DataFusion releases frequently (roughly monthly) and often includes breaking changes. This crate wraps DataFusion's Rust API via C-FFI for Java consumption, so upgrades require updating both the Rust bindings and potentially the Java side.

## Step 1: Research Breaking Changes

Before upgrading, gather information about what changed:

### Primary Sources

1. **DataFusion Upgrade Guide** (most important):
   ```
   https://datafusion.apache.org/library-user-guide/upgrading.html
   ```

2. **DataFusion Release Blog Posts**:
   ```
   https://datafusion.apache.org/blog/
   ```
   Look for posts titled "Apache DataFusion X.0.0 Released"

3. **DataFusion CHANGELOG**:
   ```
   https://github.com/apache/datafusion/blob/main/CHANGELOG.md
   ```

4. **crates.io** for compatible versions:
   - Check `datafusion` crate page for its Arrow version requirement
   - DataFusion pins specific Arrow versions

### Version Compatibility Matrix

DataFusion, Arrow, and object_store versions must be compatible.

Always check the actual `Cargo.toml` in the DataFusion repo for exact versions.

## Step 2: Update Cargo.toml

Update all DataFusion-related dependencies together:

```toml
[dependencies]
datafusion = "NEW_VERSION"
datafusion-ffi = "NEW_VERSION"
datafusion-datasource = "NEW_VERSION"  # if needed
arrow = { version = "MATCHING_ARROW", features = ["ffi"] }
object_store = "MATCHING_VERSION"
```

## Step 3: Build and Fix Compilation Errors

Run `cargo build` and fix errors iteratively. Common breaking changes:

## Step 4: Run Tests

```bash
# Build Rust
cargo build

# Run Java tests (requires Java 21+)
cd .. && ./gradlew :datafusion-ffi-java:test
```

If tests fail with errors from the native library, check:
1. Error messages often indicate which trait method failed
2. Search DataFusion source for the error message to find context
3. Compare with built-in implementations (e.g., `CsvSource`) for patterns

## Step 5: Update Java SessionConfig Option Records

The Java option records (`CatalogOptions`, `ExecutionOptions`, `ParquetOptions`, `OptimizerOptions`,
`SqlParserOptions`, `ExplainOptions`, `FormatOptions`) mirror DataFusion's `ConfigOptions`.
Their Javadoc is copied from Rust doc comments.

When upgrading DataFusion, review `datafusion-common/src/config.rs` for:
- **Added options**: Add new fields to the corresponding Java record and its Builder
- **Removed options**: Remove the Java field (or deprecate if backward compatibility is needed)
- **Renamed options**: Update the config key in `writeTo()` and rename the Java field
- **New option groups**: Create a new `*Options.java` record and add it to `SessionConfig.Builder`

After updating the records, update `SessionConfigTest.java`:
- Each `testAll*OptionsViaShow()` test sets every field via the builder and asserts the value via `SHOW`
- Add new fields to the appropriate test method's builder and add a corresponding `assertShow` call
- Remove deleted fields from both the builder and assertions
- If a new option group is added, create a new `testAll*OptionsViaShow()` test method following the same pattern

## Step 6: Update Documentation

After a successful upgrade:
1. Update CLAUDE.md if patterns changed significantly
2. Note any Java-side changes needed in the commit message

## Common Patterns

### Searching DataFusion Source

When you need to understand a trait or error:

```bash
# Find where an error message comes from
grep -rn "error message" ~/.cargo/registry/src/*/datafusion-*VERSION/

# See how a trait method is implemented
grep -A 30 "fn method_name" ~/.cargo/registry/src/*/datafusion-*VERSION/src/*.rs
```
