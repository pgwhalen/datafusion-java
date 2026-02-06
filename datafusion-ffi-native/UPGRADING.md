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

DataFusion, Arrow, and object_store versions must be compatible:

| DataFusion | Arrow | object_store |
|------------|-------|--------------|
| 45 | 54 | 0.11 |
| 46-48 | 54-55 | 0.11-0.12 |
| 49 | 56 | 0.12 |
| 50-52 | 57 | 0.12 |

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

### Error Type Boxing (v49+)

DataFusion 49+ boxes error variants to reduce stack size:

```rust
// Old
Err(DataFusionError::ArrowError(e, None))

// New (v49+)
Err(DataFusionError::ArrowError(Box::new(e), None))
```

### Trait Method Signature Changes

Check these traits for signature changes:
- `ExecutionPlan` - `execute()`, `properties()`, `with_new_children()`
- `TableProvider` - `scan()` parameter types
- `FileFormat` - new required methods in v46+
- `FileSource` - new trait in v46+, major changes in v51-52
- `FileOpener` - `open()` parameter and return type changes
- `CatalogProvider` / `SchemaProvider` - `&SessionState` → `&dyn Session`

### FileFormat/FileSource Changes (v46-52)

This is the area with the most churn. Key changes:

**v46**: `FileSource` trait introduced
**v48**: Filter pushdown moved to `FileSource::try_pushdown_filters`
**v51**:
  - `TableSchema` struct introduced (replaces `SchemaRef` in many places)
  - `FileOpenFuture` uses `DataFusionError` instead of `ArrowError`
**v52**:
  - `FileSource::create_file_opener` returns `Result<Arc<dyn FileOpener>>`
  - Projection handling moved into `FileSource`
  - `try_pushdown_projection` must return `Some(...)` to accept projections

### Import Path Changes

Modules get reorganized. Common moves:
- `PartitionedFile`: `physical_plan` → `listing`
- `ListingTable`: core → `datafusion-catalog-listing` (re-exported)
- File sources: core → `datafusion-datasource-{csv,json,parquet,arrow}`

## Step 4: Check Trait Implementations

For each trait we implement, verify:

1. **Required methods**: New required methods may have been added
2. **Method signatures**: Parameters and return types may have changed
3. **Default implementations**: Behavior of default impls may have changed

Use `cargo doc` or check docs.rs to see current trait definitions:
```
https://docs.rs/datafusion/VERSION/datafusion/
```

## Step 5: Run Tests

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

## Step 6: Update Documentation

After a successful upgrade:
1. Update this file's version compatibility matrix
2. Update CLAUDE.md if patterns changed significantly
3. Note any Java-side changes needed in the commit message

## Common Patterns

### Searching DataFusion Source

When you need to understand a trait or error:

```bash
# Find where an error message comes from
grep -rn "error message" ~/.cargo/registry/src/*/datafusion-*VERSION/

# See how a trait method is implemented
grep -A 30 "fn method_name" ~/.cargo/registry/src/*/datafusion-*VERSION/src/*.rs
```

### Projection Pushdown Pattern (v52+)

FileSource implementations must handle projection pushdown:

```rust
fn try_pushdown_projection(
    &self,
    projection: &ProjectionExprs,
) -> Result<Option<Arc<dyn FileSource>>> {
    // Return Some(...) to accept the projection
    // Return None = error in v52 (treated as "unsupported")
    let mut source = self.clone();
    let new_projection = self.projection.source.try_merge(projection)?;
    source.projection = SplitProjection::new(
        self.table_schema.file_schema(),
        &new_projection,
    );
    Ok(Some(Arc::new(source)))
}
```

### Shared Ownership for Cloneable Sources

If a source needs to be cloneable (for projection pushdown), wrap raw pointers in Arc:

```rust
struct CallbacksHolder {
    ptr: *mut Callbacks,
}

impl Drop for CallbacksHolder {
    fn drop(&mut self) {
        // Release callback
    }
}

#[derive(Clone)]
struct MySource {
    callbacks: Arc<CallbacksHolder>,  // Shared ownership
    // ...
}
```

## Upgrade History

| Version | Date | Notes |
|---------|------|-------|
| 45 → 52.1 | 2026-02 | Major FileSource refactor, projection pushdown |
