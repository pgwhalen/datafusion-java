//! Arrow C Data Interface FFI struct transfer helpers (Rust side).
//!
//! Mirrors the Java-side `ArrowFfiUtil`: this is the single home for code that constructs,
//! exports, or imports `FFI_ArrowSchema` / `FFI_ArrowArray` structs across the boundary.
//! Centralizing it keeps the unsafe `std::ptr::write` / `try_from` / `to_ffi` / `from_ffi`
//! patterns in one place so individual `Foreign*` wrappers stay free of low-level Arrow FFI
//! mechanics.
//!
//! Errors flow through `Box<DfError>` via the `From` impls in `bridge.rs`, so this module — like
//! `diplomat_util` — must not live inside a `#[diplomat::bridge]` mod.
//!
//! Reach for this module any time you are touching an `FFI_ArrowSchema` or `FFI_ArrowArray`
//! struct; reach for `udf_common` for UDF-shaped upcall plumbing built on top of these helpers.

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::{DataFusionError, Result as DFResult};

use crate::bridge::ffi::DfError;

/// Write an Arrow schema as `FFI_ArrowSchema` to a Java-provided `out_addr`. Centralizes the
/// null-address check, `FFI_ArrowSchema::try_from`, and the `unsafe { std::ptr::write(...) }`.
///
/// **Always use this** in any bridge method that exports a schema to a Java-allocated address —
/// do not open-code the pattern. If the schema must be constructed first (e.g., from `Field`s),
/// build it then pass a reference:
/// `export_schema_to(&ArrowSchema::new(fields), out_addr)`.
pub(crate) fn export_schema_to(
    schema: &ArrowSchema,
    out_addr: usize,
) -> Result<(), Box<DfError>> {
    if out_addr == 0 {
        return Err("Null output address".into());
    }
    let ffi_schema = FFI_ArrowSchema::try_from(schema)?;
    unsafe {
        std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
    }
    Ok(())
}

/// Export a single field as an `FFI_ArrowSchema`, wrapped in a parent Schema (format `"+s"`) so
/// the Java side can import it uniformly via `Data.importSchema`.
pub(crate) fn export_field_as_ffi_schema(field: &Field) -> DFResult<FFI_ArrowSchema> {
    let schema = ArrowSchema::new(vec![field.clone()]);
    FFI_ArrowSchema::try_from(&schema)
        .map_err(|e| DataFusionError::External(format!("Failed to export field: {}", e).into()))
}

/// Export a list of field refs as a vector of `FFI_ArrowSchema`s.
pub(crate) fn export_field_refs_as_ffi_schemas(
    fields: &[FieldRef],
) -> DFResult<Vec<FFI_ArrowSchema>> {
    fields
        .iter()
        .map(|f| export_field_as_ffi_schema(f.as_ref()))
        .collect()
}

/// Export a list of data types as `FFI_ArrowSchema`s by wrapping each in an anonymous field.
pub(crate) fn export_types_as_ffi_schemas(types: &[DataType]) -> DFResult<Vec<FFI_ArrowSchema>> {
    types
        .iter()
        .map(|dt| export_field_as_ffi_schema(&Field::new("", dt.clone(), true)))
        .collect()
}

/// Import a `Field` from an `FFI_ArrowSchema` (unwrapping the single-field Schema convention
/// used by `export_field_as_ffi_schema`).
pub(crate) fn import_field_from_ffi_schema(ffi: &FFI_ArrowSchema) -> DFResult<Field> {
    let schema = ArrowSchema::try_from(ffi)
        .map_err(|e| DataFusionError::External(format!("Failed to import field: {}", e).into()))?;
    Ok(schema.field(0).clone())
}

/// Export Arrow arrays as `(FFI_ArrowArray, FFI_ArrowSchema)` pairs for FFI.
pub(crate) fn export_arrays_as_ffi(
    arrays: &[ArrayRef],
) -> DFResult<Vec<(FFI_ArrowArray, FFI_ArrowSchema)>> {
    arrays
        .iter()
        .map(|array| {
            to_ffi(&array.to_data()).map_err(|e| {
                DataFusionError::External(format!("Failed to export array: {}", e).into())
            })
        })
        .collect()
}
