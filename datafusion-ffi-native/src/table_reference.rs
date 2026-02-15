//! FFI helpers for writing `TableReference` into output parameters.

use datafusion::common::TableReference;
use std::ffi::{c_char, CString};

/// Write a `TableReference` into FFI output parameters.
///
/// - `rel_type_out`: receives 0 (none), 1 (bare), 2 (partial), or 3 (full)
/// - `rel_strs_out`: receives up to 3 `CString` pointers (caller must free via `CString::from_raw`)
///
/// # Safety
/// - `rel_type_out` must be a valid pointer to an `i32`
/// - `rel_strs_out` must point to an array of at least 3 `*const c_char` elements
pub unsafe fn write_table_reference(
    relation: &Option<TableReference>,
    rel_type_out: *mut i32,
    rel_strs_out: *mut *const c_char,
) {
    match relation {
        None => {
            *rel_type_out = 0;
        }
        Some(rel) => match rel {
            TableReference::Bare { table } => {
                *rel_type_out = 1;
                let table_cstr = CString::new(table.as_ref()).unwrap_or_default();
                *rel_strs_out.add(0) = table_cstr.into_raw();
            }
            TableReference::Partial { schema, table } => {
                *rel_type_out = 2;
                let schema_cstr = CString::new(schema.as_ref()).unwrap_or_default();
                let table_cstr = CString::new(table.as_ref()).unwrap_or_default();
                *rel_strs_out.add(0) = schema_cstr.into_raw();
                *rel_strs_out.add(1) = table_cstr.into_raw();
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                *rel_type_out = 3;
                let catalog_cstr = CString::new(catalog.as_ref()).unwrap_or_default();
                let schema_cstr = CString::new(schema.as_ref()).unwrap_or_default();
                let table_cstr = CString::new(table.as_ref()).unwrap_or_default();
                *rel_strs_out.add(0) = catalog_cstr.into_raw();
                *rel_strs_out.add(1) = schema_cstr.into_raw();
                *rel_strs_out.add(2) = table_cstr.into_raw();
            }
        },
    }
}
