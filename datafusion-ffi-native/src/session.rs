use arrow::ffi::FFI_ArrowSchema;
use datafusion::catalog::Session;
use datafusion::logical_expr::Expr;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::{clear_error, set_error_return_null};

/// Creates a physical expression from logical filter expressions using the session state.
///
/// The filters are conjoined with AND and compiled using the session's state
/// and the provided table schema.
///
/// # Safety
/// - `session` must be a valid pointer to a boxed `*const dyn Session` fat pointer
/// - `filter_ptrs` must point to `filter_count` valid `*const Expr` pointers
/// - `schema_ffi` must be a valid FFI_ArrowSchema pointer
#[no_mangle]
pub unsafe extern "C" fn datafusion_session_create_physical_expr(
    session: *mut c_void,
    filter_ptrs: *const *const c_void,
    filter_count: usize,
    schema_ffi: *const FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if session.is_null() {
        return set_error_return_null(error_out, "Session is null");
    }
    if schema_ffi.is_null() {
        return set_error_return_null(error_out, "Schema is null");
    }
    if filter_count == 0 {
        return set_error_return_null(error_out, "No filters to create physical expression from");
    }
    if filter_ptrs.is_null() {
        return set_error_return_null(error_out, "filter_ptrs is null");
    }

    // Dereference the boxed fat pointer to get the session reference
    let session_fat_ptr = *(session as *const *const dyn Session);
    let session_ref = &*session_fat_ptr;

    // Convert FFI schema to Arrow schema
    let schema = match arrow::datatypes::Schema::try_from(&*schema_ffi) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            return set_error_return_null(error_out, &format!("Invalid schema: {}", e));
        }
    };

    // Read filter expressions from the pointer array
    let filters: Vec<&Expr> = (0..filter_count)
        .map(|i| &*(*filter_ptrs.add(i) as *const Expr))
        .collect();

    // Conjoin all filters with AND
    let combined = filters
        .iter()
        .cloned()
        .cloned()
        .reduce(|a, b| a.and(b))
        .unwrap();

    // Create physical expression from the logical expression
    let df_schema = match datafusion::common::DFSchema::try_from(schema.as_ref().clone()) {
        Ok(s) => s,
        Err(e) => {
            return set_error_return_null(
                error_out,
                &format!("Failed to create DFSchema: {}", e),
            );
        }
    };

    let physical_expr = match session_ref.create_physical_expr(combined, &df_schema) {
        Ok(expr) => expr,
        Err(e) => {
            return set_error_return_null(
                error_out,
                &format!("Failed to create physical expression: {}", e),
            );
        }
    };

    Box::into_raw(Box::new(physical_expr)) as *mut c_void
}
