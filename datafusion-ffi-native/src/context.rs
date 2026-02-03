use arrow::array::StructArray;
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error_return, set_error_return_null};

/// Create a new SessionContext.
///
/// # Returns
/// A pointer to the SessionContext.
///
/// # Safety
/// The caller must call `datafusion_context_destroy` to free the context.
#[no_mangle]
pub extern "C" fn datafusion_context_create() -> *mut c_void {
    let context = SessionContext::new();
    Box::into_raw(Box::new(context)) as *mut c_void
}

/// Destroy a SessionContext.
///
/// # Safety
/// The pointer must have been created by `datafusion_context_create`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_destroy(ctx: *mut c_void) {
    if !ctx.is_null() {
        drop(Box::from_raw(ctx as *mut SessionContext));
    }
}

/// Register a RecordBatch as a table in the context using the Arrow C Data Interface.
///
/// # Arguments
/// * `ctx` - Pointer to the SessionContext
/// * `name` - Table name (null-terminated C string)
/// * `schema` - Pointer to FFI_ArrowSchema
/// * `array` - Pointer to FFI_ArrowArray
/// * `error_out` - Pointer to receive error message (caller must free with datafusion_free_string)
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid. The schema and array are consumed by this function.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_record_batch(
    ctx: *mut c_void,
    name: *const c_char,
    schema: *mut FFI_ArrowSchema,
    array: *mut FFI_ArrowArray,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null() || name.is_null() || schema.is_null() || array.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let context = &*(ctx as *mut SessionContext);

    // Get table name
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid table name: {}", e)),
    };

    // Import the Arrow data from FFI
    // We need to take ownership of the FFI structs
    let ffi_schema = std::ptr::read(schema);
    let ffi_array = std::ptr::read(array);

    // Import the array data using the schema
    let array_data = match from_ffi(ffi_array, &ffi_schema) {
        Ok(d) => d,
        Err(e) => return set_error_return(error_out, &format!("Failed to import array: {}", e)),
    };

    // The imported data is a struct array representing the record batch
    let struct_array = StructArray::from(array_data);

    // Extract schema from the struct array fields
    let schema = Schema::new(struct_array.fields().clone());
    let batch = RecordBatch::from(struct_array);

    // Create a MemTable from the batch and register it
    match datafusion::datasource::MemTable::try_new(Arc::new(schema), vec![vec![batch]]) {
        Ok(table) => {
            if let Err(e) = context.register_table(&name_str, Arc::new(table)) {
                return set_error_return(error_out, &format!("Failed to register table: {}", e));
            }
        }
        Err(e) => return set_error_return(error_out, &format!("Failed to create memory table: {}", e)),
    }

    0
}

/// Execute a SQL query and return a DataFrame.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `ctx` - Pointer to the SessionContext
/// * `sql` - SQL query string (null-terminated C string)
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to DataFrame on success, null on error.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_sql(
    rt: *mut c_void,
    ctx: *mut c_void,
    sql: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || ctx.is_null() || sql.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let context = &*(ctx as *mut SessionContext);

    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return_null(error_out, &format!("Invalid SQL string: {}", e)),
    };

    runtime.block_on(async {
        match context.sql(sql_str).await {
            Ok(df) => Box::into_raw(Box::new(df)) as *mut c_void,
            Err(e) => set_error_return_null(error_out, &format!("SQL execution failed: {}", e)),
        }
    })
}
