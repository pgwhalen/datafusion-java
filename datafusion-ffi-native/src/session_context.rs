//! FFI functions for SessionContext lifecycle and operations.
//!
//! This module contains all `#[no_mangle]` entry points called by
//! `SessionContextFfi.java`: context creation/destruction, SQL execution,
//! table registration (record batches, catalogs, listing tables), and
//! Tokio runtime management.

use arrow::array::StructArray;
use arrow::datatypes::Schema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error_return, set_error_return_null};
use crate::file_format::{JavaBackedFileFormat, JavaFileFormatCallbacks};
use crate::session_state::SessionStateWithRuntime;

use datafusion::catalog::CatalogProvider;
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;

// ============================================================================
// Tokio Runtime
// ============================================================================

/// Create a new Tokio runtime for executing async DataFusion operations.
///
/// # Returns
/// A pointer to the runtime, or null if creation failed.
///
/// # Safety
/// The caller must call `datafusion_runtime_destroy` to free the runtime.
#[no_mangle]
pub extern "C" fn datafusion_runtime_create() -> *mut c_void {
    match Runtime::new() {
        Ok(runtime) => Box::into_raw(Box::new(runtime)) as *mut c_void,
        Err(_) => std::ptr::null_mut(),
    }
}

/// Destroy a Tokio runtime.
///
/// # Safety
/// The pointer must have been created by `datafusion_runtime_create`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_runtime_destroy(rt: *mut c_void) {
    if !rt.is_null() {
        let runtime = Box::from_raw(rt as *mut Runtime);
        runtime.shutdown_timeout(Duration::from_millis(100));
    }
}

// ============================================================================
// SessionContext lifecycle
// ============================================================================

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

/// Create a new SessionContext with configuration options.
///
/// Options are passed as parallel arrays of null-terminated C strings (keys and values).
/// Keys use dotted notation (e.g., "datafusion.execution.batch_size").
///
/// # Arguments
/// * `keys` - Pointer to array of C string pointers (config keys)
/// * `values` - Pointer to array of C string pointers (config values)
/// * `len` - Number of key/value pairs
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// A pointer to the SessionContext, or null on error.
///
/// # Safety
/// All pointers must be valid. The caller must call `datafusion_context_destroy` to free.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_create_with_config(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if len > 0 && (keys.is_null() || values.is_null()) {
        return set_error_return_null(error_out, "Null keys or values pointer with non-zero length");
    }

    let mut settings = HashMap::with_capacity(len);
    for i in 0..len {
        let key_ptr = *keys.add(i);
        let value_ptr = *values.add(i);

        if key_ptr.is_null() || value_ptr.is_null() {
            return set_error_return_null(
                error_out,
                &format!("Null key or value at index {}", i),
            );
        }

        let key = match CStr::from_ptr(key_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(e) => {
                return set_error_return_null(
                    error_out,
                    &format!("Invalid key at index {}: {}", i, e),
                )
            }
        };

        let value = match CStr::from_ptr(value_ptr).to_str() {
            Ok(s) => s.to_string(),
            Err(e) => {
                return set_error_return_null(
                    error_out,
                    &format!("Invalid value at index {}: {}", i, e),
                )
            }
        };

        settings.insert(key, value);
    }

    let config = match SessionConfig::from_string_hash_map(&settings) {
        Ok(c) => c,
        Err(e) => {
            return set_error_return_null(
                error_out,
                &format!("Failed to create SessionConfig: {}", e),
            )
        }
    };

    let context = SessionContext::new_with_config(config);
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

// ============================================================================
// SQL execution
// ============================================================================

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

// ============================================================================
// SessionState
// ============================================================================

/// Create a SessionState from an existing SessionContext.
///
/// The returned pointer owns a `SessionStateWithRuntime` containing the state and a new
/// Tokio runtime. This means the SessionState can outlive the SessionContext.
///
/// # Returns
/// A pointer to SessionStateWithRuntime, or null on error.
///
/// # Safety
/// `ctx` must have been created by `datafusion_context_create`.
/// The caller must call `datafusion_session_state_destroy` to free the result.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_state(
    ctx: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if ctx.is_null() {
        return set_error_return_null(error_out, "Context is null");
    }

    let context = &*(ctx as *mut SessionContext);

    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            return set_error_return_null(
                error_out,
                &format!("Failed to create Tokio runtime: {}", e),
            )
        }
    };

    let state = context.state();

    let sw = SessionStateWithRuntime { state, runtime };
    Box::into_raw(Box::new(sw)) as *mut c_void
}

// ============================================================================
// Table registration
// ============================================================================

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
        Err(e) => {
            return set_error_return(error_out, &format!("Failed to create memory table: {}", e))
        }
    }

    0
}

/// Register a catalog with the session context.
///
/// Java constructs an `FFI_CatalogProvider` in arena memory and passes a pointer.
/// Rust reads the struct, converts it to a `ForeignCatalogProvider` (via `From`),
/// and registers it with the context.
///
/// # Arguments
/// * `ctx` - Pointer to the SessionContext
/// * `name` - Catalog name (null-terminated C string)
/// * `ffi_provider` - Pointer to FFI_CatalogProvider (read, not consumed)
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid. `ffi_provider` must point to a valid FFI_CatalogProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_catalog(
    ctx: *mut c_void,
    name: *const c_char,
    ffi_provider: *const FFI_CatalogProvider,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null() || name.is_null() || ffi_provider.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let context = &*(ctx as *mut SessionContext);

    // Get catalog name
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid catalog name: {}", e)),
    };

    // Convert FFI_CatalogProvider -> Arc<dyn CatalogProvider + Send>
    // This clones the FFI struct (via our clone callback) and wraps in ForeignCatalogProvider
    let provider: Arc<dyn CatalogProvider + Send> = (&*ffi_provider).into();

    // Register the catalog (returns the old catalog if one was registered with the same name)
    context.register_catalog(&name_str, provider);

    0
}

/// Create a listing table from config and register it with the session context.
///
/// Takes ownership of `format_callbacks`. All other pointer args are borrowed.
///
/// # Arguments
/// * `ctx` - Pointer to SessionContext
/// * `rt` - Pointer to Tokio Runtime
/// * `name` - Table name (null-terminated C string)
/// * `urls` - Array of directory path pointers (null-terminated C strings)
/// * `urls_len` - Number of URLs in the array
/// * `file_extension` - File extension (null-terminated C string, e.g. ".tsv")
/// * `schema` - Pointer to FFI_ArrowSchema (borrowed, Java owns release)
/// * `format_callbacks` - Pointer to JavaFileFormatCallbacks (takes ownership)
/// * `collect_stat` - Whether to collect file statistics
/// * `target_partitions` - Target number of partitions
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_listing_table(
    ctx: *mut c_void,
    rt: *mut c_void,
    name: *const c_char,
    urls: *const *const c_char,
    urls_len: usize,
    file_extension: *const c_char,
    schema: *mut FFI_ArrowSchema,
    format_callbacks: *mut JavaFileFormatCallbacks,
    collect_stat: i32,
    target_partitions: usize,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null()
        || rt.is_null()
        || name.is_null()
        || urls.is_null()
        || urls_len == 0
        || file_extension.is_null()
        || schema.is_null()
        || format_callbacks.is_null()
    {
        return set_error_return(error_out, "Null pointer argument or empty URLs");
    }

    let context = &*(ctx as *mut SessionContext);
    let runtime = &*(rt as *mut Runtime);

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid table name: {}", e)),
    };

    let ext_str = match CStr::from_ptr(file_extension).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid file extension: {}", e)),
    };

    // Import schema from FFI. We read the FFI_ArrowSchema into an owned value so that
    // its Drop impl will call the release callback, freeing Java-allocated buffers.
    let ffi_schema = std::ptr::read(schema);
    let arrow_schema = match Schema::try_from(&ffi_schema) {
        Ok(s) => Arc::new(s),
        Err(e) => return set_error_return(error_out, &format!("Failed to import schema: {}", e)),
    };
    // ffi_schema is dropped here, calling the release callback

    // Create the JavaBackedFileFormat (takes ownership of callbacks)
    let format = Arc::new(JavaBackedFileFormat {
        callbacks: format_callbacks,
        schema: Arc::clone(&arrow_schema),
        extension: ext_str.clone(),
    });

    // Build ListingOptions
    let options = ListingOptions::new(format as Arc<dyn FileFormat>)
        .with_file_extension(ext_str)
        .with_collect_stat(collect_stat != 0)
        .with_target_partitions(target_partitions);

    // Parse URLs and create table config
    runtime.block_on(async {
        let urls_slice = std::slice::from_raw_parts(urls, urls_len);
        let mut table_urls = Vec::with_capacity(urls_len);
        for &url_ptr in urls_slice {
            let url_str = match CStr::from_ptr(url_ptr).to_str() {
                Ok(s) => s.to_string(),
                Err(e) => {
                    return set_error_return(error_out, &format!("Invalid URL: {}", e))
                }
            };
            let table_url = match ListingTableUrl::parse(&url_str) {
                Ok(u) => u,
                Err(e) => {
                    return set_error_return(
                        error_out,
                        &format!("Failed to parse URL: {}", e),
                    )
                }
            };
            table_urls.push(table_url);
        }

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(options)
            .with_schema(arrow_schema);

        let table = match ListingTable::try_new(config) {
            Ok(t) => t,
            Err(e) => {
                return set_error_return(
                    error_out,
                    &format!("Failed to create listing table: {}", e),
                )
            }
        };

        if let Err(e) = context.register_table(&name_str, Arc::new(table)) {
            return set_error_return(error_out, &format!("Failed to register table: {}", e));
        }

        0
    })
}
