//! C-compatible callback struct definitions for Java-backed providers.
//!
//! These structs define the callback interface that Java uses to implement
//! DataFusion's TableProvider, ExecutionPlan, SchemaProvider, and CatalogProvider
//! traits. Java creates upcall stubs that Rust stores and invokes.

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use std::ffi::{c_char, c_void};

/// Callback struct for a Java-backed RecordBatchReader.
///
/// This corresponds to Java's RecordBatchReader interface. Rust iterates the reader
/// by calling `load_next_batch_fn` repeatedly until it returns 0 (end of stream).
#[repr(C)]
pub struct JavaRecordBatchReaderCallbacks {
    /// Opaque pointer to the Java object (GlobalRef or similar)
    pub java_object: *mut c_void,

    /// Loads the next batch into the provided FFI structs.
    ///
    /// Returns:
    ///   1 = batch available (array_out and schema_out populated)
    ///   0 = end of stream
    ///  -1 = error (error_out populated)
    pub load_next_batch_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        array_out: *mut FFI_ArrowArray,
        schema_out: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this reader. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

/// FFI bridge struct for plan properties, written by Java's `properties_fn` callback.
///
/// Maps 1:1 to DataFusion's `PlanProperties` and Java's `PlanProperties` record.
#[repr(C)]
pub struct FFI_PlanProperties {
    /// Number of output partitions.
    pub output_partitioning: i32,
    /// Emission type (0=Incremental, 1=Final, 2=Both).
    pub emission_type: i32,
    /// Boundedness (0=Bounded, 1=Unbounded).
    pub boundedness: i32,
}

/// Callback struct for a Java-backed ExecutionPlan.
///
/// This corresponds to Java's ExecutionPlan interface.
#[repr(C)]
pub struct JavaExecutionPlanCallbacks {
    /// Opaque pointer to the Java object
    pub java_object: *mut c_void,

    /// Gets the schema of the execution plan output.
    ///
    /// Returns 0 on success, -1 on error.
    pub schema_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        schema_out: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Gets the plan properties (partitioning, emission type, boundedness).
    ///
    /// Writes to the provided FFI_PlanProperties struct.
    pub properties_fn:
        unsafe extern "C" fn(java_object: *mut c_void, properties_out: *mut FFI_PlanProperties),

    /// Executes the plan for the given partition.
    ///
    /// Returns a new JavaRecordBatchReaderCallbacks pointer on success.
    /// Returns 0 on success, -1 on error.
    pub execute_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        partition: usize,
        reader_out: *mut *mut JavaRecordBatchReaderCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this plan. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

/// Callback struct for a Java-backed TableProvider.
///
/// This corresponds to Java's TableProvider interface.
#[repr(C)]
pub struct JavaTableProviderCallbacks {
    /// Opaque pointer to the Java object
    pub java_object: *mut c_void,

    /// Gets the schema of the table.
    ///
    /// Returns 0 on success, -1 on error.
    pub schema_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        schema_out: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Returns the table type (0=BASE, 1=VIEW, 2=TEMPORARY).
    pub table_type_fn: unsafe extern "C" fn(java_object: *mut c_void) -> i32,

    /// Creates a scan (execution plan) for this table.
    ///
    /// projection: array of column indices, or null for all columns
    /// projection_len: length of projection array
    /// limit: maximum rows, or -1 for no limit
    ///
    /// Returns a new JavaExecutionPlanCallbacks pointer on success.
    /// Returns 0 on success, -1 on error.
    pub scan_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        projection: *const usize,
        projection_len: usize,
        limit: i64,
        plan_out: *mut *mut JavaExecutionPlanCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this provider. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

/// Callback struct for a Java-backed SchemaProvider.
///
/// This corresponds to Java's SchemaProvider interface.
#[repr(C)]
pub struct JavaSchemaProviderCallbacks {
    /// Opaque pointer to the Java object
    pub java_object: *mut c_void,

    /// Gets the list of table names.
    ///
    /// names_out: pointer to receive array of C strings
    /// names_len_out: pointer to receive number of names
    ///
    /// Returns 0 on success, -1 on error.
    /// Caller must free the returned array using the free_string_array function.
    pub table_names_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        names_out: *mut *mut *mut c_char,
        names_len_out: *mut usize,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Gets a table by name.
    ///
    /// name: table name (null-terminated C string)
    /// table_out: pointer to receive JavaTableProviderCallbacks, or null if not found
    ///
    /// Returns 0 on success (even if table not found), -1 on error.
    pub table_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        name: *const c_char,
        table_out: *mut *mut JavaTableProviderCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Checks if a table exists.
    ///
    /// Returns 1 if exists, 0 if not exists.
    pub table_exists_fn: unsafe extern "C" fn(java_object: *mut c_void, name: *const c_char) -> i32,

    /// Called when Rust is done with this provider. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

/// Callback struct for a Java-backed CatalogProvider.
///
/// This corresponds to Java's CatalogProvider interface.
#[repr(C)]
pub struct JavaCatalogProviderCallbacks {
    /// Opaque pointer to the Java object
    pub java_object: *mut c_void,

    /// Gets the list of schema names.
    ///
    /// names_out: pointer to receive array of C strings
    /// names_len_out: pointer to receive number of names
    ///
    /// Returns 0 on success, -1 on error.
    /// Caller must free the returned array using the free_string_array function.
    pub schema_names_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        names_out: *mut *mut *mut c_char,
        names_len_out: *mut usize,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Gets a schema by name.
    ///
    /// name: schema name (null-terminated C string)
    /// schema_out: pointer to receive JavaSchemaProviderCallbacks, or null if not found
    ///
    /// Returns 0 on success (even if schema not found), -1 on error.
    pub schema_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        name: *const c_char,
        schema_out: *mut *mut JavaSchemaProviderCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this provider. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

/// Free a string array allocated by Java.
///
/// # Safety
/// The array must have been allocated using Java's arena and the strings
/// must be valid C strings.
#[no_mangle]
pub unsafe extern "C" fn datafusion_free_string_array(strings: *mut *mut c_char, len: usize) {
    if strings.is_null() {
        return;
    }

    // Free each string
    for i in 0..len {
        let s = *strings.add(i);
        if !s.is_null() {
            crate::datafusion_free_string(s);
        }
    }

    // Note: The array itself is allocated by Java's Arena, so we don't free it here.
    // Java's Arena will free it when the Arena is closed.
}
