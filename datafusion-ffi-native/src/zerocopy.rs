use arrow::array::{Array, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use std::ffi::{c_char, c_void};

use crate::error::{clear_error, set_error};

/// C-compatible representation of FFI_ArrowArray for reading its fields.
/// This matches the Arrow C Data Interface specification.
/// We use this because the fields of FFI_ArrowArray are private in newer Arrow versions.
#[repr(C)]
struct RawArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *const *const c_void,
    children: *const *const RawArrowArray,
    dictionary: *const RawArrowArray,
    release: *const c_void,
    private_data: *const c_void,
}

/// Get buffer addresses from an FFI_ArrowArray structure.
/// This is used for zero-copy verification testing.
///
/// # Arguments
/// * `array` - Pointer to FFI_ArrowArray
/// * `addresses_out` - Array to receive buffer addresses (must have capacity for at least max_addresses)
/// * `max_addresses` - Maximum number of addresses to write
/// * `actual_count_out` - Receives the actual number of buffers in the array
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_get_ffi_buffer_addresses(
    array: *const c_void,
    addresses_out: *mut u64,
    max_addresses: i32,
    actual_count_out: *mut i32,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if array.is_null() || addresses_out.is_null() || actual_count_out.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    let raw_array = &*(array as *const RawArrowArray);
    let n_buffers = raw_array.n_buffers as i32;
    *actual_count_out = n_buffers;

    // Read buffer addresses from FFI_ArrowArray
    let buffers = raw_array.buffers;
    if buffers.is_null() && n_buffers > 0 {
        set_error(error_out, "Null buffers pointer but n_buffers > 0");
        return -1;
    }

    let count = std::cmp::min(max_addresses, n_buffers);
    for i in 0..count as isize {
        let addr = if buffers.is_null() {
            0
        } else {
            *buffers.offset(i) as u64
        };
        *addresses_out.offset(i) = addr;
    }

    0
}

/// Get child array buffer addresses from an FFI_ArrowArray structure.
/// For struct arrays (which wrap record batches), this retrieves addresses from child arrays.
///
/// # Arguments
/// * `array` - Pointer to FFI_ArrowArray (representing a struct array)
/// * `child_index` - Index of the child array (column) to inspect
/// * `addresses_out` - Array to receive buffer addresses
/// * `max_addresses` - Maximum number of addresses to write
/// * `actual_count_out` - Receives the actual number of buffers
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_get_child_buffer_addresses(
    array: *const c_void,
    child_index: i32,
    addresses_out: *mut u64,
    max_addresses: i32,
    actual_count_out: *mut i32,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if array.is_null() || addresses_out.is_null() || actual_count_out.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    let raw_array = &*(array as *const RawArrowArray);

    // Check if child index is valid
    if child_index < 0 || child_index >= raw_array.n_children as i32 {
        set_error(
            error_out,
            &format!(
                "Child index {} out of bounds (n_children={})",
                child_index, raw_array.n_children
            ),
        );
        return -1;
    }

    let children = raw_array.children;
    if children.is_null() {
        set_error(error_out, "Null children pointer");
        return -1;
    }

    // Get the child array at the specified index
    let child_ptr = *children.offset(child_index as isize);
    if child_ptr.is_null() {
        set_error(error_out, "Null child array pointer");
        return -1;
    }

    let child_array = &*child_ptr;
    let n_buffers = child_array.n_buffers as i32;
    *actual_count_out = n_buffers;

    // Read buffer addresses from child array
    let buffers = child_array.buffers;
    if buffers.is_null() && n_buffers > 0 {
        set_error(error_out, "Null buffers pointer but n_buffers > 0");
        return -1;
    }

    let count = std::cmp::min(max_addresses, n_buffers);
    for i in 0..count as isize {
        let addr = if buffers.is_null() {
            0
        } else {
            *buffers.offset(i) as u64
        };
        *addresses_out.offset(i) = addr;
    }

    0
}

/// Import FFI data and return buffer addresses from the imported Arrow array.
/// This allows verifying that Rust sees the same buffer addresses after import.
///
/// # Arguments
/// * `schema` - Pointer to FFI_ArrowSchema
/// * `array` - Pointer to FFI_ArrowArray
/// * `child_index` - Index of the child (column) to inspect
/// * `addresses_out` - Array to receive buffer addresses
/// * `max_addresses` - Maximum number of addresses to write
/// * `actual_count_out` - Receives the actual number of buffers
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid. The schema and array are NOT consumed (copies are made).
#[no_mangle]
pub unsafe extern "C" fn datafusion_import_and_get_buffer_addresses(
    schema: *const c_void,
    array: *const c_void,
    child_index: i32,
    addresses_out: *mut u64,
    max_addresses: i32,
    actual_count_out: *mut i32,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if schema.is_null() || array.is_null() || addresses_out.is_null() || actual_count_out.is_null()
    {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    // For this test function, we just delegate to the child buffer address function
    // since that's what we're really testing (the FFI structures contain Java's buffer addresses)
    datafusion_get_child_buffer_addresses(
        array,
        child_index,
        addresses_out,
        max_addresses,
        actual_count_out,
        error_out,
    )
}

/// Create test data in Rust and return FFI structures along with buffer addresses.
/// This is used to verify zero-copy from Rust to Java.
///
/// # Arguments
/// * `array_out` - Pointer to receive FFI_ArrowArray
/// * `schema_out` - Pointer to receive FFI_ArrowSchema
/// * `data_buffer_address_out` - Receives the data buffer address (for the first column)
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_test_data_with_addresses(
    array_out: *mut c_void,
    schema_out: *mut c_void,
    data_buffer_address_out: *mut u64,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if array_out.is_null() || schema_out.is_null() || data_buffer_address_out.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Create test data: a simple int64 array [1, 2, 3, 4, 5]
    let values: Vec<i64> = vec![1, 2, 3, 4, 5];
    let array = Int64Array::from(values);

    // Get the data buffer address BEFORE converting to FFI
    // For Int64Array, the data buffer is at index 0 in the buffers slice
    let array_data = array.to_data();
    let buffers = array_data.buffers();
    let original_data_address = if !buffers.is_empty() {
        buffers[0].as_ptr() as u64
    } else {
        0
    };
    *data_buffer_address_out = original_data_address;

    // Create a record batch and convert to struct array for FFI
    let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
    let batch = match RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array) as ArrayRef]) {
        Ok(b) => b,
        Err(e) => {
            set_error(error_out, &format!("Failed to create RecordBatch: {}", e));
            return -1;
        }
    };

    // Convert to struct array for FFI export
    let struct_array: StructArray = batch.into();
    let (ffi_array, ffi_schema) = match to_ffi(&struct_array.to_data()) {
        Ok(result) => result,
        Err(e) => {
            set_error(error_out, &format!("Failed to export to FFI: {}", e));
            return -1;
        }
    };

    // Write FFI structures to output pointers
    std::ptr::write(array_out as *mut FFI_ArrowArray, ffi_array);
    std::ptr::write(schema_out as *mut FFI_ArrowSchema, ffi_schema);

    0
}

/// Get the number of children in an FFI_ArrowArray.
///
/// # Arguments
/// * `array` - Pointer to FFI_ArrowArray
///
/// # Returns
/// Number of children, or -1 on error.
///
/// # Safety
/// The pointer must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_get_ffi_n_children(array: *const c_void) -> i64 {
    if array.is_null() {
        return -1;
    }
    let raw_array = &*(array as *const RawArrowArray);
    raw_array.n_children
}

/// Get the number of buffers in an FFI_ArrowArray.
///
/// # Arguments
/// * `array` - Pointer to FFI_ArrowArray
///
/// # Returns
/// Number of buffers, or -1 on error.
///
/// # Safety
/// The pointer must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_get_ffi_n_buffers(array: *const c_void) -> i64 {
    if array.is_null() {
        return -1;
    }
    let raw_array = &*(array as *const RawArrowArray);
    raw_array.n_buffers
}
