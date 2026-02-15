use datafusion::error::DataFusionError;
use std::ffi::{c_char, CStr, CString};
use std::ptr;

/// Check the result of a Java callback and return a DataFusionError if it failed.
///
/// This is a reusable utility for handling errors from Java upcalls. It reads the
/// error message from `error_out` if available, otherwise uses the provided description.
///
/// # Safety
/// The `error_out` pointer must be valid. Note: error_out is Java-allocated and
/// managed by Java's arena, so we do NOT free it.
///
/// # Returns
/// - `Ok(())` if result == 0
/// - `Err(DataFusionError::Execution(...))` if result != 0
pub unsafe fn check_callback_result(
    result: i32,
    error_out: *mut c_char,
    operation: &str,
) -> Result<(), DataFusionError> {
    if result == 0 {
        return Ok(());
    }

    Err(callback_error(error_out, operation))
}

/// Create a DataFusionError from a callback error_out pointer.
///
/// Use this when you know an error occurred (e.g., in a match arm for the error case)
/// and need to construct the error without checking the result code.
///
/// # Safety
/// The `error_out` pointer must be valid. Note: error_out is Java-allocated and
/// managed by Java's arena, so we do NOT free it.
pub unsafe fn callback_error(error_out: *mut c_char, operation: &str) -> DataFusionError {
    let msg = if !error_out.is_null() {
        // Note: Don't free error_out - it's Java-allocated and managed by Java's arena
        CStr::from_ptr(error_out).to_string_lossy().to_string()
    } else {
        format!("Failed to {}", operation)
    };
    DataFusionError::Execution(msg)
}

/// Set an error message in the provided error_out pointer.
/// Returns true if an error was set.
pub fn set_error(error_out: *mut *mut c_char, message: &str) -> bool {
    if !error_out.is_null() {
        if let Ok(c_string) = CString::new(message) {
            unsafe {
                *error_out = c_string.into_raw();
            }
            return true;
        }
    }
    false
}

/// Set an error message and return -1 (for i32-returning FFI functions).
///
/// Use this in downcalls (Rust functions called from Java) to simplify the common pattern:
/// ```ignore
/// set_error(error_out, "...");
/// return -1;
/// ```
///
/// # Example
/// ```ignore
/// Err(e) => return set_error_return(error_out, &format!("Stream error: {}", e)),
/// ```
pub fn set_error_return(error_out: *mut *mut c_char, message: &str) -> i32 {
    set_error(error_out, message);
    -1
}

/// Set an error message and return null (for pointer-returning FFI functions).
///
/// Use this in downcalls (Rust functions called from Java) to simplify the common pattern:
/// ```ignore
/// set_error(error_out, "...");
/// return std::ptr::null_mut();
/// ```
///
/// # Example
/// ```ignore
/// Err(e) => return set_error_return_null(error_out, &format!("SQL failed: {}", e)),
/// ```
pub fn set_error_return_null<T>(error_out: *mut *mut c_char, message: &str) -> *mut T {
    set_error(error_out, message);
    ptr::null_mut()
}

/// Clear the error pointer (set to null).
pub fn clear_error(error_out: *mut *mut c_char) {
    if !error_out.is_null() {
        unsafe {
            *error_out = ptr::null_mut();
        }
    }
}

