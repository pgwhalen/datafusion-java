use std::ffi::{c_char, CString};
use std::ptr;

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

/// Clear the error pointer (set to null).
pub fn clear_error(error_out: *mut *mut c_char) {
    if !error_out.is_null() {
        unsafe {
            *error_out = ptr::null_mut();
        }
    }
}

/// Get the length of a null-terminated C string (not including the null terminator).
///
/// # Safety
/// The pointer must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn datafusion_string_len(s: *const c_char) -> usize {
    if s.is_null() {
        return 0;
    }
    std::ffi::CStr::from_ptr(s).to_bytes().len()
}

/// Free a string that was allocated by Rust.
///
/// # Safety
/// The pointer must have been allocated by Rust using CString::into_raw().
#[no_mangle]
pub unsafe extern "C" fn datafusion_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}
