//! Shared FFI helpers called by `NativeUtil` on the Java side.
//!
//! These are not specific to any single Handle or Ffi class.

use std::ffi::{c_char, c_void, CStr, CString};

use abi_stable::std_types::RString;

/// Get the length of a null-terminated C string (not including the null terminator).
///
/// # Safety
/// The pointer must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn datafusion_string_len(s: *const c_char) -> usize {
    if s.is_null() {
        return 0;
    }
    CStr::from_ptr(s).to_bytes().len()
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

/// Return sizeof(RString) for Java to know error payload size.
#[no_mangle]
pub extern "C" fn datafusion_rstring_size() -> usize {
    std::mem::size_of::<RString>()
}

/// Write an RString into the output buffer. Called by Java for error returns
/// in `poll_next` and `execute`. Java provides UTF-8 bytes; Rust constructs
/// an `RString` and writes it into the output.
///
/// # Safety
/// - `ptr` must point to `len` valid UTF-8 bytes.
/// - `out` must point to a buffer of at least `size_of::<RString>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_rstring(
    ptr: *const u8,
    len: usize,
    out: *mut c_void,
) {
    let bytes = std::slice::from_raw_parts(ptr, len);
    let s = std::str::from_utf8_unchecked(bytes);
    let rstring = RString::from(s);
    std::ptr::write(out as *mut RString, rstring);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rstring_helper() {
        let msg = "hello world";
        let mut out: std::mem::MaybeUninit<RString> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_rstring(
                msg.as_ptr(),
                msg.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let rstring = out.assume_init();
            assert_eq!(rstring.as_str(), "hello world");
        }
    }

    #[test]
    fn test_rstring_size() {
        let rstring_size = std::mem::size_of::<RString>();
        assert!(rstring_size > 0, "RString should have non-zero size");
    }
}
