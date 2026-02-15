//! Shared FFI helpers called by `NativeUtil` on the Java side.
//!
//! These are not specific to any single Handle or Ffi class.

use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Arc;

use abi_stable::std_types::{ROption, RString, RVec};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

use crate::execution::noop_task_ctx_provider;

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

// ============================================================================
// RVec<RString> helpers
// ============================================================================

/// Return sizeof(RVec<RString>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_rvec_rstring_size() -> usize {
    std::mem::size_of::<RVec<RString>>()
}

/// Create an RVec<RString> from parallel arrays of UTF-8 byte pointers and lengths.
///
/// # Safety
/// - `ptrs` must point to `count` valid `*const u8` pointers.
/// - `lens` must point to `count` valid `usize` values.
/// - Each `ptrs[i]` must point to `lens[i]` valid UTF-8 bytes.
/// - `out` must point to a buffer of at least `size_of::<RVec<RString>>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_rvec_rstring(
    ptrs: *const *const u8,
    lens: *const usize,
    count: usize,
    out: *mut c_void,
) {
    let mut rvec: RVec<RString> = RVec::with_capacity(count);
    for i in 0..count {
        let ptr = *ptrs.add(i);
        let len = *lens.add(i);
        let bytes = std::slice::from_raw_parts(ptr, len);
        let s = std::str::from_utf8_unchecked(bytes);
        rvec.push(RString::from(s));
    }
    std::ptr::write(out as *mut RVec<RString>, rvec);
}

// ============================================================================
// ROption<RString> helpers (for FFI_SchemaProvider.owner_name)
// ============================================================================

/// Return sizeof(ROption<RString>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_roption_rstring_size() -> usize {
    std::mem::size_of::<ROption<RString>>()
}

/// Write ROption<RString>::RNone into the output buffer.
///
/// # Safety
/// `out` must point to a buffer of at least `size_of::<ROption<RString>>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_roption_rstring_none(out: *mut c_void) {
    let none: ROption<RString> = ROption::RNone;
    std::ptr::write(out as *mut ROption<RString>, none);
}

// ============================================================================
// FFI_LogicalExtensionCodec helpers
// ============================================================================

/// Return sizeof(FFI_LogicalExtensionCodec) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_logical_codec_size() -> usize {
    std::mem::size_of::<FFI_LogicalExtensionCodec>()
}

/// Write a default/noop FFI_LogicalExtensionCodec into the output buffer.
///
/// Uses DefaultLogicalExtensionCodec and a noop FFI_TaskContextProvider.
/// The resulting codec is valid, clonable, and droppable.
///
/// # Safety
/// `out` must point to a buffer of at least `size_of::<FFI_LogicalExtensionCodec>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_noop_logical_codec(out: *mut c_void) {
    let codec = Arc::new(DefaultLogicalExtensionCodec {});
    let tcp = noop_task_ctx_provider();
    let ffi_codec = FFI_LogicalExtensionCodec::new(codec, None, tcp);
    std::ptr::write(out as *mut FFI_LogicalExtensionCodec, ffi_codec);
}

/// Clone an FFI_LogicalExtensionCodec from src into dest.
///
/// Calls the codec's clone function pointer to properly clone the internal state.
///
/// # Safety
/// - `src` must point to a valid FFI_LogicalExtensionCodec.
/// - `dest` must point to a buffer of at least `size_of::<FFI_LogicalExtensionCodec>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_clone_logical_codec(
    src: *const c_void,
    dest: *mut c_void,
) {
    let codec = &*(src as *const FFI_LogicalExtensionCodec);
    let cloned = codec.clone();
    std::ptr::write(dest as *mut FFI_LogicalExtensionCodec, cloned);
}

/// Drop an FFI_LogicalExtensionCodec in place, releasing its internal resources.
///
/// After this call, the memory at `codec_ptr` is invalid and must not be used.
///
/// # Safety
/// `codec_ptr` must point to a valid, initialized FFI_LogicalExtensionCodec
/// that has not already been dropped.
#[no_mangle]
pub unsafe extern "C" fn datafusion_drop_logical_codec(codec_ptr: *mut c_void) {
    let codec = std::ptr::read(codec_ptr as *const FFI_LogicalExtensionCodec);
    drop(codec);
}

// ============================================================================
// Version and library marker helpers
// ============================================================================

/// Return the DataFusion FFI major version number.
///
/// This is the value that goes in the `version` field of all FFI provider structs.
/// Java looks up this symbol and stores the function pointer.
#[no_mangle]
pub extern "C" fn datafusion_ffi_version() -> u64 {
    datafusion_ffi::version()
}

/// Library marker function for Java-backed FFI structs.
///
/// Prevents the `TryFrom<&FFI_*>` impls from taking the "local" fast-path
/// (which would try to downcast private_data). Java looks up this symbol and
/// stores the function pointer in the `library_marker_id` field.
#[no_mangle]
pub extern "C" fn datafusion_java_marker_id() -> usize {
    static MARKER: u8 = 0;
    &MARKER as *const u8 as usize
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

    #[test]
    fn test_rvec_rstring_helper() {
        let strings = ["hello", "world", "test"];
        let ptrs: Vec<*const u8> = strings.iter().map(|s| s.as_ptr()).collect();
        let lens: Vec<usize> = strings.iter().map(|s| s.len()).collect();

        let mut out: std::mem::MaybeUninit<RVec<RString>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_rvec_rstring(
                ptrs.as_ptr(),
                lens.as_ptr(),
                ptrs.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 3);
            assert_eq!(rvec[0].as_str(), "hello");
            assert_eq!(rvec[1].as_str(), "world");
            assert_eq!(rvec[2].as_str(), "test");
        }
    }

    #[test]
    fn test_roption_rstring_none() {
        let mut out: std::mem::MaybeUninit<ROption<RString>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_roption_rstring_none(out.as_mut_ptr() as *mut c_void);
            let opt = out.assume_init();
            assert!(opt.is_none());
        }
    }

    #[test]
    fn test_noop_logical_codec() {
        let mut out: std::mem::MaybeUninit<FFI_LogicalExtensionCodec> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_noop_logical_codec(out.as_mut_ptr() as *mut c_void);
            let codec = out.assume_init();
            // Verify it can be cloned and dropped without panicking
            let cloned = codec.clone();
            drop(cloned);
            drop(codec);
        }
    }

    #[test]
    fn test_clone_logical_codec() {
        let mut src: std::mem::MaybeUninit<FFI_LogicalExtensionCodec> =
            std::mem::MaybeUninit::uninit();
        let mut dest: std::mem::MaybeUninit<FFI_LogicalExtensionCodec> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_noop_logical_codec(src.as_mut_ptr() as *mut c_void);
            datafusion_clone_logical_codec(
                src.as_ptr() as *const c_void,
                dest.as_mut_ptr() as *mut c_void,
            );
            let src_codec = src.assume_init();
            let dest_codec = dest.assume_init();
            drop(dest_codec);
            drop(src_codec);
        }
    }

    #[test]
    fn test_version() {
        let v = datafusion_ffi_version();
        assert!(v > 0, "Version should be positive");
    }
}
