//! Size helpers, stub functions, and FfiFuture helpers for FFI_SchemaProvider.
//!
//! Java constructs `FFI_SchemaProvider` directly in Java arena memory.
//! This module provides size validation helpers, clone/release callbacks,
//! stub functions for unimplemented trait methods, and Rust helpers
//! for constructing FfiFuture return values.

use std::ffi::c_void;

use abi_stable::std_types::{ROption, RString};
use async_ffi::{FfiFuture, FutureExt};
use datafusion_ffi::schema_provider::FFI_SchemaProvider;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::util::FFIResult;

// ============================================================================
// Size helpers
// ============================================================================

/// Return sizeof(FFI_SchemaProvider) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_schema_provider_size() -> usize {
    std::mem::size_of::<FFI_SchemaProvider>()
}

/// Return sizeof(FfiFuture<FFIResult<ROption<FFI_TableProvider>>>) for Java validation.
///
/// This is the return type of the `table` callback.
#[no_mangle]
pub extern "C" fn datafusion_ffi_table_future_size() -> usize {
    std::mem::size_of::<FfiFuture<FFIResult<ROption<FFI_TableProvider>>>>()
}

/// Return sizeof(ROption<FFI_TableProvider>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_roption_table_provider_size() -> usize {
    std::mem::size_of::<ROption<FFI_TableProvider>>()
}

/// Return sizeof(FFIResult<ROption<FFI_TableProvider>>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_roption_table_provider_size() -> usize {
    std::mem::size_of::<FFIResult<ROption<FFI_TableProvider>>>()
}

// ============================================================================
// Clone and release callbacks
// ============================================================================

/// Clone an FFI_SchemaProvider.
///
/// Copies all fields, deep-clones the embedded logical_codec and owner_name.
///
/// Java stores this function's symbol in the `clone` field of FFI_SchemaProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_provider_clone(
    provider: &FFI_SchemaProvider,
) -> FFI_SchemaProvider {
    FFI_SchemaProvider {
        owner_name: provider.owner_name.clone(),
        table_names: provider.table_names,
        table: provider.table,
        register_table: provider.register_table,
        deregister_table: provider.deregister_table,
        table_exist: provider.table_exist,
        logical_codec: provider.logical_codec.clone(),
        clone: provider.clone,
        release: provider.release,
        version: provider.version,
        private_data: provider.private_data,
        library_marker_id: provider.library_marker_id,
    }
}

/// Release an FFI_SchemaProvider.
///
/// No-op for Java-backed providers.
///
/// Java stores this function's symbol in the `release` field of FFI_SchemaProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_provider_release(
    _provider: &mut FFI_SchemaProvider,
) {
    // No-op: private_data is NULL.
    // Rust's drop glue will drop logical_codec and owner_name automatically.
}

// ============================================================================
// Stub functions for unimplemented trait methods
// ============================================================================

/// Stub for register_table — always returns error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_provider_stub_register_table(
    _provider: &FFI_SchemaProvider,
    _name: RString,
    _table: FFI_TableProvider,
) -> FFIResult<ROption<FFI_TableProvider>> {
    FFIResult::RErr(RString::from(
        "register_table not supported from Java-backed SchemaProvider",
    ))
}

/// Stub for deregister_table — always returns error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_provider_stub_deregister_table(
    _provider: &FFI_SchemaProvider,
    _name: RString,
) -> FFIResult<ROption<FFI_TableProvider>> {
    FFIResult::RErr(RString::from(
        "deregister_table not supported from Java-backed SchemaProvider",
    ))
}

// ============================================================================
// FfiFuture construction helpers for the `table` callback
// ============================================================================

/// Create a ready FfiFuture wrapping ROption::RSome(table_provider).
///
/// `ffi_table_ptr` must point to a valid FFI_TableProvider. This function
/// takes ownership (reads + forgets the source).
/// `out` must point to a buffer of at least `datafusion_ffi_table_future_size()` bytes.
///
/// # Safety
/// Both pointers must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_create_table_future_some(
    ffi_table_ptr: *const c_void,
    out: *mut c_void,
) {
    let table = std::ptr::read(ffi_table_ptr as *const FFI_TableProvider);
    let result: FFIResult<ROption<FFI_TableProvider>> =
        FFIResult::ROk(ROption::RSome(table));
    let future = async move { result }.into_ffi();
    std::ptr::write(
        out as *mut FfiFuture<FFIResult<ROption<FFI_TableProvider>>>,
        future,
    );
}

/// Create a ready FfiFuture wrapping ROption::RNone (table not found).
///
/// `out` must point to a buffer of at least `datafusion_ffi_table_future_size()` bytes.
///
/// # Safety
/// `out` must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_create_table_future_none(out: *mut c_void) {
    let result: FFIResult<ROption<FFI_TableProvider>> =
        FFIResult::ROk(ROption::RNone);
    let future = async move { result }.into_ffi();
    std::ptr::write(
        out as *mut FfiFuture<FFIResult<ROption<FFI_TableProvider>>>,
        future,
    );
}

/// Create a ready FfiFuture wrapping an error message.
///
/// `ptr` must point to `len` valid UTF-8 bytes.
/// `out` must point to a buffer of at least `datafusion_ffi_table_future_size()` bytes.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_schema_create_table_future_error(
    ptr: *const u8,
    len: usize,
    out: *mut c_void,
) {
    let bytes = std::slice::from_raw_parts(ptr, len);
    let s = std::str::from_utf8_unchecked(bytes);
    let result: FFIResult<ROption<FFI_TableProvider>> =
        FFIResult::RErr(RString::from(s));
    let future = async move { result }.into_ffi();
    std::ptr::write(
        out as *mut FfiFuture<FFIResult<ROption<FFI_TableProvider>>>,
        future,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;

    #[test]
    fn test_schema_provider_size() {
        let size = datafusion_ffi_schema_provider_size();
        // FFI_SchemaProvider: ROption<RString>(40) + 5 fn ptrs + logical_codec + 5 more fields
        let codec_size = std::mem::size_of::<FFI_LogicalExtensionCodec>();
        let roption_rstring_size = std::mem::size_of::<ROption<RString>>();
        let expected = roption_rstring_size + 5 * 8 + codec_size + 5 * 8;
        assert_eq!(size, expected, "FFI_SchemaProvider size mismatch");
    }

    #[test]
    fn test_ffi_future_size() {
        // FfiFuture is 24 bytes for any T (3 pointers: fut_ptr, poll_fn, drop_fn)
        let size = datafusion_ffi_table_future_size();
        assert_eq!(size, 24, "FfiFuture should be 24 bytes");
    }

    #[test]
    fn test_table_future_none() {
        let mut out: std::mem::MaybeUninit<FfiFuture<FFIResult<ROption<FFI_TableProvider>>>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_schema_create_table_future_none(
                out.as_mut_ptr() as *mut c_void,
            );
            let future = out.assume_init();
            // Poll the future — it should be immediately ready
            let result = futures::executor::block_on(future);
            match result {
                FFIResult::ROk(opt) => assert!(opt.is_none()),
                FFIResult::RErr(e) => panic!("Expected ROk(RNone), got error: {}", e),
            }
        }
    }

    #[test]
    fn test_table_future_error() {
        let msg = "test error";
        let mut out: std::mem::MaybeUninit<FfiFuture<FFIResult<ROption<FFI_TableProvider>>>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_schema_create_table_future_error(
                msg.as_ptr(),
                msg.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let future = out.assume_init();
            let result = futures::executor::block_on(future);
            match result {
                FFIResult::RErr(e) => assert_eq!(e.as_str(), "test error"),
                FFIResult::ROk(_) => panic!("Expected RErr, got ROk"),
            }
        }
    }
}
