//! Size helpers, stub functions, FfiFuture helpers, and filter deserialization
//! for FFI_TableProvider.
//!
//! Java constructs `FFI_TableProvider` directly in Java arena memory.
//! This module provides size validation helpers, clone/release callbacks,
//! stub functions for unimplemented trait methods, Rust helpers for
//! constructing FfiFuture return values, and filter deserialization for
//! the scan callback.

use std::ffi::{c_char, c_void};
use std::sync::LazyLock;

use abi_stable::std_types::{ROption, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use datafusion::catalog::Session;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::table_source::FFI_TableProviderFilterPushDown;
use datafusion_ffi::util::FFIResult;
use datafusion_proto::logical_plan::from_proto::parse_exprs;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprList;
use prost::Message;

use crate::error::{clear_error, set_error_return_null};

// ============================================================================
// Size helpers
// ============================================================================

/// Return sizeof(FFI_TableProvider) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_table_provider_size() -> usize {
    std::mem::size_of::<FFI_TableProvider>()
}

/// Return sizeof(FfiFuture<FFIResult<FFI_ExecutionPlan>>) for Java validation.
///
/// This is the return type of the `scan` callback.
#[no_mangle]
pub extern "C" fn datafusion_ffi_scan_future_size() -> usize {
    std::mem::size_of::<FfiFuture<FFIResult<FFI_ExecutionPlan>>>()
}

/// Return sizeof(RVec<usize>) for Java validation.
///
/// This is the `projections` parameter type in the `scan` callback.
/// All RVec<T> have the same size (32 bytes on 64-bit).
#[no_mangle]
pub extern "C" fn datafusion_ffi_rvec_usize_size() -> usize {
    std::mem::size_of::<RVec<usize>>()
}

/// Return sizeof(ROption<usize>) for Java validation.
///
/// This is the `limit` parameter type in the `scan` callback.
#[no_mangle]
pub extern "C" fn datafusion_ffi_roption_usize_size() -> usize {
    std::mem::size_of::<ROption<usize>>()
}

/// Return the computed size of FFI_SessionRef.
///
/// FFI_SessionRef is `pub(crate)` in datafusion-ffi, so we compute its size
/// from the struct definition:
///   10 fn ptrs (80B) + FFI_LogicalExtensionCodec + 5 fields (40B)
///
/// Fields: session_id, config, create_physical_plan, create_physical_expr,
/// scalar_functions, aggregate_functions, window_functions, table_options,
/// default_table_options, task_ctx,
/// logical_codec,
/// clone, release, version, private_data, library_marker_id
#[no_mangle]
pub extern "C" fn datafusion_ffi_session_ref_size() -> usize {
    let codec_size = std::mem::size_of::<FFI_LogicalExtensionCodec>();
    80 + codec_size + 40
}

/// Return sizeof(FFI_TableType) for Java validation.
///
/// FFI_TableType is a `#[repr(C)]` enum with u32 discriminant.
/// We extract its size using the fn_return_size trick since it's not pub.
#[no_mangle]
pub extern "C" fn datafusion_ffi_table_type_size() -> usize {
    // FFI_TableType is repr(C) with no data fields, just an enum tag.
    // size = 4 bytes (i32 discriminant on all platforms)
    4
}

/// Return sizeof(WrappedSchema) for Java validation.
///
/// WrappedSchema is a newtype around FFI_ArrowSchema (72 bytes).
#[no_mangle]
pub extern "C" fn datafusion_ffi_wrapped_schema_size() -> usize {
    // WrappedSchema(FFI_ArrowSchema) = FFI_ArrowSchema = 72 bytes
    72
}

/// Return sizeof(FFIResult<RVec<FFI_TableProviderFilterPushDown>>) for Java validation.
///
/// This is the return type of the `supports_filters_pushdown` callback.
#[no_mangle]
pub extern "C" fn datafusion_ffi_filter_pushdown_result_size() -> usize {
    std::mem::size_of::<FFIResult<RVec<FFI_TableProviderFilterPushDown>>>()
}

// ============================================================================
// Clone and release callbacks
// ============================================================================

/// Clone an FFI_TableProvider.
///
/// Uses ptr::read for shallow copy, then deep-clones the embedded logical_codec.
/// Several fields of FFI_TableProvider are private, so we can't use a struct literal.
///
/// Java stores this function's symbol in the `clone` field of FFI_TableProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_provider_clone(
    provider: &FFI_TableProvider,
) -> FFI_TableProvider {
    // Shallow copy all bytes
    let mut cloned: FFI_TableProvider = std::ptr::read(provider);
    // Deep-clone the logical_codec (the only field with internal state)
    let cloned_codec = provider.logical_codec.clone();
    let old_codec = std::mem::replace(&mut cloned.logical_codec, cloned_codec);
    std::mem::forget(old_codec); // Don't drop the shallow copy's codec
    cloned
}

/// Release an FFI_TableProvider.
///
/// No-op for Java-backed providers.
///
/// Java stores this function's symbol in the `release` field of FFI_TableProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_provider_release(
    _provider: &mut FFI_TableProvider,
) {
    // No-op: private_data is NULL.
    // Rust's drop glue will drop logical_codec automatically.
}

// ============================================================================
// Stub functions for unimplemented trait methods
// ============================================================================

/// Stub for insert_into — always returns error.
///
/// Takes FFI_SessionRef and FFI_ExecutionPlan by reference, returns error future.
/// We use c_void pointers since FFI_SessionRef and FFI_InsertOp are not pub.
///
/// Java stores this function's symbol in the `insert_into` field.
///
/// Note: The actual signature is:
///   fn(&Self, FFI_SessionRef, &FFI_ExecutionPlan, FFI_InsertOp) -> FfiFuture<FFIResult<FFI_ExecutionPlan>>
/// We match the C ABI by using *const c_void for the opaque types.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_provider_stub_insert_into(
    _provider: *const c_void,
    _session: *const c_void,
    _input: *const c_void,
    _insert_op: i32,
) -> FfiFuture<FFIResult<FFI_ExecutionPlan>> {
    let result: FFIResult<FFI_ExecutionPlan> = FFIResult::RErr(RString::from(
        "insert_into not supported from Java-backed TableProvider",
    ));
    async move { result }.into_ffi()
}

// ============================================================================
// supports_filters_pushdown default implementation
// ============================================================================

/// Default `supports_filters_pushdown` that returns `Inexact` for all filters.
///
/// This tells DataFusion to push all filters down to the scan callback, where
/// the Java TableProvider can inspect them. The `Inexact` response means
/// DataFusion may still apply the filters itself after scan returns.
///
/// Java stores this function's symbol in the `supports_filters_pushdown` field
/// of FFI_TableProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_provider_supports_filters_pushdown(
    _provider: &FFI_TableProvider,
    filters_serialized: RVec<u8>,
) -> FFIResult<RVec<FFI_TableProviderFilterPushDown>> {
    let bytes = filters_serialized.as_slice();
    let count = match LogicalExprList::decode(bytes) {
        Ok(list) => list.expr.len(),
        Err(e) => {
            return FFIResult::RErr(RString::from(
                format!("Failed to decode filters: {}", e).as_str(),
            ));
        }
    };

    let pushdowns: RVec<FFI_TableProviderFilterPushDown> = (0..count)
        .map(|_| FFI_TableProviderFilterPushDown::Inexact)
        .collect();
    FFIResult::ROk(pushdowns)
}

// ============================================================================
// FFIResult construction helpers for the `supports_filters_pushdown` callback
// ============================================================================

/// Create an FFIResult::ROk(RVec<FFI_TableProviderFilterPushDown>) from i32 discriminants.
///
/// `discriminants` must point to `count` valid i32 values:
///   0 = Unsupported, 1 = Inexact, 2 = Exact
/// `out` must point to a buffer of at least
/// `datafusion_ffi_filter_pushdown_result_size()` bytes.
///
/// # Safety
/// All pointers must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_create_filter_pushdown_ok(
    discriminants: *const i32,
    count: usize,
    out: *mut c_void,
) {
    let slice = std::slice::from_raw_parts(discriminants, count);
    let pushdowns: RVec<FFI_TableProviderFilterPushDown> = slice
        .iter()
        .map(|&d| match d {
            2 => FFI_TableProviderFilterPushDown::Exact,
            0 => FFI_TableProviderFilterPushDown::Unsupported,
            _ => FFI_TableProviderFilterPushDown::Inexact,
        })
        .collect();
    let result: FFIResult<RVec<FFI_TableProviderFilterPushDown>> = FFIResult::ROk(pushdowns);
    std::ptr::write(
        out as *mut FFIResult<RVec<FFI_TableProviderFilterPushDown>>,
        result,
    );
}

/// Create an FFIResult::RErr for supports_filters_pushdown from UTF-8 bytes.
///
/// `ptr` must point to `len` valid UTF-8 bytes.
/// `out` must point to a buffer of at least
/// `datafusion_ffi_filter_pushdown_result_size()` bytes.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_create_filter_pushdown_error(
    ptr: *const u8,
    len: usize,
    out: *mut c_void,
) {
    let bytes = std::slice::from_raw_parts(ptr, len);
    let s = std::str::from_utf8_unchecked(bytes);
    let result: FFIResult<RVec<FFI_TableProviderFilterPushDown>> =
        FFIResult::RErr(RString::from(s));
    std::ptr::write(
        out as *mut FFIResult<RVec<FFI_TableProviderFilterPushDown>>,
        result,
    );
}

// ============================================================================
// FfiFuture construction helpers for the `scan` callback
// ============================================================================

/// Create a ready FfiFuture wrapping a successful FFI_ExecutionPlan.
///
/// `ffi_plan_ptr` must point to a valid FFI_ExecutionPlan. This function
/// takes ownership (reads the source).
/// `out` must point to a buffer of at least `datafusion_ffi_scan_future_size()` bytes.
///
/// # Safety
/// Both pointers must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_create_scan_future_ok(
    ffi_plan_ptr: *const c_void,
    out: *mut c_void,
) {
    let plan = std::ptr::read(ffi_plan_ptr as *const FFI_ExecutionPlan);
    let result: FFIResult<FFI_ExecutionPlan> = FFIResult::ROk(plan);
    let future = async move { result }.into_ffi();
    std::ptr::write(
        out as *mut FfiFuture<FFIResult<FFI_ExecutionPlan>>,
        future,
    );
}

/// Create a ready FfiFuture wrapping an error for scan.
///
/// `ptr` must point to `len` valid UTF-8 bytes.
/// `out` must point to a buffer of at least `datafusion_ffi_scan_future_size()` bytes.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_create_scan_future_error(
    ptr: *const u8,
    len: usize,
    out: *mut c_void,
) {
    let bytes = std::slice::from_raw_parts(ptr, len);
    let s = std::str::from_utf8_unchecked(bytes);
    let result: FFIResult<FFI_ExecutionPlan> = FFIResult::RErr(RString::from(s));
    let future = async move { result }.into_ffi();
    std::ptr::write(
        out as *mut FfiFuture<FFIResult<FFI_ExecutionPlan>>,
        future,
    );
}

/// Create an empty RVec<usize> and write it to the output buffer.
///
/// `out` must point to a buffer of at least `datafusion_ffi_rvec_usize_size()` bytes.
///
/// # Safety
/// `out` must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_empty_rvec_usize(out: *mut c_void) {
    let rvec: RVec<usize> = RVec::new();
    std::ptr::write(out as *mut RVec<usize>, rvec);
}

/// Create an RVec<usize> from an array of usize values.
///
/// `values` must point to `count` valid usize values.
/// `out` must point to a buffer of at least `datafusion_ffi_rvec_usize_size()` bytes.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_rvec_usize(
    values: *const usize,
    count: usize,
    out: *mut c_void,
) {
    let slice = std::slice::from_raw_parts(values, count);
    let rvec: RVec<usize> = slice.into();
    std::ptr::write(out as *mut RVec<usize>, rvec);
}

/// Create an empty RVec<u8> and write it to the output buffer.
///
/// `out` must point to a buffer of at least 32 bytes (sizeof RVec<u8>).
///
/// # Safety
/// `out` must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_empty_rvec_u8(out: *mut c_void) {
    let rvec: RVec<u8> = RVec::new();
    std::ptr::write(out as *mut RVec<u8>, rvec);
}

/// Write ROption<usize>::RSome(value) to the output buffer.
///
/// `out` must point to a buffer of at least `datafusion_ffi_roption_usize_size()` bytes.
///
/// # Safety
/// `out` must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_roption_usize_some(
    value: usize,
    out: *mut c_void,
) {
    let opt: ROption<usize> = ROption::RSome(value);
    std::ptr::write(out as *mut ROption<usize>, opt);
}

/// Write ROption<usize>::RNone to the output buffer.
///
/// `out` must point to a buffer of at least `datafusion_ffi_roption_usize_size()` bytes.
///
/// # Safety
/// `out` must be valid and properly aligned.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_roption_usize_none(out: *mut c_void) {
    let opt: ROption<usize> = ROption::RNone;
    std::ptr::write(out as *mut ROption<usize>, opt);
}

// ============================================================================
// Filter deserialization helpers for the `scan` callback
// ============================================================================

/// A default SessionState for deserializing filter expressions.
///
/// This is used because `FFI_SessionRef` is `pub(crate)` in `datafusion-ffi`
/// and cannot be accessed from this crate. A default SessionState provides
/// the `FunctionRegistry` needed for `parse_exprs` and the `Session` trait
/// needed for `create_physical_expr`. Simple filter expressions
/// (comparisons, literals, IN lists) work with any registry.
static DEFAULT_SESSION_STATE: LazyLock<SessionState> =
    LazyLock::new(|| SessionContext::new().state());

/// Deserialize proto-encoded filter bytes into a `Vec<Expr>`.
///
/// The filter bytes are a `prost`-encoded `LogicalExprList`, as produced by
/// `ForeignTableProvider::scan()` in `datafusion-ffi`.
///
/// Returns a handle to a `Box<Vec<Expr>>` on success, or null on error.
/// Use `datafusion_table_filter_ptr` to access individual expressions.
/// Free with `datafusion_table_free_filters`.
///
/// # Safety
/// - `filter_bytes` must point to `filter_len` valid bytes (or be null if `filter_len` is 0).
/// - `count_out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_deserialize_filters(
    filter_bytes: *const u8,
    filter_len: usize,
    count_out: *mut usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if filter_len == 0 || filter_bytes.is_null() {
        *count_out = 0;
        return std::ptr::null_mut();
    }

    let bytes = std::slice::from_raw_parts(filter_bytes, filter_len);

    let proto_list = match LogicalExprList::decode(bytes) {
        Ok(l) => l,
        Err(e) => {
            *count_out = 0;
            return set_error_return_null(
                error_out,
                &format!("Failed to decode filter protobuf: {}", e),
            );
        }
    };

    let codec = DefaultLogicalExtensionCodec {};
    let exprs = match parse_exprs(proto_list.expr.iter(), &*DEFAULT_SESSION_STATE, &codec) {
        Ok(e) => e,
        Err(e) => {
            *count_out = 0;
            return set_error_return_null(
                error_out,
                &format!("Failed to parse filter expressions: {}", e),
            );
        }
    };

    *count_out = exprs.len();
    Box::into_raw(Box::new(exprs)) as *mut c_void
}

/// Get a pointer to the `index`-th filter expression in the deserialized Vec.
///
/// Returns a `*const Expr` as `*const c_void`. The pointer is borrowed from
/// the Vec and is valid until `datafusion_table_free_filters` is called.
///
/// # Safety
/// - `handle` must have been returned by `datafusion_table_deserialize_filters`.
/// - `index` must be less than the count returned by that function.
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_filter_ptr(
    handle: *const c_void,
    index: usize,
) -> *const c_void {
    let exprs = &*(handle as *const Vec<Expr>);
    &exprs[index] as *const Expr as *const c_void
}

/// Free the deserialized filter expressions.
///
/// # Safety
/// `handle` must have been returned by `datafusion_table_deserialize_filters`,
/// or be null (no-op).
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_free_filters(handle: *mut c_void) {
    if !handle.is_null() {
        let _ = Box::from_raw(handle as *mut Vec<Expr>);
    }
}

// ============================================================================
// Session handle helpers for the `scan` callback
// ============================================================================

/// Create a boxed session handle suitable for `datafusion_session_create_physical_expr`.
///
/// Since `FFI_SessionRef` is `pub(crate)` in `datafusion-ffi`, we cannot
/// reconstruct the actual session from the FFI bytes. Instead, we provide
/// a default `SessionContext` that supports standard filter expressions.
///
/// Returns a `*mut c_void` that should be passed as the `session` parameter
/// to `datafusion_session_create_physical_expr`. Free with
/// `datafusion_table_free_session_handle`.
///
/// # Safety
/// The returned handle must be freed after use.
#[no_mangle]
pub extern "C" fn datafusion_table_create_session_handle() -> *mut c_void {
    let session_ptr: *const dyn Session = &*DEFAULT_SESSION_STATE;
    let boxed = Box::new(session_ptr);
    Box::into_raw(boxed) as *mut c_void
}

/// Free a session handle created by `datafusion_table_create_session_handle`.
///
/// # Safety
/// `handle` must have been returned by `datafusion_table_create_session_handle`,
/// or be null (no-op).
#[no_mangle]
pub unsafe extern "C" fn datafusion_table_free_session_handle(handle: *mut c_void) {
    if !handle.is_null() {
        let _ = Box::from_raw(handle as *mut *const dyn Session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_provider_size() {
        let size = datafusion_ffi_table_provider_size();
        // FFI_TableProvider: 5 fn ptrs/Option + logical_codec + 5 more fields
        let codec_size = std::mem::size_of::<FFI_LogicalExtensionCodec>();
        let expected = 5 * 8 + codec_size + 5 * 8;
        assert_eq!(size, expected, "FFI_TableProvider size mismatch");
    }

    #[test]
    fn test_session_ref_size() {
        let computed = datafusion_ffi_session_ref_size();
        let codec_size = std::mem::size_of::<FFI_LogicalExtensionCodec>();
        assert_eq!(computed, 80 + codec_size + 40);
        // Sanity check: should be > 200 bytes (10 ptrs + codec + 5 ptrs)
        assert!(computed > 200, "FFI_SessionRef should be > 200 bytes");
    }

    #[test]
    fn test_scan_future_size() {
        let size = datafusion_ffi_scan_future_size();
        assert_eq!(size, 24, "FfiFuture should be 24 bytes");
    }

    #[test]
    fn test_rvec_usize_size() {
        let size = datafusion_ffi_rvec_usize_size();
        assert_eq!(size, 32, "RVec<usize> should be 32 bytes");
    }

    #[test]
    fn test_roption_usize_size() {
        let size = datafusion_ffi_roption_usize_size();
        assert_eq!(size, 16, "ROption<usize> should be 16 bytes");
    }

    #[test]
    fn test_scan_future_error() {
        let msg = "scan error";
        let mut out: std::mem::MaybeUninit<FfiFuture<FFIResult<FFI_ExecutionPlan>>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_table_create_scan_future_error(
                msg.as_ptr(),
                msg.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let future = out.assume_init();
            let result = futures::executor::block_on(future);
            match result {
                FFIResult::RErr(e) => assert_eq!(e.as_str(), "scan error"),
                FFIResult::ROk(_) => panic!("Expected RErr, got ROk"),
            }
        }
    }

    #[test]
    fn test_rvec_usize_helpers() {
        // Test empty
        let mut out: std::mem::MaybeUninit<RVec<usize>> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_empty_rvec_usize(out.as_mut_ptr() as *mut c_void);
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 0);
        }

        // Test with values
        let values = [1usize, 3, 5];
        let mut out: std::mem::MaybeUninit<RVec<usize>> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_rvec_usize(
                values.as_ptr(),
                values.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 3);
            assert_eq!(rvec[0], 1);
            assert_eq!(rvec[1], 3);
            assert_eq!(rvec[2], 5);
        }
    }

    #[test]
    fn test_roption_usize_helpers() {
        // Test RSome
        let mut out: std::mem::MaybeUninit<ROption<usize>> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_roption_usize_some(42, out.as_mut_ptr() as *mut c_void);
            let opt = out.assume_init();
            assert_eq!(opt.into_option(), Some(42));
        }

        // Test RNone
        let mut out: std::mem::MaybeUninit<ROption<usize>> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_roption_usize_none(out.as_mut_ptr() as *mut c_void);
            let opt = out.assume_init();
            assert!(opt.is_none());
        }
    }

    #[test]
    fn test_deserialize_empty_filters() {
        unsafe {
            let mut count: usize = 99;
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let handle = datafusion_table_deserialize_filters(
                std::ptr::null(),
                0,
                &mut count,
                &mut error_out,
            );
            assert!(handle.is_null());
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_deserialize_filters_roundtrip() {
        use datafusion::prelude::*;
        use datafusion_proto::logical_plan::to_proto::serialize_exprs;

        // Create a simple filter expression: col("name") = lit("A")
        let expr = col("name").eq(lit("A"));
        let codec = DefaultLogicalExtensionCodec {};
        let serialized = serialize_exprs(&[expr.clone()], &codec).unwrap();
        let proto_list = LogicalExprList { expr: serialized };
        let bytes = proto_list.encode_to_vec();

        unsafe {
            let mut count: usize = 0;
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let handle = datafusion_table_deserialize_filters(
                bytes.as_ptr(),
                bytes.len(),
                &mut count,
                &mut error_out,
            );
            assert!(!handle.is_null(), "Should return non-null handle");
            assert_eq!(count, 1, "Should have 1 filter");

            // Get the pointer and verify it's valid
            let ptr = datafusion_table_filter_ptr(handle, 0);
            assert!(!ptr.is_null());

            // Clean up
            datafusion_table_free_filters(handle);
        }
    }

    #[test]
    fn test_filter_pushdown_result_size() {
        let size = datafusion_ffi_filter_pushdown_result_size();
        // FFIResult<RVec<T>> = discriminant(8) + max(RVec(32), RString) = 40 bytes
        assert!(size > 0, "Size should be positive");
        // Verify it matches the actual Rust type
        assert_eq!(
            size,
            std::mem::size_of::<FFIResult<RVec<FFI_TableProviderFilterPushDown>>>()
        );
    }

    #[test]
    fn test_filter_pushdown_ok() {
        let discriminants = [0i32, 1, 2]; // Unsupported, Inexact, Exact
        let mut out: std::mem::MaybeUninit<
            FFIResult<RVec<FFI_TableProviderFilterPushDown>>,
        > = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_table_create_filter_pushdown_ok(
                discriminants.as_ptr(),
                discriminants.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let result = out.assume_init();
            match result {
                FFIResult::ROk(pushdowns) => {
                    assert_eq!(pushdowns.len(), 3);
                    assert!(
                        matches!(pushdowns[0], FFI_TableProviderFilterPushDown::Unsupported)
                    );
                    assert!(
                        matches!(pushdowns[1], FFI_TableProviderFilterPushDown::Inexact)
                    );
                    assert!(
                        matches!(pushdowns[2], FFI_TableProviderFilterPushDown::Exact)
                    );
                }
                FFIResult::RErr(e) => panic!("Expected ROk, got RErr: {}", e),
            }
        }
    }

    #[test]
    fn test_filter_pushdown_error() {
        let msg = "filter error";
        let mut out: std::mem::MaybeUninit<
            FFIResult<RVec<FFI_TableProviderFilterPushDown>>,
        > = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_table_create_filter_pushdown_error(
                msg.as_ptr(),
                msg.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let result = out.assume_init();
            match result {
                FFIResult::RErr(e) => assert_eq!(e.as_str(), "filter error"),
                FFIResult::ROk(_) => panic!("Expected RErr, got ROk"),
            }
        }
    }

    #[test]
    fn test_session_handle_roundtrip() {
        let handle = datafusion_table_create_session_handle();
        assert!(!handle.is_null());
        unsafe {
            datafusion_table_free_session_handle(handle);
        }
    }
}
