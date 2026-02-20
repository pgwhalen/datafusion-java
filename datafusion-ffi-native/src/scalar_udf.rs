//! FFI helpers for `ScalarUdfHandle.java`.
//!
//! This module provides size-validation helpers for the upstream `FFI_ScalarUDF`
//! struct and its related types, plus thin construction helpers for `abi_stable`
//! types that Java cannot write directly (RVec<RString>, RVec<WrappedSchema>).

use std::ffi::c_void;

use abi_stable::std_types::{RString, RVec};
use arrow::ffi::FFI_ArrowSchema;
use datafusion_ffi::arrow_wrappers::WrappedSchema;
use datafusion_ffi::udf::return_type_args::FFI_ReturnFieldArgs;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::util::FFIResult;
use datafusion_ffi::volatility::FFI_Volatility;

use datafusion_ffi::arrow_wrappers::WrappedArray;

// ============================================================================
// Size helpers — called from ScalarUdfHandle.validateSizes()
// ============================================================================

/// Return sizeof(FFI_ScalarUDF).
#[no_mangle]
pub extern "C" fn datafusion_ffi_scalar_udf_size() -> usize {
    std::mem::size_of::<FFI_ScalarUDF>()
}

/// Return sizeof(FFI_ReturnFieldArgs).
#[no_mangle]
pub extern "C" fn datafusion_ffi_return_field_args_size() -> usize {
    std::mem::size_of::<FFI_ReturnFieldArgs>()
}

/// Return sizeof(FFIResult<WrappedSchema>).
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_wrapped_schema_size() -> usize {
    std::mem::size_of::<FFIResult<WrappedSchema>>()
}

/// Return sizeof(FFIResult<WrappedArray>).
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_wrapped_array_size() -> usize {
    std::mem::size_of::<FFIResult<WrappedArray>>()
}

/// Return sizeof(WrappedArray).
#[no_mangle]
pub extern "C" fn datafusion_ffi_wrapped_array_size() -> usize {
    std::mem::size_of::<WrappedArray>()
}

/// Return sizeof(FFIResult<RVec<WrappedSchema>>).
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_rvec_wrapped_schema_size() -> usize {
    std::mem::size_of::<FFIResult<RVec<WrappedSchema>>>()
}

/// Return sizeof(RVec<RString>) for Java validation of the aliases field.
#[no_mangle]
pub extern "C" fn datafusion_ffi_rvec_rstring_size() -> usize {
    std::mem::size_of::<RVec<RString>>()
}

/// Return sizeof(FFI_Volatility) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_volatility_size() -> usize {
    std::mem::size_of::<FFI_Volatility>()
}

/// Return sizeof(RVec<WrappedSchema>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_rvec_wrapped_schema_size() -> usize {
    std::mem::size_of::<RVec<WrappedSchema>>()
}

// Note: datafusion_ffi_wrapped_schema_size() is in table_provider.rs (shared)

// ============================================================================
// abi_stable construction helpers
// ============================================================================

/// Write an empty RVec<RString> into the output buffer.
/// Used for the `aliases` field of FFI_ScalarUDF.
///
/// # Safety
/// `out` must point to a buffer of at least `size_of::<RVec<RString>>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_empty_rvec_rstring(out: *mut c_void) {
    let rvec: RVec<RString> = RVec::new();
    std::ptr::write(out as *mut RVec<RString>, rvec);
}

/// Construct an RVec<WrappedSchema> from an array of FFI_ArrowSchema structs.
/// Each schema is read from `schemas[i]` and wrapped in WrappedSchema.
///
/// # Safety
/// - `schemas` must point to `count` valid FFI_ArrowSchema structs.
/// - `out` must point to a buffer of at least `size_of::<RVec<WrappedSchema>>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_rvec_wrapped_schema(
    schemas: *const FFI_ArrowSchema,
    count: usize,
    out: *mut c_void,
) {
    let mut rvec: RVec<WrappedSchema> = RVec::with_capacity(count);
    for i in 0..count {
        let schema = std::ptr::read(schemas.add(i));
        rvec.push(WrappedSchema(schema));
    }
    std::ptr::write(out as *mut RVec<WrappedSchema>, rvec);
}

// ============================================================================
// Clone / Release — stored as function pointers in FFI_ScalarUDF
// ============================================================================

/// Clone an FFI_ScalarUDF. Since Java-backed UDFs have NULL private_data,
/// we byte-copy the struct and re-clone the name/aliases via their Clone impls.
///
/// # Safety
/// `udf` must point to a valid FFI_ScalarUDF.
#[no_mangle]
pub unsafe extern "C" fn datafusion_scalar_udf_clone(udf: &FFI_ScalarUDF) -> FFI_ScalarUDF {
    // For Java-backed UDFs, private_data is NULL, so we can't use the
    // upstream clone_fn_wrapper (which dereferences private_data).
    // Instead, we clone the abi_stable fields and copy function pointers.
    FFI_ScalarUDF {
        name: udf.name.clone(),
        aliases: udf.aliases.clone(),
        volatility: udf.volatility.clone(),
        return_field_from_args: udf.return_field_from_args,
        invoke_with_args: udf.invoke_with_args,
        short_circuits: udf.short_circuits,
        coerce_types: udf.coerce_types,
        clone: udf.clone,
        release: udf.release,
        private_data: std::ptr::null_mut(),
        library_marker_id: udf.library_marker_id,
    }
}

/// Release an FFI_ScalarUDF. For Java-backed UDFs, this is a no-op because:
/// - name/aliases are abi_stable types whose Drop handles cleanup
/// - private_data is NULL (no Rust-side heap data)
/// - Java arena manages the upcall stubs
///
/// # Safety
/// `udf` must point to a valid FFI_ScalarUDF.
#[no_mangle]
pub unsafe extern "C" fn datafusion_scalar_udf_release(_udf: &mut FFI_ScalarUDF) {
    // No-op: name and aliases are dropped by Rust's field Drop;
    // private_data is NULL for Java-backed UDFs.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_udf_size() {
        let size = datafusion_ffi_scalar_udf_size();
        assert!(size > 0, "FFI_ScalarUDF should have non-zero size");
    }

    #[test]
    fn test_return_field_args_size() {
        let size = datafusion_ffi_return_field_args_size();
        assert!(size > 0, "FFI_ReturnFieldArgs should have non-zero size");
    }

    #[test]
    fn test_result_wrapped_schema_size() {
        let size = datafusion_ffi_result_wrapped_schema_size();
        assert!(size > 0, "FFIResult<WrappedSchema> should have non-zero size");
    }

    #[test]
    fn test_result_wrapped_array_size() {
        let size = datafusion_ffi_result_wrapped_array_size();
        assert!(size > 0, "FFIResult<WrappedArray> should have non-zero size");
    }

    #[test]
    fn test_empty_rvec_rstring() {
        let mut out: std::mem::MaybeUninit<RVec<RString>> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_empty_rvec_rstring(out.as_mut_ptr() as *mut c_void);
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 0);
        }
    }

    #[test]
    fn test_rvec_wrapped_schema() {
        // Create two empty schemas
        let schemas = vec![FFI_ArrowSchema::empty(), FFI_ArrowSchema::empty()];
        let mut out: std::mem::MaybeUninit<RVec<WrappedSchema>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_rvec_wrapped_schema(
                schemas.as_ptr(),
                schemas.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 2);
        }
    }
}
