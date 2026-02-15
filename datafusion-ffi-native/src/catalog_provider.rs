//! Size helpers and stub functions for FFI_CatalogProvider.
//!
//! Java constructs `FFI_CatalogProvider` directly in Java arena memory.
//! This module provides size validation helpers, clone/release callbacks,
//! and stub functions for unimplemented trait methods.

use abi_stable::std_types::{ROption, RString};
use datafusion_ffi::catalog_provider::FFI_CatalogProvider;
use datafusion_ffi::schema_provider::FFI_SchemaProvider;
use datafusion_ffi::util::FFIResult;

// ============================================================================
// Size helpers
// ============================================================================

/// Return sizeof(FFI_CatalogProvider) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_catalog_provider_size() -> usize {
    std::mem::size_of::<FFI_CatalogProvider>()
}

/// Return sizeof(ROption<FFI_SchemaProvider>) for Java validation.
///
/// This is the return type of the `schema` callback.
#[no_mangle]
pub extern "C" fn datafusion_ffi_roption_schema_provider_size() -> usize {
    std::mem::size_of::<ROption<FFI_SchemaProvider>>()
}

/// Return sizeof(FFIResult<ROption<FFI_SchemaProvider>>) for Java validation.
///
/// This is the return type of register_schema/deregister_schema stubs.
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_roption_schema_provider_size() -> usize {
    std::mem::size_of::<FFIResult<ROption<FFI_SchemaProvider>>>()
}

// ============================================================================
// Clone and release callbacks
// ============================================================================

/// Clone an FFI_CatalogProvider.
///
/// Copies all function pointers, deep-clones the embedded logical_codec,
/// and copies private_data (which is NULL for Java-backed providers).
///
/// Java stores this function's symbol in the `clone` field of FFI_CatalogProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_catalog_provider_clone(
    provider: &FFI_CatalogProvider,
) -> FFI_CatalogProvider {
    FFI_CatalogProvider {
        schema_names: provider.schema_names,
        schema: provider.schema,
        register_schema: provider.register_schema,
        deregister_schema: provider.deregister_schema,
        logical_codec: provider.logical_codec.clone(),
        clone: provider.clone,
        release: provider.release,
        version: provider.version,
        private_data: provider.private_data,
        library_marker_id: provider.library_marker_id,
    }
}

/// Release an FFI_CatalogProvider.
///
/// No-op for Java-backed providers: private_data is NULL, and Rust's drop
/// glue handles the embedded logical_codec after this returns.
///
/// Java stores this function's symbol in the `release` field of FFI_CatalogProvider.
#[no_mangle]
pub unsafe extern "C" fn datafusion_catalog_provider_release(
    _provider: &mut FFI_CatalogProvider,
) {
    // No-op: private_data is NULL.
    // Rust's drop glue will drop logical_codec automatically after this returns.
}

// ============================================================================
// Stub functions for unimplemented trait methods
// ============================================================================

/// Stub for register_schema — always returns error.
///
/// Java stores this function's symbol in the `register_schema` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_catalog_provider_stub_register_schema(
    _provider: &FFI_CatalogProvider,
    _name: RString,
    _schema: &FFI_SchemaProvider,
) -> FFIResult<ROption<FFI_SchemaProvider>> {
    FFIResult::RErr(RString::from(
        "register_schema not supported from Java-backed CatalogProvider",
    ))
}

/// Stub for deregister_schema — always returns error.
///
/// Java stores this function's symbol in the `deregister_schema` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_catalog_provider_stub_deregister_schema(
    _provider: &FFI_CatalogProvider,
    _name: RString,
    _cascade: bool,
) -> FFIResult<ROption<FFI_SchemaProvider>> {
    FFIResult::RErr(RString::from(
        "deregister_schema not supported from Java-backed CatalogProvider",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;

    #[test]
    fn test_catalog_provider_size() {
        let size = datafusion_ffi_catalog_provider_size();
        // FFI_CatalogProvider: 4 fn ptrs + logical_codec + 5 more fields
        let codec_size = std::mem::size_of::<FFI_LogicalExtensionCodec>();
        let expected = 4 * 8 + codec_size + 5 * 8;
        assert_eq!(size, expected, "FFI_CatalogProvider size mismatch");
    }

    #[test]
    fn test_roption_schema_provider_size() {
        let size = datafusion_ffi_roption_schema_provider_size();
        // ROption<FFI_SchemaProvider>: disc(u8) + pad(7) + FFI_SchemaProvider
        let sp_size = std::mem::size_of::<FFI_SchemaProvider>();
        let expected = 8 + sp_size;
        assert_eq!(size, expected, "ROption<FFI_SchemaProvider> size mismatch");
    }
}
