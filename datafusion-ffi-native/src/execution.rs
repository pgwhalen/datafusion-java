//! Noop FFI_TaskContextProvider for use in Java-backed providers.
//!
//! Java-backed providers embed an FFI_LogicalExtensionCodec which needs
//! an FFI_TaskContextProvider. Since our providers don't need a real task
//! context (the codec is used for serialization, not execution), we create
//! a noop provider that returns an error if task context is requested.

use abi_stable::std_types::{RResult, RString};
use datafusion_ffi::execution::{FFI_TaskContext, FFI_TaskContextProvider};

/// Create a noop FFI_TaskContextProvider for Java-backed FFI providers.
///
/// The task_ctx function always returns an error. The clone function creates
/// a new noop provider. The release function is a no-op (no private data).
pub fn noop_task_ctx_provider() -> FFI_TaskContextProvider {
    FFI_TaskContextProvider {
        task_ctx: noop_task_ctx_fn,
        clone: noop_clone_fn,
        release: noop_release_fn,
        private_data: std::ptr::null_mut(),
        library_marker_id: crate::datafusion_java_marker_id,
    }
}

unsafe extern "C" fn noop_task_ctx_fn(
    _: &FFI_TaskContextProvider,
) -> RResult<FFI_TaskContext, RString> {
    RResult::RErr(RString::from(
        "No task context available from Java-backed provider",
    ))
}

unsafe extern "C" fn noop_clone_fn(_: &FFI_TaskContextProvider) -> FFI_TaskContextProvider {
    noop_task_ctx_provider()
}

unsafe extern "C" fn noop_release_fn(_: &mut FFI_TaskContextProvider) {
    // No-op: no private data to free
}
