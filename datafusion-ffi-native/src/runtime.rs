use std::ffi::c_void;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Create a new Tokio runtime for executing async DataFusion operations.
///
/// # Returns
/// A pointer to the runtime, or null if creation failed.
///
/// # Safety
/// The caller must call `datafusion_runtime_destroy` to free the runtime.
#[no_mangle]
pub extern "C" fn datafusion_runtime_create() -> *mut c_void {
    match Runtime::new() {
        Ok(runtime) => Box::into_raw(Box::new(runtime)) as *mut c_void,
        Err(_) => std::ptr::null_mut(),
    }
}

/// Destroy a Tokio runtime.
///
/// # Safety
/// The pointer must have been created by `datafusion_runtime_create`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_runtime_destroy(rt: *mut c_void) {
    if !rt.is_null() {
        let runtime = Box::from_raw(rt as *mut Runtime);
        runtime.shutdown_timeout(Duration::from_millis(100));
    }
}
