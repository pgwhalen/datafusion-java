//! FFI functions for RuntimeEnv lifecycle.
//!
//! This module contains all `#[no_mangle]` entry points called by
//! `RuntimeEnvFfi.java`: runtime environment creation and destruction.

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

use crate::error::{clear_error, set_error_return_null};

/// Create a new RuntimeEnv, optionally with a memory limit.
///
/// If `max_memory` is > 0, the runtime env is created with a memory pool limited
/// to `max_memory` bytes and the given `memory_fraction`. Otherwise, a default
/// runtime env with no memory limit is created.
///
/// # Returns
/// A pointer to a `Box<Arc<RuntimeEnv>>`, or null on error.
///
/// # Safety
/// The caller must call `datafusion_runtime_env_destroy` to free the result.
#[no_mangle]
pub unsafe extern "C" fn datafusion_runtime_env_create(
    max_memory: usize,
    memory_fraction: f64,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    let builder = if max_memory > 0 {
        RuntimeEnvBuilder::new().with_memory_limit(max_memory, memory_fraction)
    } else {
        RuntimeEnvBuilder::new()
    };

    match builder.build_arc() {
        Ok(rt_env) => Box::into_raw(Box::new(rt_env)) as *mut c_void,
        Err(e) => set_error_return_null(
            error_out,
            &format!("Failed to create RuntimeEnv: {}", e),
        ),
    }
}

/// Destroy a RuntimeEnv.
///
/// # Safety
/// The pointer must have been created by `datafusion_runtime_env_create`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_runtime_env_destroy(rt_env: *mut c_void) {
    if !rt_env.is_null() {
        drop(Box::from_raw(rt_env as *mut Arc<RuntimeEnv>));
    }
}
