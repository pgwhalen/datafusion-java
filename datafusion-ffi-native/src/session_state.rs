use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionState;
use std::ffi::{c_char, c_void, CStr};
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error_return_null};

/// Bundles a SessionState with its own Tokio runtime so it can outlive the SessionContext.
struct SessionStateWithRuntime {
    state: SessionState,
    runtime: Runtime,
}

/// Create a SessionState from an existing SessionContext.
///
/// The returned pointer owns a `SessionStateWithRuntime` containing the state and a new
/// Tokio runtime. This means the SessionState can outlive the SessionContext.
///
/// # Returns
/// A pointer to SessionStateWithRuntime, or null on error.
///
/// # Safety
/// `ctx` must have been created by `datafusion_context_create`.
/// The caller must call `datafusion_session_state_destroy` to free the result.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_state(
    ctx: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if ctx.is_null() {
        return set_error_return_null(error_out, "Context is null");
    }

    let context = &*(ctx as *mut SessionContext);

    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            return set_error_return_null(
                error_out,
                &format!("Failed to create Tokio runtime: {}", e),
            )
        }
    };

    let state = context.state();

    let sw = SessionStateWithRuntime { state, runtime };
    Box::into_raw(Box::new(sw)) as *mut c_void
}

/// Destroy a SessionStateWithRuntime.
///
/// # Safety
/// The pointer must have been created by `datafusion_context_state`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_session_state_destroy(state_with_rt: *mut c_void) {
    if !state_with_rt.is_null() {
        drop(Box::from_raw(state_with_rt as *mut SessionStateWithRuntime));
    }
}

/// Create a LogicalPlan from a SQL string using the given SessionState.
///
/// # Returns
/// A pointer to a boxed LogicalPlan, or null on error.
///
/// # Safety
/// `state_with_rt` must have been created by `datafusion_context_state`.
/// `sql` must be a valid null-terminated C string.
/// The caller must call `datafusion_logical_plan_destroy` to free the result.
#[no_mangle]
pub unsafe extern "C" fn datafusion_session_state_create_logical_plan(
    state_with_rt: *mut c_void,
    sql: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if state_with_rt.is_null() {
        return set_error_return_null(error_out, "SessionState is null");
    }
    if sql.is_null() {
        return set_error_return_null(error_out, "SQL string is null");
    }

    let sw = &*(state_with_rt as *mut SessionStateWithRuntime);

    let sql_str = match CStr::from_ptr(sql).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return_null(error_out, &format!("Invalid SQL string: {}", e)),
    };

    match sw
        .runtime
        .block_on(async { sw.state.create_logical_plan(sql_str).await })
    {
        Ok(plan) => Box::into_raw(Box::new(plan)) as *mut c_void,
        Err(e) => {
            set_error_return_null(error_out, &format!("Failed to create logical plan: {}", e))
        }
    }
}
