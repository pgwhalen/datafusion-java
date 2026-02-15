use datafusion::execution::SessionState;
use std::ffi::{c_char, c_void, CStr};
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error_return_null};

/// Bundles a SessionState with its own Tokio runtime so it can outlive the SessionContext.
pub(crate) struct SessionStateWithRuntime {
    pub(crate) state: SessionState,
    pub(crate) runtime: Runtime,
}

/// Destroy a SessionStateWithRuntime.
///
/// # Safety
/// The pointer must have been created by `datafusion_context_state`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_session_state_destroy(state_with_rt: *mut c_void) {
    if !state_with_rt.is_null() {
        drop(Box::from_raw(
            state_with_rt as *mut SessionStateWithRuntime,
        ));
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
        Err(e) => {
            return set_error_return_null(error_out, &format!("Invalid SQL string: {}", e))
        }
    };

    match sw
        .runtime
        .block_on(async { sw.state.create_logical_plan(sql_str).await })
    {
        Ok(plan) => Box::into_raw(Box::new(plan)) as *mut c_void,
        Err(e) => set_error_return_null(
            error_out,
            &format!("Failed to create logical plan: {}", e),
        ),
    }
}
