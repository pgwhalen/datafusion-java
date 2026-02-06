use datafusion::logical_expr::LogicalPlan;
use std::ffi::c_void;

/// Destroy a LogicalPlan.
///
/// # Safety
/// The pointer must have been created by `datafusion_session_state_create_logical_plan`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_logical_plan_destroy(plan: *mut c_void) {
    if !plan.is_null() {
        drop(Box::from_raw(plan as *mut LogicalPlan));
    }
}
