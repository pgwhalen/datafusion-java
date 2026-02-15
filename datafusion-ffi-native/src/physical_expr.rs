use datafusion::physical_plan::PhysicalExpr;
use std::ffi::c_void;
use std::sync::Arc;

/// Destroys a physical expression created by `datafusion_session_create_physical_expr`.
///
/// # Safety
/// The pointer must have been created by `datafusion_session_create_physical_expr`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_physical_expr_destroy(expr: *mut c_void) {
    if !expr.is_null() {
        drop(Box::from_raw(expr as *mut Arc<dyn PhysicalExpr>));
    }
}
