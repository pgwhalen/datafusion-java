//! Rust TableProvider implementation that calls back into Java.

use crate::error::{check_callback_result, set_error_return};
use crate::java_provider::JavaTableProviderCallbacks;
use arrow::datatypes::SchemaRef;
use arrow::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use std::any::Any;
use std::ffi::{c_char, c_void};
use std::mem::MaybeUninit;
use std::sync::Arc;

/// A TableProvider that calls back into Java.
pub struct JavaBackedTableProvider {
    callbacks: *mut JavaTableProviderCallbacks,
    schema: SchemaRef,
}

impl std::fmt::Debug for JavaBackedTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JavaBackedTableProvider")
    }
}

impl JavaBackedTableProvider {
    /// Create a new JavaBackedTableProvider from callback pointers.
    ///
    /// # Safety
    /// The callbacks pointer must be valid and point to a properly initialized struct.
    pub unsafe fn new(callbacks: *mut JavaTableProviderCallbacks) -> Result<Self> {
        let cb = &*callbacks;

        // Get schema from Java
        let mut schema_out: FFI_ArrowSchema = std::mem::zeroed();
        let mut error_out: *mut c_char = std::ptr::null_mut();

        let result = (cb.schema_fn)(cb.java_object, &mut schema_out, &mut error_out);
        check_callback_result(result, error_out, "get schema from Java TableProvider")?;

        // Convert FFI schema to Arrow schema
        let schema = match arrow::datatypes::Schema::try_from(&schema_out) {
            Ok(s) => Arc::new(s),
            Err(e) => return Err(datafusion::error::DataFusionError::ArrowError(Box::new(e), None)),
        };

        Ok(Self { callbacks, schema })
    }
}

unsafe impl Send for JavaBackedTableProvider {}
unsafe impl Sync for JavaBackedTableProvider {}

#[async_trait]
impl TableProvider for JavaBackedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        unsafe {
            let cb = &*self.callbacks;
            let type_val = (cb.table_type_fn)(cb.java_object);
            match type_val {
                0 => TableType::Base,
                1 => TableType::View,
                2 => TableType::Temporary,
                _ => TableType::Base,
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Report all filters as Inexact: DataFusion passes them to scan() but still applies them
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unsafe {
            let cb = &*self.callbacks;

            // Box the session fat pointer so it can be passed as *mut c_void.
            // Safety: The boxed pointer only lives for the duration of this scan() call.
            // The session reference is only used within the Java callback, which returns
            // before we reclaim the box. We transmute to erase the borrow lifetime.
            let session_ptr: *const dyn Session =
                std::mem::transmute::<*const dyn Session, *const dyn Session>(state);
            let session_box = Box::new(session_ptr);
            let session_handle = Box::into_raw(session_box) as *mut c_void;

            // Create filter pointer array (pointers into the borrowed filters slice)
            let filter_ptrs: Vec<*const c_void> = filters
                .iter()
                .map(|e| e as *const Expr as *const c_void)
                .collect();

            // Convert projection to C array
            let (projection_ptr, projection_len) = match projection {
                Some(p) => (p.as_ptr(), p.len()),
                None => (std::ptr::null(), 0),
            };

            // Convert limit to i64 (-1 for no limit)
            let limit_val = match limit {
                Some(l) => l as i64,
                None => -1,
            };

            // Allocate FFI_ExecutionPlan on the stack for Java to write into
            let mut ffi_plan = MaybeUninit::<FFI_ExecutionPlan>::uninit();
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (cb.scan_fn)(
                cb.java_object,
                session_handle,
                filter_ptrs.as_ptr() as *const *const c_void,
                filter_ptrs.len(),
                projection_ptr,
                projection_len,
                limit_val,
                ffi_plan.as_mut_ptr(),
                &mut error_out,
            );

            // Reclaim and drop the boxed session pointer
            let _ = Box::from_raw(session_handle as *mut *const dyn Session);

            check_callback_result(result, error_out, "scan Java TableProvider")?;

            // Convert FFI_ExecutionPlan to ForeignExecutionPlan via TryFrom
            let ffi_plan = ffi_plan.assume_init();
            let plan: Arc<dyn ExecutionPlan> = (&ffi_plan).try_into().map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to convert FFI_ExecutionPlan: {}",
                    e
                ))
            })?;

            Ok(plan)
        }
    }
}

impl Drop for JavaBackedTableProvider {
    fn drop(&mut self) {
        unsafe {
            let callbacks = &*self.callbacks;
            (callbacks.release_fn)(callbacks.java_object);
            // Free the callbacks struct itself
            drop(Box::from_raw(self.callbacks));
        }
    }
}

/// Allocate a JavaTableProviderCallbacks struct.
///
/// # Safety
/// The returned pointer must be freed by Rust when the provider is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_table_provider_callbacks() -> *mut JavaTableProviderCallbacks {
    Box::into_raw(Box::new(JavaTableProviderCallbacks {
        java_object: std::ptr::null_mut(),
        schema_fn: dummy_schema_fn,
        table_type_fn: dummy_table_type_fn,
        scan_fn: dummy_scan_fn,
        release_fn: dummy_release_fn,
    }))
}

// Dummy functions for initialization - Java will set the actual function pointers
unsafe extern "C" fn dummy_schema_fn(
    _java_object: *mut std::ffi::c_void,
    _schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "TableProvider callbacks not initialized")
}

unsafe extern "C" fn dummy_table_type_fn(_java_object: *mut std::ffi::c_void) -> i32 {
    0 // BASE
}

unsafe extern "C" fn dummy_scan_fn(
    _java_object: *mut std::ffi::c_void,
    _session: *mut std::ffi::c_void,
    _filter_ptrs: *const *const std::ffi::c_void,
    _filter_count: usize,
    _projection: *const usize,
    _projection_len: usize,
    _limit: i64,
    _plan_out: *mut FFI_ExecutionPlan,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "TableProvider callbacks not initialized")
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut std::ffi::c_void) {
    // Do nothing
}
