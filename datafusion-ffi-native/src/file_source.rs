//! Rust FileSource and ExecutionPlan implementations that call back into Java.

use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource, FileStream};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_datasource::TableSchema;
use object_store::ObjectStore;
use std::any::Any;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::{check_callback_result, set_error_return};
use crate::file_opener::{JavaBackedFileOpener, JavaFileOpenerCallbacks, OpenerCallbacksHolder};

/// C-compatible callback struct for a Java-backed FileSource.
///
/// Created by `JavaFileFormatCallbacks.file_source_fn`. Owned by `JavaBackedFileSource`.
#[repr(C)]
pub struct JavaFileSourceCallbacks {
    /// Opaque pointer to the Java object.
    pub java_object: *mut c_void,

    /// Create a FileOpener, returning a JavaFileOpenerCallbacks pointer.
    ///
    /// Parameters:
    /// - java_object: opaque pointer
    /// - opener_out: receives a `*mut JavaFileOpenerCallbacks`
    /// - error_out: receives error message on failure
    ///
    /// Returns 0 on success, -1 on error.
    pub create_file_opener_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        opener_out: *mut *mut JavaFileOpenerCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this source. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

unsafe impl Send for JavaFileSourceCallbacks {}
unsafe impl Sync for JavaFileSourceCallbacks {}

/// Wraps a raw FileSource callback pointer for shared ownership.
pub(crate) struct SourceCallbacksHolder {
    pub(crate) ptr: *mut JavaFileSourceCallbacks,
}

unsafe impl Send for SourceCallbacksHolder {}
unsafe impl Sync for SourceCallbacksHolder {}

impl Drop for SourceCallbacksHolder {
    fn drop(&mut self) {
        unsafe {
            let cb = &*self.ptr;
            (cb.release_fn)(cb.java_object);
            drop(Box::from_raw(self.ptr));
        }
    }
}

/// A FileSource that delegates to Java via FFI callbacks.
#[derive(Clone)]
pub(crate) struct JavaBackedFileSource {
    pub(crate) callbacks: Arc<SourceCallbacksHolder>,
    pub(crate) table_schema: TableSchema,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: datafusion_datasource::projection::SplitProjection,
    pub(crate) metrics: Arc<ExecutionPlanMetricsSet>,
}

unsafe impl Send for JavaBackedFileSource {}
unsafe impl Sync for JavaBackedFileSource {}

impl std::fmt::Debug for JavaBackedFileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JavaBackedFileSource").finish()
    }
}

impl FileSource for JavaBackedFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Call Java to get a FileOpener
        let cb = unsafe { &*self.callbacks.ptr };
        let mut opener_out: *mut JavaFileOpenerCallbacks = std::ptr::null_mut();
        let mut error_out: *mut c_char = std::ptr::null_mut();

        let result =
            unsafe { (cb.create_file_opener_fn)(cb.java_object, &mut opener_out, &mut error_out) };
        unsafe {
            check_callback_result(result, error_out, "create FileOpener from Java FileSource")?;
        }

        if opener_out.is_null() {
            return Err(datafusion::error::DataFusionError::Execution(
                "Java FileSource.createFileOpener returned null".to_string(),
            ));
        }

        Ok(Arc::new(JavaBackedFileOpener {
            callbacks: Arc::new(OpenerCallbacksHolder { ptr: opener_out }),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(JavaBackedFileSource {
            callbacks: Arc::clone(&self.callbacks),
            table_schema: self.table_schema.clone(),
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "java"
    }

    fn try_pushdown_projection(
        &self,
        projection: &datafusion::physical_plan::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        // Accept the projection by creating a new source with the merged projection.
        // SplitProjection handles splitting file columns vs partition columns.
        use datafusion_datasource::projection::SplitProjection;
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&datafusion::physical_plan::projection::ProjectionExprs> {
        Some(&self.projection.source)
    }
}

// ============================================================================
// JavaBackedFileExec (ExecutionPlan)
// ============================================================================

/// An ExecutionPlan that scans files by delegating to a Java FileSource via FFI.
pub(crate) struct JavaBackedFileExec {
    pub(crate) base_config: FileScanConfig,
    pub(crate) file_source: Arc<dyn FileSource>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) properties: PlanProperties,
    pub(crate) projected_statistics: Statistics,
}

unsafe impl Send for JavaBackedFileExec {}
unsafe impl Sync for JavaBackedFileExec {}

impl std::fmt::Debug for JavaBackedFileExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JavaBackedFileExec").finish()
    }
}

impl DisplayAs for JavaBackedFileExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "JavaBackedFileExec")
    }
}

impl ExecutionPlan for JavaBackedFileExec {
    fn name(&self) -> &str {
        "JavaBackedFileExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let opener = self.file_source.create_file_opener(
            object_store,
            &self.base_config,
            partition,
        )?;

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// Dummy functions for initialization
unsafe extern "C" fn dummy_create_file_opener_fn(
    _java_object: *mut c_void,
    _opener_out: *mut *mut JavaFileOpenerCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "FileSource callbacks not initialized")
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut c_void) {
    // Do nothing
}

/// Allocate a JavaFileSourceCallbacks struct.
///
/// Java fills in the function pointers after allocation.
///
/// # Safety
/// The returned pointer is owned by Rust and freed when the exec plan is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_file_source_callbacks() -> *mut JavaFileSourceCallbacks {
    Box::into_raw(Box::new(JavaFileSourceCallbacks {
        java_object: std::ptr::null_mut(),
        create_file_opener_fn: dummy_create_file_opener_fn,
        release_fn: dummy_release_fn,
    }))
}
