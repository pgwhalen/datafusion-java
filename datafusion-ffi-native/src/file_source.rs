//! Rust FileSource and ExecutionPlan implementations that call back into Java.

use abi_stable::StableAbi;
use arrow::datatypes::SchemaRef;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource, FileStream};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_datasource::TableSchema;
use object_store::ObjectStore;
use std::any::Any;
use std::ffi::{c_void, CStr};
use datafusion::error::DataFusionError;
use datafusion_ffi::util::FFIResult;
use std::sync::Arc;

use crate::file_opener::{FFI_FileOpener, ForeignFileOpener};

/// C-compatible FFI struct for a Java-backed FileSource.
///
/// Follows the upstream `datafusion-ffi` convention. Java allocates this struct
/// in arena memory and populates all fields. Rust copies it via `ptr::read`.
///
/// Layout (56 bytes, align 8):
/// ```text
/// offset  0: create_file_opener fn ptr (ADDRESS)
/// offset  8: file_type fn ptr          (ADDRESS)
/// offset 16: clone fn ptr              (ADDRESS)
/// offset 24: release fn ptr            (ADDRESS)
/// offset 32: version fn ptr            (ADDRESS)
/// offset 40: private_data ptr          (ADDRESS)
/// offset 48: library_marker_id fn ptr  (ADDRESS)
/// ```
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_FileSource {
    /// Create a FileOpener, returning an `FFIResult<FFI_FileOpener>`.
    ///
    /// Parameters passed from Rust to Java:
    /// - `projection_ptr` / `projection_len`: file column indices (null + 0 = no projection)
    /// - `has_limit` / `limit_value`: row limit (has_limit=0 means no limit; i32 not bool per FFI rules)
    /// - `has_batch_size` / `batch_size_value`: batch size (has_batch_size=0 means default)
    /// - `partitioned_by_file_group`: whether scan is partitioned by file group (i32: 0=false, 1=true)
    /// - `partition`: partition index
    ///
    /// Returns `FFIResult::ROk(FFI_FileOpener)` on success,
    /// `FFIResult::RErr(RString)` on error.
    pub create_file_opener: unsafe extern "C" fn(
        source: &Self,
        projection_ptr: *const usize,
        projection_len: usize,
        has_limit: i32,
        limit_value: usize,
        has_batch_size: i32,
        batch_size_value: usize,
        partitioned_by_file_group: i32,
        partition: usize,
    ) -> FFIResult<FFI_FileOpener>,

    /// Return the file type identifier as a null-terminated UTF-8 string.
    /// The pointer is valid for the lifetime of the source (Java arena memory).
    pub file_type: unsafe extern "C" fn(source: &Self) -> *const u8,

    /// Clone this FFI_FileSource struct. Implemented in Rust.
    pub clone: unsafe extern "C" fn(source: &Self) -> Self,

    /// Release resources. Implemented in Rust.
    pub release: unsafe extern "C" fn(source: &mut Self),

    /// Return the FFI version. Implemented in Rust.
    pub version: unsafe extern "C" fn() -> u64,

    /// Opaque pointer to Java-side data (NULL for Java-backed providers).
    pub private_data: *mut c_void,

    /// Library marker function pointer for foreign-library detection.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_FileSource {}
unsafe impl Sync for FFI_FileSource {}

impl Clone for FFI_FileSource {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_FileSource {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// Clone an FFI_FileSource.
///
/// Copies all function pointers and private_data.
/// Java stores this function's symbol in the `clone` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_source_clone(source: &FFI_FileSource) -> FFI_FileSource {
    FFI_FileSource {
        create_file_opener: source.create_file_opener,
        file_type: source.file_type,
        clone: source.clone,
        release: source.release,
        version: source.version,
        private_data: source.private_data,
        library_marker_id: source.library_marker_id,
    }
}

/// Release an FFI_FileSource.
///
/// No-op for Java-backed sources: private_data is NULL and Java's arena
/// manages the lifecycle.
/// Java stores this function's symbol in the `release` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_source_release(_source: &mut FFI_FileSource) {
    // No-op: private_data is NULL for Java-backed sources.
}

/// A FileSource that delegates to Java via FFI callbacks.
///
/// The `ffi` field is a `Result` to support deferred error propagation: if
/// `file_source()` (which is infallible) receives an `FFIResult::RErr`, we
/// store the error here and surface it at the first fallible call site
/// (`create_file_opener`).
#[derive(Clone)]
pub(crate) struct ForeignFileSource {
    pub(crate) ffi: Result<Arc<FFI_FileSource>, String>,
    pub(crate) table_schema: TableSchema,
    pub(crate) schema: SchemaRef,
    pub(crate) projection: datafusion_datasource::projection::SplitProjection,
    /// Whether `try_pushdown_projection` has been called (projection was pushed).
    pub(crate) projection_pushed: bool,
    pub(crate) metrics: Arc<ExecutionPlanMetricsSet>,
}

unsafe impl Send for ForeignFileSource {}
unsafe impl Sync for ForeignFileSource {}

impl std::fmt::Debug for ForeignFileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForeignFileSource").finish()
    }
}

impl FileSource for ForeignFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Propagate deferred error from file_source() if any
        let cb = self.ffi.as_ref().map_err(|e| {
            DataFusionError::Execution(format!("FileSource creation failed: {}", e))
        })?;

        // Extract projection file column indices
        let file_indices = &self.projection.file_indices;
        let (proj_ptr, proj_len) = if self.projection_pushed {
            (file_indices.as_ptr(), file_indices.len())
        } else {
            (std::ptr::null(), 0usize)
        };

        // Extract limit
        let (has_limit, limit_value) = match base_config.limit {
            Some(l) => (1i32, l),
            None => (0i32, 0usize),
        };

        // Extract batch_size
        let (has_batch_size, batch_size_value) = match base_config.batch_size {
            Some(bs) => (1i32, bs),
            None => (0i32, 0usize),
        };

        // Extract partitioned_by_file_group
        let partitioned_by_file_group = if base_config.partitioned_by_file_group { 1i32 } else { 0i32 };

        // Call Java to get a FileOpener via FFIResult
        let result: FFIResult<FFI_FileOpener> = unsafe {
            (cb.create_file_opener)(cb, proj_ptr, proj_len, has_limit, limit_value, has_batch_size, batch_size_value, partitioned_by_file_group, partition)
        };
        match result {
            FFIResult::ROk(opener_ffi) => Ok(Arc::new(ForeignFileOpener {
                ffi: Arc::new(opener_ffi),
            })),
            FFIResult::RErr(e) => Err(DataFusionError::Execution(format!(
                "Failed to create FileOpener from Java FileSource: {}",
                e.as_str()
            ))),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(ForeignFileSource {
            ffi: self.ffi.as_ref().map(Arc::clone).map_err(|e| e.clone()),
            table_schema: self.table_schema.clone(),
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            projection_pushed: self.projection_pushed,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        let cb = match &self.ffi {
            Ok(cb) => cb,
            Err(_) => return "java",
        };
        let ptr = unsafe { (cb.file_type)(cb) };
        if ptr.is_null() {
            return "java";
        }
        // SAFETY: ptr points to arena-allocated memory that outlives ForeignFileSource
        unsafe { CStr::from_ptr(ptr as *const i8).to_str().unwrap_or("java") }
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
        source.projection_pushed = true;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&datafusion::physical_plan::projection::ProjectionExprs> {
        Some(&self.projection.source)
    }
}

// ============================================================================
// ForeignFileExec (ExecutionPlan)
// ============================================================================

/// An ExecutionPlan that scans files by delegating to a Java FileSource via FFI.
pub(crate) struct ForeignFileExec {
    pub(crate) base_config: FileScanConfig,
    pub(crate) file_source: Arc<dyn FileSource>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) properties: PlanProperties,
    pub(crate) projected_statistics: Statistics,
}

unsafe impl Send for ForeignFileExec {}
unsafe impl Sync for ForeignFileExec {}

impl std::fmt::Debug for ForeignFileExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForeignFileExec").finish()
    }
}

impl DisplayAs for ForeignFileExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ForeignFileExec")
    }
}

impl ExecutionPlan for ForeignFileExec {
    fn name(&self) -> &str {
        "ForeignFileExec"
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

/// Return the size of `FFI_FileSource` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_source_size() -> usize {
    std::mem::size_of::<FFI_FileSource>()
}

/// Return the size of `FFIResult<FFI_FileOpener>` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_source_result_size() -> usize {
    std::mem::size_of::<FFIResult<FFI_FileOpener>>()
}
