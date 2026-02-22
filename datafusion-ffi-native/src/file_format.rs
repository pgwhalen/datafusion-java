//! Rust FileFormat implementation that calls back into Java.

use abi_stable::StableAbi;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Result, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use datafusion_datasource::TableSchema;
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use datafusion_ffi::util::FFIResult;
use std::ffi::c_void;
use std::sync::Arc;

use crate::file_source::{FFI_FileSource, ForeignFileExec, ForeignFileSource};

/// C-compatible FFI struct for a Java-backed FileFormat.
///
/// Follows the upstream `datafusion-ffi` convention. Java allocates this struct
/// in arena memory and populates all fields. Rust copies it via `ptr::read`.
///
/// Layout (48 bytes, align 8):
/// ```text
/// offset  0: file_source fn ptr        (ADDRESS)
/// offset  8: clone fn ptr              (ADDRESS)
/// offset 16: release fn ptr            (ADDRESS)
/// offset 24: version fn ptr            (ADDRESS)
/// offset 32: private_data ptr          (ADDRESS)
/// offset 40: library_marker_id fn ptr  (ADDRESS)
/// ```
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_FileFormat {
    /// Create a FileSource, returning an `FFIResult<FFI_FileSource>`.
    ///
    /// Returns `FFIResult::ROk(FFI_FileSource)` on success,
    /// `FFIResult::RErr(RString)` on error.
    pub file_source: unsafe extern "C" fn(format: &Self) -> FFIResult<FFI_FileSource>,

    /// Clone this FFI_FileFormat struct. Implemented in Rust.
    pub clone: unsafe extern "C" fn(format: &Self) -> Self,

    /// Release resources. Implemented in Rust.
    pub release: unsafe extern "C" fn(format: &mut Self),

    /// Return the FFI version. Implemented in Rust.
    pub version: unsafe extern "C" fn() -> u64,

    /// Opaque pointer to Java-side data (NULL for Java-backed providers).
    pub private_data: *mut c_void,

    /// Library marker function pointer for foreign-library detection.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_FileFormat {}
unsafe impl Sync for FFI_FileFormat {}

impl Clone for FFI_FileFormat {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_FileFormat {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// Clone an FFI_FileFormat.
///
/// Copies all function pointers and private_data.
/// Java stores this function's symbol in the `clone` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_format_clone(format: &FFI_FileFormat) -> FFI_FileFormat {
    FFI_FileFormat {
        file_source: format.file_source,
        clone: format.clone,
        release: format.release,
        version: format.version,
        private_data: format.private_data,
        library_marker_id: format.library_marker_id,
    }
}

/// Release an FFI_FileFormat.
///
/// No-op for Java-backed formats: private_data is NULL and Java's arena
/// manages the lifecycle.
/// Java stores this function's symbol in the `release` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_format_release(_format: &mut FFI_FileFormat) {
    // No-op: private_data is NULL for Java-backed formats.
}

/// A FileFormat that delegates to Java via FFI callbacks.
///
/// When DataFusion calls `file_source()`, this format calls `file_source`
/// to get an `FFI_FileSource`, which is used to create a `ForeignFileSource`.
///
/// Note: No manual `Drop` impl — the `FFI_FileFormat` field has its own `Drop`
/// that calls the release callback.
pub(crate) struct ForeignFileFormat {
    pub(crate) ffi: FFI_FileFormat,
    pub(crate) schema: SchemaRef,
    pub(crate) extension: String,
}

unsafe impl Send for ForeignFileFormat {}
unsafe impl Sync for ForeignFileFormat {}

impl std::fmt::Debug for ForeignFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForeignFileFormat")
            .field("extension", &self.extension)
            .finish()
    }
}

#[async_trait]
impl FileFormat for ForeignFileFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.extension.clone()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(format!(
            "{}{}",
            self.get_ext(),
            file_compression_type.get_ext()
        ))
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        // No compression support for Java-backed formats
        None
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        // Call Java to get a FileSource via FFIResult
        let result: FFIResult<FFI_FileSource> = unsafe { (self.ffi.file_source)(&self.ffi) };

        // file_source() is infallible, so defer the error to the first fallible
        // method (create_file_opener) via ForeignFileSource.ffi: Result<Arc<FFI_FileSource>, String>.
        let ffi = match result {
            FFIResult::ROk(s) => Ok(Arc::new(s)),
            FFIResult::RErr(e) => Err(e.to_string()),
        };

        let projection =
            datafusion_datasource::projection::SplitProjection::unprojected(&table_schema);
        Arc::new(ForeignFileSource {
            ffi,
            table_schema,
            schema: Arc::clone(&self.schema),
            projection,
            projection_pushed: false,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        })
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // Schema is provided by Java at registration time
        Ok(Arc::clone(&self.schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // In DataFusion 52+, we create the plan through file_source().
        // This method is still called by ListingTable, so we delegate to our FileSource.

        // Resolve batch_size from session config when not explicitly set,
        // so Java callbacks can observe the configured value.
        if conf.batch_size.is_none() {
            conf.batch_size = Some(state.config().batch_size());
        }

        let file_source = Arc::clone(conf.file_source());

        let projected_schema = conf.projected_schema()?;
        let projected_statistics = Statistics::new_unknown(&projected_schema);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(conf.file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(ForeignFileExec {
            base_config: conf,
            file_source,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            projected_statistics,
        }))
    }
}

/// Return the size of `FFI_FileFormat` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_format_size() -> usize {
    std::mem::size_of::<FFI_FileFormat>()
}

/// Return the size of `FFIResult<FFI_FileSource>` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_format_result_size() -> usize {
    std::mem::size_of::<FFIResult<FFI_FileSource>>()
}
