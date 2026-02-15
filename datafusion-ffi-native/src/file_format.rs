//! Rust FileFormat implementation that calls back into Java.

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
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::set_error_return;
use crate::file_source::{
    JavaBackedFileExec, JavaBackedFileSource, JavaFileSourceCallbacks, SourceCallbacksHolder,
};

/// C-compatible callback struct for a Java-backed FileFormat.
///
/// Java creates upcall stubs and populates this struct. Rust stores it and
/// invokes the `file_source_fn` callback when DataFusion needs to create a
/// physical plan for scanning files.
#[repr(C)]
pub struct JavaFileFormatCallbacks {
    /// Opaque pointer to the Java object (unused, for consistency with other callback structs).
    pub java_object: *mut c_void,

    /// Create a FileSource, returning a JavaFileSourceCallbacks pointer.
    ///
    /// Parameters:
    /// - java_object: opaque pointer (same as above)
    /// - source_out: receives a `*mut JavaFileSourceCallbacks`
    /// - error_out: receives error message on failure
    ///
    /// Returns 0 on success, -1 on error.
    pub file_source_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        source_out: *mut *mut JavaFileSourceCallbacks,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this format. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

unsafe impl Send for JavaFileFormatCallbacks {}
unsafe impl Sync for JavaFileFormatCallbacks {}

/// A FileFormat that delegates to Java via FFI callbacks.
///
/// When DataFusion calls `file_source()`, this format calls `file_source_fn`
/// to get a `JavaFileSourceCallbacks`, which is used to create a `JavaBackedFileSource`.
pub(crate) struct JavaBackedFileFormat {
    pub(crate) callbacks: *mut JavaFileFormatCallbacks,
    pub(crate) schema: SchemaRef,
    pub(crate) extension: String,
}

unsafe impl Send for JavaBackedFileFormat {}
unsafe impl Sync for JavaBackedFileFormat {}

impl std::fmt::Debug for JavaBackedFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JavaBackedFileFormat")
            .field("extension", &self.extension)
            .finish()
    }
}

impl Drop for JavaBackedFileFormat {
    fn drop(&mut self) {
        unsafe {
            let cb = &*self.callbacks;
            (cb.release_fn)(cb.java_object);
            drop(Box::from_raw(self.callbacks));
        }
    }
}

#[async_trait]
impl FileFormat for JavaBackedFileFormat {
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
        // Call Java to get a FileSource
        let cb = unsafe { &*self.callbacks };
        let mut source_out: *mut JavaFileSourceCallbacks = std::ptr::null_mut();
        let mut error_out: *mut c_char = std::ptr::null_mut();

        let result =
            unsafe { (cb.file_source_fn)(cb.java_object, &mut source_out, &mut error_out) };

        // Note: file_source() cannot return an error, so we panic on failure.
        // This matches how other FileFormat impls handle infallible initialization.
        if result != 0 || source_out.is_null() {
            panic!("Java FileFormat.fileSource failed");
        }

        let projection =
            datafusion_datasource::projection::SplitProjection::unprojected(&table_schema);
        Arc::new(JavaBackedFileSource {
            callbacks: Arc::new(SourceCallbacksHolder { ptr: source_out }),
            table_schema,
            schema: Arc::clone(&self.schema),
            projection,
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
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // In DataFusion 52+, we create the plan through file_source().
        // This method is still called by ListingTable, so we delegate to our FileSource.
        let file_source = Arc::clone(conf.file_source());

        let projected_schema = conf.projected_schema()?;
        let projected_statistics = Statistics::new_unknown(&projected_schema);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(conf.file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Arc::new(JavaBackedFileExec {
            base_config: conf,
            file_source,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            projected_statistics,
        }))
    }
}

// Dummy functions for initialization
unsafe extern "C" fn dummy_file_source_fn(
    _java_object: *mut c_void,
    _source_out: *mut *mut JavaFileSourceCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "FileFormat callbacks not initialized")
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut c_void) {
    // Do nothing
}

/// Allocate a JavaFileFormatCallbacks struct.
///
/// Java fills in the function pointers after allocation.
///
/// # Safety
/// The returned pointer is owned by Rust and freed when the format is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_file_format_callbacks() -> *mut JavaFileFormatCallbacks {
    Box::into_raw(Box::new(JavaFileFormatCallbacks {
        java_object: std::ptr::null_mut(),
        file_source_fn: dummy_file_source_fn,
        release_fn: dummy_release_fn,
    }))
}
