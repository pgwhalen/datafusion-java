//! Rust FFI module for Java-backed listing tables.
//!
//! This module provides a 3-level callback chain mirroring DataFusion's trait hierarchy:
//! - `JavaFileFormatCallbacks` / `JavaBackedFileFormat`: Top-level format, creates file sources
//! - `JavaFileSourceCallbacks` / `JavaBackedFileSource`: File source, creates file openers
//! - `JavaFileOpenerCallbacks` / `JavaBackedFileOpener`: Opens individual files via Java
//!
//! The chain is: FileFormat → FileSource → FileOpener → RecordBatchReader

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Result, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource, FileStream};
use datafusion::execution::context::SessionContext;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_datasource::TableSchema;
use futures::stream::BoxStream;
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::error::{check_callback_result, clear_error, set_error_return};
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;

// ============================================================================
// Callback structs
// ============================================================================

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

/// C-compatible callback struct for a Java-backed FileOpener.
///
/// Created by `JavaFileSourceCallbacks.create_file_opener_fn`. Owned by `JavaBackedFileOpener`.
#[repr(C)]
pub struct JavaFileOpenerCallbacks {
    /// Opaque pointer to the Java object.
    pub java_object: *mut c_void,

    /// Open a file, writing an FFI_RecordBatchStream into the output.
    ///
    /// Parameters:
    /// - java_object: opaque pointer
    /// - file_path: pointer to UTF-8 file path bytes
    /// - file_path_len: length of file path bytes
    /// - file_size: file size in bytes
    /// - has_range: 1 if byte range present, 0 if not
    /// - range_start: start of byte range (0 if no range)
    /// - range_end: end of byte range (0 if no range)
    /// - stream_out: receives an FFI_RecordBatchStream struct (32 bytes)
    /// - error_out: receives error message on failure
    ///
    /// Returns 0 on success, -1 on error.
    pub open_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        file_path: *const u8,
        file_path_len: usize,
        file_size: u64,
        has_range: i32,
        range_start: i64,
        range_end: i64,
        stream_out: *mut FFI_RecordBatchStream,
        error_out: *mut *mut c_char,
    ) -> i32,

    /// Called when Rust is done with this opener. Java should release resources.
    pub release_fn: unsafe extern "C" fn(java_object: *mut c_void),
}

// Raw pointers require manual Send/Sync. Safe because Java callbacks use shared arenas.
unsafe impl Send for JavaFileFormatCallbacks {}
unsafe impl Sync for JavaFileFormatCallbacks {}
unsafe impl Send for JavaFileSourceCallbacks {}
unsafe impl Sync for JavaFileSourceCallbacks {}
unsafe impl Send for JavaFileOpenerCallbacks {}
unsafe impl Sync for JavaFileOpenerCallbacks {}

// ============================================================================
// JavaBackedFileFormat
// ============================================================================

/// A FileFormat that delegates to Java via FFI callbacks.
///
/// When DataFusion calls `file_source()`, this format calls `file_source_fn`
/// to get a `JavaFileSourceCallbacks`, which is used to create a `JavaBackedFileSource`.
struct JavaBackedFileFormat {
    callbacks: *mut JavaFileFormatCallbacks,
    schema: SchemaRef,
    extension: String,
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

// ============================================================================
// JavaBackedFileSource (FileSource trait)
// ============================================================================

/// Wraps a raw FileSource callback pointer for shared ownership.
struct SourceCallbacksHolder {
    ptr: *mut JavaFileSourceCallbacks,
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
struct JavaBackedFileSource {
    callbacks: Arc<SourceCallbacksHolder>,
    table_schema: TableSchema,
    schema: SchemaRef,
    projection: datafusion_datasource::projection::SplitProjection,
    metrics: Arc<ExecutionPlanMetricsSet>,
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
struct JavaBackedFileExec {
    base_config: FileScanConfig,
    file_source: Arc<dyn FileSource>,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
    projected_statistics: Statistics,
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

        let opener =
            self.file_source
                .create_file_opener(object_store, &self.base_config, partition)?;

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

// ============================================================================
// JavaBackedFileOpener
// ============================================================================

/// Wraps a raw opener callback pointer for shared ownership.
///
/// This allows the callback pointer to be shared across threads (via Arc)
/// and into async blocks that require `Send`.
struct OpenerCallbacksHolder {
    ptr: *mut JavaFileOpenerCallbacks,
}

unsafe impl Send for OpenerCallbacksHolder {}
unsafe impl Sync for OpenerCallbacksHolder {}

impl Drop for OpenerCallbacksHolder {
    fn drop(&mut self) {
        unsafe {
            let cb = &*self.ptr;
            (cb.release_fn)(cb.java_object);
            drop(Box::from_raw(self.ptr));
        }
    }
}

/// A FileOpener that passes file paths to Java for reading.
///
/// Owns a `JavaFileOpenerCallbacks` pointer (via `OpenerCallbacksHolder`).
/// When `open()` is called, it extracts the file path from the metadata and calls
/// `open_fn`, which writes an `FFI_RecordBatchStream` directly from Java.
struct JavaBackedFileOpener {
    callbacks: Arc<OpenerCallbacksHolder>,
}

impl Unpin for JavaBackedFileOpener {}

impl FileOpener for JavaBackedFileOpener {
    fn open(
        &self,
        partitioned_file: PartitionedFile,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            BoxStream<
                                'static,
                                Result<
                                    arrow::record_batch::RecordBatch,
                                    datafusion::error::DataFusionError,
                                >,
                            >,
                        >,
                    > + Send,
            >,
        >,
    > {
        let callbacks = Arc::clone(&self.callbacks);

        Ok(Box::pin(async move {
            // 1. Extract fields from PartitionedFile
            let path = format!("/{}", partitioned_file.path().as_ref());
            let path_bytes = path.as_bytes();
            let file_size = partitioned_file.object_meta.size as u64;
            let (has_range, range_start, range_end) = match &partitioned_file.range {
                Some(range) => (1i32, range.start as i64, range.end as i64),
                None => (0i32, 0i64, 0i64),
            };

            // 2. Call Java callback to open file — Java writes FFI_RecordBatchStream directly
            let cb = unsafe { &*callbacks.ptr };
            let mut stream_out =
                std::mem::MaybeUninit::<FFI_RecordBatchStream>::uninit();
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let result = unsafe {
                (cb.open_fn)(
                    cb.java_object,
                    path_bytes.as_ptr(),
                    path_bytes.len(),
                    file_size,
                    has_range,
                    range_start,
                    range_end,
                    stream_out.as_mut_ptr(),
                    &mut error_out,
                )
            };
            unsafe {
                check_callback_result(result, error_out, "open file from Java FileOpener")?;
            }

            // 3. stream_out is an FFI_RecordBatchStream (implements Stream<Item = Result<RecordBatch>>)
            let stream = unsafe { stream_out.assume_init() };
            Ok(Box::pin(stream)
                as BoxStream<
                    'static,
                    Result<arrow::record_batch::RecordBatch, datafusion::error::DataFusionError>,
                >)
        }))
    }
}

// ============================================================================
// FFI Functions
// ============================================================================

// Dummy functions for initialization
unsafe extern "C" fn dummy_file_source_fn(
    _java_object: *mut c_void,
    _source_out: *mut *mut JavaFileSourceCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "FileFormat callbacks not initialized")
}

unsafe extern "C" fn dummy_create_file_opener_fn(
    _java_object: *mut c_void,
    _opener_out: *mut *mut JavaFileOpenerCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "FileSource callbacks not initialized")
}

unsafe extern "C" fn dummy_open_fn(
    _java_object: *mut c_void,
    _file_path: *const u8,
    _file_path_len: usize,
    _file_size: u64,
    _has_range: i32,
    _range_start: i64,
    _range_end: i64,
    _stream_out: *mut FFI_RecordBatchStream,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "FileOpener callbacks not initialized")
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

/// Allocate a JavaFileOpenerCallbacks struct.
///
/// Java fills in the function pointers after allocation.
///
/// # Safety
/// The returned pointer is owned by Rust and freed when the opener is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_file_opener_callbacks() -> *mut JavaFileOpenerCallbacks {
    Box::into_raw(Box::new(JavaFileOpenerCallbacks {
        java_object: std::ptr::null_mut(),
        open_fn: dummy_open_fn,
        release_fn: dummy_release_fn,
    }))
}

/// Create a listing table from config and register it with the session context.
///
/// Takes ownership of `format_callbacks`. All other pointer args are borrowed.
///
/// # Arguments
/// * `ctx` - Pointer to SessionContext
/// * `rt` - Pointer to Tokio Runtime
/// * `name` - Table name (null-terminated C string)
/// * `urls` - Array of directory path pointers (null-terminated C strings)
/// * `urls_len` - Number of URLs in the array
/// * `file_extension` - File extension (null-terminated C string, e.g. ".tsv")
/// * `schema` - Pointer to FFI_ArrowSchema (borrowed, Java owns release)
/// * `format_callbacks` - Pointer to JavaFileFormatCallbacks (takes ownership)
/// * `collect_stat` - Whether to collect file statistics
/// * `target_partitions` - Target number of partitions
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_listing_table(
    ctx: *mut c_void,
    rt: *mut c_void,
    name: *const c_char,
    urls: *const *const c_char,
    urls_len: usize,
    file_extension: *const c_char,
    schema: *mut FFI_ArrowSchema,
    format_callbacks: *mut JavaFileFormatCallbacks,
    collect_stat: i32,
    target_partitions: usize,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null()
        || rt.is_null()
        || name.is_null()
        || urls.is_null()
        || urls_len == 0
        || file_extension.is_null()
        || schema.is_null()
        || format_callbacks.is_null()
    {
        return set_error_return(error_out, "Null pointer argument or empty URLs");
    }

    let context = &*(ctx as *mut SessionContext);
    let runtime = &*(rt as *mut Runtime);

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid table name: {}", e)),
    };

    let ext_str = match CStr::from_ptr(file_extension).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid file extension: {}", e)),
    };

    // Import schema from FFI. We read the FFI_ArrowSchema into an owned value so that
    // its Drop impl will call the release callback, freeing Java-allocated buffers.
    let ffi_schema = std::ptr::read(schema);
    let arrow_schema = match Schema::try_from(&ffi_schema) {
        Ok(s) => Arc::new(s),
        Err(e) => return set_error_return(error_out, &format!("Failed to import schema: {}", e)),
    };
    // ffi_schema is dropped here, calling the release callback

    // Create the JavaBackedFileFormat (takes ownership of callbacks)
    let format = Arc::new(JavaBackedFileFormat {
        callbacks: format_callbacks,
        schema: Arc::clone(&arrow_schema),
        extension: ext_str.clone(),
    });

    // Build ListingOptions
    let options = ListingOptions::new(format as Arc<dyn FileFormat>)
        .with_file_extension(ext_str)
        .with_collect_stat(collect_stat != 0)
        .with_target_partitions(target_partitions);

    // Parse URLs and create table config
    runtime.block_on(async {
        let urls_slice = std::slice::from_raw_parts(urls, urls_len);
        let mut table_urls = Vec::with_capacity(urls_len);
        for &url_ptr in urls_slice {
            let url_str = match CStr::from_ptr(url_ptr).to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return set_error_return(error_out, &format!("Invalid URL: {}", e)),
            };
            let table_url = match ListingTableUrl::parse(&url_str) {
                Ok(u) => u,
                Err(e) => {
                    return set_error_return(error_out, &format!("Failed to parse URL: {}", e))
                }
            };
            table_urls.push(table_url);
        }

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(options)
            .with_schema(arrow_schema);

        let table = match ListingTable::try_new(config) {
            Ok(t) => t,
            Err(e) => {
                return set_error_return(
                    error_out,
                    &format!("Failed to create listing table: {}", e),
                )
            }
        };

        if let Err(e) = context.register_table(&name_str, Arc::new(table)) {
            return set_error_return(error_out, &format!("Failed to register table: {}", e));
        }

        0
    })
}
