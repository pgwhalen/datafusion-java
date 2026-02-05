//! Rust FFI module for Java-backed listing tables.
//!
//! This module provides a 3-level callback chain mirroring DataFusion's trait hierarchy:
//! - `JavaFileFormatCallbacks` / `JavaBackedFileFormat`: Top-level format, creates file sources
//! - `JavaFileSourceCallbacks` / `JavaBackedFileExec`: Execution plan, creates file openers
//! - `JavaFileOpenerCallbacks` / `JavaBackedFileOpener`: Opens individual files via Java
//!
//! The chain is: FileFormat → FileSource → FileOpener → RecordBatchReader

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion::common::{GetExt, Result, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore};
use std::any::Any;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::error::{check_callback_result, clear_error, set_error_return};
use crate::java_backed_plan::JavaBackedRecordBatchStream;
use crate::java_provider::JavaRecordBatchReaderCallbacks;

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
/// Created by `JavaFileFormatCallbacks.file_source_fn`. Owned by `JavaBackedFileExec`.
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

    /// Open file content, returning a RecordBatchReaderCallbacks pointer.
    ///
    /// Parameters:
    /// - java_object: opaque pointer
    /// - file_content: pointer to file bytes
    /// - file_content_len: length of file bytes
    /// - reader_out: receives a `*mut JavaRecordBatchReaderCallbacks`
    /// - error_out: receives error message on failure
    ///
    /// Returns 0 on success, -1 on error.
    pub open_fn: unsafe extern "C" fn(
        java_object: *mut c_void,
        file_content: *const u8,
        file_content_len: usize,
        reader_out: *mut *mut JavaRecordBatchReaderCallbacks,
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
/// When DataFusion calls `create_physical_plan()`, this format calls `file_source_fn`
/// to get a `JavaFileSourceCallbacks`, which is used to create a `JavaBackedFileExec`.
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

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // Schema is provided by Java at registration time
        Ok(Arc::clone(&self.schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Call Java to get a FileSource
        let cb = unsafe { &*self.callbacks };
        let mut source_out: *mut JavaFileSourceCallbacks = std::ptr::null_mut();
        let mut error_out: *mut c_char = std::ptr::null_mut();

        let result =
            unsafe { (cb.file_source_fn)(cb.java_object, &mut source_out, &mut error_out) };
        unsafe {
            check_callback_result(result, error_out, "get FileSource from Java FileFormat")?;
        }

        if source_out.is_null() {
            return Err(datafusion::error::DataFusionError::Execution(
                "Java FileFormat.fileSource returned null".to_string(),
            ));
        }

        Ok(Arc::new(JavaBackedFileExec::new(
            conf,
            source_out,
            Arc::clone(&self.schema),
        )))
    }
}

// ============================================================================
// JavaBackedFileExec (ExecutionPlan)
// ============================================================================

/// An ExecutionPlan that scans files by delegating to a Java FileSource via FFI.
///
/// Owns a `JavaFileSourceCallbacks` pointer. When `execute()` is called, it calls
/// `create_file_opener_fn` to get a `JavaFileOpenerCallbacks`, which is used to
/// create a `JavaBackedFileOpener` for the `FileStream`.
struct JavaBackedFileExec {
    base_config: FileScanConfig,
    source_callbacks: *mut JavaFileSourceCallbacks,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
    projected_statistics: Statistics,
}

unsafe impl Send for JavaBackedFileExec {}
unsafe impl Sync for JavaBackedFileExec {}

impl JavaBackedFileExec {
    fn new(
        base_config: FileScanConfig,
        source_callbacks: *mut JavaFileSourceCallbacks,
        schema: SchemaRef,
    ) -> Self {
        let (projected_schema, ..) = base_config.project();
        let projected_statistics = Statistics::new_unknown(&projected_schema);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(base_config.file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            base_config,
            source_callbacks,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            projected_statistics,
        }
    }
}

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

impl Drop for JavaBackedFileExec {
    fn drop(&mut self) {
        unsafe {
            let cb = &*self.source_callbacks;
            (cb.release_fn)(cb.java_object);
            drop(Box::from_raw(self.source_callbacks));
        }
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

        // Call Java to get a FileOpener
        let cb = unsafe { &*self.source_callbacks };
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

        let opener = JavaBackedFileOpener {
            object_store,
            callbacks: Arc::new(OpenerCallbacksHolder { ptr: opener_out }),
            schema: Arc::clone(&self.schema),
        };

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

/// A FileOpener that reads file bytes from ObjectStore and sends them to Java.
///
/// Owns a `JavaFileOpenerCallbacks` pointer (via `OpenerCallbacksHolder`).
/// When `open()` is called, it reads the file from the object store and calls
/// `open_fn` to get a `JavaRecordBatchReaderCallbacks`, which is used to create
/// a `JavaBackedRecordBatchStream`.
struct JavaBackedFileOpener {
    object_store: Arc<dyn ObjectStore>,
    callbacks: Arc<OpenerCallbacksHolder>,
    schema: SchemaRef,
}

impl Unpin for JavaBackedFileOpener {}

impl FileOpener for JavaBackedFileOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let callbacks = Arc::clone(&self.callbacks);
        let schema = Arc::clone(&self.schema);

        Ok(Box::pin(async move {
            // 1. Read file bytes from ObjectStore
            let path = file_meta.object_meta.location.clone();
            let data = store.get(&path).await?.bytes().await?;
            let bytes = data.to_vec();

            // 2. Call Java callback to open file
            let cb = unsafe { &*callbacks.ptr };
            let mut reader_out: *mut JavaRecordBatchReaderCallbacks = std::ptr::null_mut();
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let result = unsafe {
                (cb.open_fn)(
                    cb.java_object,
                    bytes.as_ptr(),
                    bytes.len(),
                    &mut reader_out,
                    &mut error_out,
                )
            };
            unsafe {
                check_callback_result(result, error_out, "open file from Java FileOpener")?;
            }

            if reader_out.is_null() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Java FileOpener.open returned null reader".to_string(),
                ));
            }

            // 3. Wrap reader as stream, mapping DataFusionError to ArrowError
            let stream = JavaBackedRecordBatchStream::new(reader_out, schema);
            let mapped = stream.map(|result| {
                result.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))
            });
            Ok(Box::pin(mapped)
                as BoxStream<
                    'static,
                    Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>,
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
    _file_content: *const u8,
    _file_content_len: usize,
    _reader_out: *mut *mut JavaRecordBatchReaderCallbacks,
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
/// * `url` - Directory path (null-terminated C string)
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
    url: *const c_char,
    file_extension: *const c_char,
    schema: *mut FFI_ArrowSchema,
    format_callbacks: *mut JavaFileFormatCallbacks,
    collect_stat: bool,
    target_partitions: usize,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null()
        || rt.is_null()
        || name.is_null()
        || url.is_null()
        || file_extension.is_null()
        || schema.is_null()
        || format_callbacks.is_null()
    {
        return set_error_return(error_out, "Null pointer argument");
    }

    let context = &*(ctx as *mut SessionContext);
    let runtime = &*(rt as *mut Runtime);

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid table name: {}", e)),
    };

    let url_str = match CStr::from_ptr(url).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => return set_error_return(error_out, &format!("Invalid URL: {}", e)),
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
        .with_collect_stat(collect_stat)
        .with_target_partitions(target_partitions);

    // Parse URL and create table config
    runtime.block_on(async {
        let table_url = match ListingTableUrl::parse(&url_str) {
            Ok(u) => u,
            Err(e) => return set_error_return(error_out, &format!("Failed to parse URL: {}", e)),
        };

        let config = ListingTableConfig::new(table_url)
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
