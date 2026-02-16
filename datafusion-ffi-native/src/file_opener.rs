//! Rust FileOpener implementation that calls back into Java.

use abi_stable::StableAbi;
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::FileOpener;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::DataFusionError;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use datafusion_ffi::util::FFIResult;
use futures::stream::BoxStream;
use std::ffi::c_void;
use std::sync::Arc;

/// C-compatible FFI struct for a Java-backed FileOpener.
///
/// Follows the upstream `datafusion-ffi` convention used by `FFI_CatalogProvider`,
/// `FFI_TableProvider`, etc. Java allocates this struct in arena memory and populates
/// all fields. Rust copies it via `ptr::read` / `MaybeUninit`.
///
/// Layout (48 bytes, align 8):
/// ```text
/// offset  0: open fn ptr              (ADDRESS)
/// offset  8: clone fn ptr             (ADDRESS)
/// offset 16: release fn ptr           (ADDRESS)
/// offset 24: version fn ptr           (ADDRESS)
/// offset 32: private_data ptr         (ADDRESS)
/// offset 40: library_marker_id fn ptr (ADDRESS)
/// ```
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_FileOpener {
    /// Open a file, returning an `FFIResult<FFI_RecordBatchStream>`.
    ///
    /// Parameters:
    /// - opener: pointer to this struct (`&Self`)
    /// - file_path: pointer to UTF-8 file path bytes
    /// - file_path_len: length of file path bytes
    /// - file_size: file size in bytes
    /// - has_range: 1 if byte range present, 0 if not
    /// - range_start: start of byte range (0 if no range)
    /// - range_end: end of byte range (0 if no range)
    ///
    /// Returns `FFIResult::ROk(FFI_RecordBatchStream)` on success,
    /// `FFIResult::RErr(RString)` on error.
    pub open: unsafe extern "C" fn(
        opener: &Self,
        file_path: *const u8,
        file_path_len: usize,
        file_size: u64,
        has_range: i32,
        range_start: i64,
        range_end: i64,
    ) -> FFIResult<FFI_RecordBatchStream>,

    /// Clone this FFI_FileOpener struct. Implemented in Rust.
    pub clone: unsafe extern "C" fn(opener: &Self) -> Self,

    /// Release resources. Implemented in Rust.
    pub release: unsafe extern "C" fn(opener: &mut Self),

    /// Return the FFI version. Implemented in Rust.
    pub version: unsafe extern "C" fn() -> u64,

    /// Opaque pointer to Java-side data (NULL for Java-backed providers).
    pub private_data: *mut c_void,

    /// Library marker function pointer for foreign-library detection.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_FileOpener {}
unsafe impl Sync for FFI_FileOpener {}

impl Clone for FFI_FileOpener {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_FileOpener {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// Clone an FFI_FileOpener.
///
/// Copies all function pointers and private_data.
/// Java stores this function's symbol in the `clone` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_opener_clone(opener: &FFI_FileOpener) -> FFI_FileOpener {
    FFI_FileOpener {
        open: opener.open,
        clone: opener.clone,
        release: opener.release,
        version: opener.version,
        private_data: opener.private_data,
        library_marker_id: opener.library_marker_id,
    }
}

/// Release an FFI_FileOpener.
///
/// No-op for Java-backed openers: private_data is NULL and Java's arena
/// manages the lifecycle.
/// Java stores this function's symbol in the `release` field.
#[no_mangle]
pub unsafe extern "C" fn datafusion_file_opener_release(_opener: &mut FFI_FileOpener) {
    // No-op: private_data is NULL for Java-backed openers.
}

/// A FileOpener that passes file paths to Java for reading.
///
/// Stores `FFI_FileOpener` in an `Arc` so the callbacks can be shared
/// into the `Send` async block returned by `open()`. The `Arc` is necessary
/// because raw pointers (`*mut c_void`) are not `Send`, and Rust's async
/// generator analysis checks individual captured fields.
pub(crate) struct ForeignFileOpener {
    pub(crate) ffi: Arc<FFI_FileOpener>,
}

impl FileOpener for ForeignFileOpener {
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
                                    RecordBatch,
                                    DataFusionError,
                                >,
                            >,
                        >,
                    > + Send,
            >,
        >,
    > {
        let ffi = Arc::clone(&self.ffi);

        Ok(Box::pin(async move {
            // 1. Extract fields from PartitionedFile
            let path = format!("/{}", partitioned_file.path().as_ref());
            let path_bytes = path.as_bytes();
            let file_size = partitioned_file.object_meta.size as u64;
            let (has_range, range_start, range_end) = match &partitioned_file.range {
                Some(range) => (1i32, range.start as i64, range.end as i64),
                None => (0i32, 0i64, 0i64),
            };

            // 2. Call Java callback to open file via FFIResult
            let result: FFIResult<FFI_RecordBatchStream> = unsafe {
                (ffi.open)(
                    &*ffi,
                    path_bytes.as_ptr(),
                    path_bytes.len(),
                    file_size,
                    has_range,
                    range_start,
                    range_end,
                )
            };

            // 3. The FFI_RecordBatchStream implements Stream<Item=Result<RecordBatch>>
            match result {
                FFIResult::ROk(ffi_stream) => Ok(Box::pin(ffi_stream)
                    as BoxStream<
                        'static,
                        Result<RecordBatch, DataFusionError>,
                    >),
                FFIResult::RErr(e) => Err(DataFusionError::Execution(format!(
                    "Failed to open file from Java FileOpener: {}",
                    e.as_str()
                ))),
            }
        }))
    }
}

/// Return the size of `FFI_FileOpener` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_opener_size() -> usize {
    std::mem::size_of::<FFI_FileOpener>()
}

/// Return the size of `FFIResult<FFI_RecordBatchStream>` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_opener_result_size() -> usize {
    std::mem::size_of::<FFIResult<FFI_RecordBatchStream>>()
}
