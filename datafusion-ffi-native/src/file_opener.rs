//! Rust FileOpener implementation that calls back into Java.

use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::FileOpener;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::DataFusionError;
use futures::stream::BoxStream;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::{check_callback_result, set_error_return};
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;

/// C-compatible callback struct for a Java-backed FileOpener.
///
/// Created by `JavaFileSourceCallbacks.create_file_opener_fn`. Owned by `JavaBackedFileOpener`.
#[repr(C)]
pub struct JavaFileOpenerCallbacks {
    /// Opaque pointer to the Java object.
    pub java_object: *mut c_void,

    /// Open a file, returning an FFI_RecordBatchStream struct.
    ///
    /// Parameters:
    /// - java_object: opaque pointer
    /// - file_path: pointer to UTF-8 file path bytes
    /// - file_path_len: length of file path bytes
    /// - file_size: file size in bytes
    /// - has_range: 1 if byte range present, 0 if not
    /// - range_start: start of byte range (0 if no range)
    /// - range_end: end of byte range (0 if no range)
    /// - stream_out: pointer to FFI_RecordBatchStream buffer (Java writes 32 bytes)
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

unsafe impl Send for JavaFileOpenerCallbacks {}
unsafe impl Sync for JavaFileOpenerCallbacks {}

/// Wraps a raw opener callback pointer for shared ownership.
///
/// This allows the callback pointer to be shared across threads (via Arc)
/// and into async blocks that require `Send`.
pub(crate) struct OpenerCallbacksHolder {
    pub(crate) ptr: *mut JavaFileOpenerCallbacks,
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
/// `open_fn` to get an `FFI_RecordBatchStream`, which directly implements
/// `Stream<Item=Result<RecordBatch>>`.
pub(crate) struct JavaBackedFileOpener {
    pub(crate) callbacks: Arc<OpenerCallbacksHolder>,
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
                                    RecordBatch,
                                    DataFusionError,
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
            let mut ffi_stream =
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
                    ffi_stream.as_mut_ptr(),
                    &mut error_out,
                )
            };
            unsafe {
                check_callback_result(result, error_out, "open file from Java FileOpener")?;
            }

            // 3. The FFI_RecordBatchStream implements Stream<Item=Result<RecordBatch>>
            let ffi_stream = unsafe { ffi_stream.assume_init() };
            Ok(Box::pin(ffi_stream)
                as BoxStream<
                    'static,
                    Result<RecordBatch, DataFusionError>,
                >)
        }))
    }
}

// Dummy functions for initialization
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
