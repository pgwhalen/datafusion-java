//! Rust FileOpener implementation that calls back into Java.

use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::datasource::physical_plan::FileOpener;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::DataFusionError;
use futures::stream::BoxStream;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::check_callback_result;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;

/// C-compatible callback struct for a Java-backed FileOpener.
///
/// Java allocates this struct in arena memory and populates the fields.
/// Rust copies it via `ptr::read` / `MaybeUninit` when constructing a `JavaBackedFileOpener`.
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

/// A FileOpener that passes file paths to Java for reading.
///
/// Stores `JavaFileOpenerCallbacks` in an `Arc` so the callbacks can be shared
/// into the `Send` async block returned by `open()`. The `Arc` is necessary
/// because raw pointers (`*mut c_void`) are not `Send`, and Rust's async
/// generator analysis checks individual captured fields.
pub(crate) struct JavaBackedFileOpener {
    pub(crate) callbacks: Arc<JavaFileOpenerCallbacks>,
}

impl Drop for JavaBackedFileOpener {
    fn drop(&mut self) {
        unsafe {
            (self.callbacks.release_fn)(self.callbacks.java_object);
        }
    }
}

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
            let cb = &*callbacks;
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

/// Return the size of `JavaFileOpenerCallbacks` for Java-side validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_file_opener_callbacks_size() -> usize {
    std::mem::size_of::<JavaFileOpenerCallbacks>()
}
