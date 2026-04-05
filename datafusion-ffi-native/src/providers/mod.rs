//! DataFusion trait wrappers that dispatch through Diplomat trait vtables.
//!
//! Each Foreign* struct stores a Diplomat-generated trait struct (vtable + data pointer)
//! and implements the corresponding DataFusion trait by calling through the vtable.

mod catalog;
mod file_format;
mod file_opener;
mod file_source;
mod plan;
mod schema;
mod stream;
mod table;
mod udf;

pub use catalog::*;
pub use file_format::*;
pub use file_opener::*;
pub use file_source::*;
pub use plan::*;
pub use schema::*;
pub use stream::*;
pub use table::*;
pub use udf::*;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use std::sync::Arc;

// ── Bridge traits (marker traits for type erasure) ──

pub trait SchemaProviderBridge: datafusion::catalog::SchemaProvider + Send + Sync {}
pub trait TableProviderBridge: datafusion::catalog::TableProvider + Send + Sync {}
pub trait ExecutionPlanBridge: datafusion::physical_plan::ExecutionPlan + Send + Sync {}
pub trait RecordBatchReaderBridge: Send + Sync {
    fn schema(&self) -> Arc<ArrowSchema>;
    fn next_batch(&self) -> Result<Option<RecordBatch>, DataFusionError>;
}

pub trait FileSourceBridge: Send + Sync {
    fn create_file_opener(
        &self,
        schema: &ArrowSchema,
        projection: Option<&[usize]>,
        limit: Option<usize>,
        batch_size: Option<usize>,
    ) -> Result<Box<dyn FileOpenerBridge>, DataFusionError>;
}

pub trait FileOpenerBridge: Send + Sync {
    fn open(
        &self,
        path: &str,
        file_size: u64,
        range: Option<(i64, i64)>,
    ) -> Result<Box<dyn RecordBatchReaderBridge>, DataFusionError>;
}

/// A 32 KiB error buffer for capturing error messages from Java FFI callbacks.
///
/// Usage:
/// ```ignore
/// let err = ErrorBuffer::new();
/// let ptr = some_ffi_call(err.addr(), err.cap());
/// if ptr == 0 {
///     let msg = err.read();
/// }
/// ```
pub(crate) struct ErrorBuffer {
    buf: Vec<u8>,
}

impl ErrorBuffer {
    pub fn new() -> Self {
        Self {
            buf: vec![0u8; 32768],
        }
    }

    pub fn addr(&self) -> usize {
        self.buf.as_ptr() as usize
    }

    pub fn cap(&self) -> usize {
        self.buf.len()
    }

    /// Read the error message written by Java. Returns empty string if none.
    pub fn read(&self) -> String {
        unsafe { read_error(self.addr(), self.cap()) }
    }
}

/// A Java upcall that accepts `(error_addr, error_cap)` and returns a raw pointer
/// (0 on error, non-zero on success).
pub(crate) type Upcall<'a> = dyn FnOnce(/*error_addr:*/ usize, /*error_cap:*/ usize) -> usize + 'a;

/// Invoke a Java upcall that returns a raw pointer (0 on error).
/// Creates an internal error buffer, calls `f` with `(error_addr, error_cap)`,
/// and reconstructs `Box<T>` on success or returns a `DataFusionError` on failure.
///
/// The pointer returned by `f` must be a valid `Box<T>` pointer created
/// by `Box::into_raw` on the Java side (via `Df*.createRaw()`).
pub(crate) fn do_returning_upcall<'a, T>(
    context: &str,
    f: Box<Upcall<'a>>,
) -> Result<Box<T>, DataFusionError> {
    let err = ErrorBuffer::new();
    let ptr = f(err.addr(), err.cap());
    if ptr == 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(unsafe { Box::from_raw(ptr as *mut T) })
}

/// Invoke a Java upcall that returns a raw pointer which may validly be null.
/// Null with a populated error buffer means failure; null with an empty error
/// buffer means `Ok(None)`; non-null means `Ok(Some(Box<T>))`.
///
/// The non-null pointer returned by `f` must be a valid `Box<T>` pointer created
/// by `Box::into_raw` on the Java side (via `Df*.createRaw()`).
pub(crate) fn do_option_returning_upcall<'a, T>(
    context: &str,
    f: Box<Upcall<'a>>,
) -> Result<Option<Box<T>>, DataFusionError> {
    let err = ErrorBuffer::new();
    let ptr = f(err.addr(), err.cap());
    if ptr == 0 {
        let msg = err.read();
        if !msg.is_empty() {
            return Err(DataFusionError::External(
                format!("{}: {}", context, msg).into(),
            ));
        }
        return Ok(None);
    }
    Ok(Some(unsafe { Box::from_raw(ptr as *mut T) }))
}

/// Invoke a Java upcall that returns 0 on success, non-zero on error.
/// Creates an internal error buffer, calls `f` with `(error_addr, error_cap)`,
/// and returns `Ok(())` on success or a `DataFusionError` on failure.
pub(crate) fn do_upcall(
    context: &str,
    f: impl FnOnce(/*error_addr:*/ usize, /*error_cap:*/ usize) -> i32,
) -> Result<(), DataFusionError> {
    let err = ErrorBuffer::new();
    let result = f(err.addr(), err.cap());
    if result != 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(())
}

/// Invoke a Java upcall that returns a non-negative count on success, or negative on error.
/// Creates an internal error buffer, calls `f` with `(error_addr, error_cap)`,
/// and returns the count as `usize` on success or a `DataFusionError` on failure.
pub(crate) fn do_counted_upcall(
    context: &str,
    f: impl FnOnce(/*error_addr:*/ usize, /*error_cap:*/ usize) -> i32,
) -> Result<usize, DataFusionError> {
    let err = ErrorBuffer::new();
    let result = f(err.addr(), err.cap());
    if result < 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(result as usize)
}

/// Read an error message from the error buffer. Returns empty string if addr is 0.
unsafe fn read_error(addr: usize, cap: usize) -> String {
    if addr == 0 || cap == 0 {
        return String::new();
    }
    // Scan for actual content (non-zero bytes)
    let slice = std::slice::from_raw_parts(addr as *const u8, cap);
    // Find the end of the message (first zero byte or end of slice)
    let len = slice.iter().position(|&b| b == 0).unwrap_or(cap);
    if len == 0 {
        return String::new();
    }
    String::from_utf8_lossy(&slice[..len]).into_owned()
}
