//! Error buffer and upcall helpers for Java FFI callbacks.
//!
//! Most Rust→Java upcalls follow the same shape: allocate an `ErrorBuffer`, hand its address +
//! capacity to Java, invoke the trait method, then translate the result. The `do_*_upcall`
//! family centralizes that boilerplate. Pick the variant whose return-code convention matches
//! the trait method you are calling — each one's doc comment names the convention.

use datafusion::common::DataFusionError;

/// A 32 KiB error buffer for capturing error messages from Java FFI callbacks.
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

    pub fn read(&self) -> String {
        let len = self.buf.iter().position(|&b| b == 0).unwrap_or(self.buf.len());
        if len == 0 {
            return String::new();
        }
        String::from_utf8_lossy(&self.buf[..len]).into_owned()
    }
}

/// Upcall that returns a raw pointer (`usize`): non-zero on success, `0` on error.
///
/// Allocates an `ErrorBuffer`, runs `f`, treats `0` as failure (reading the error buffer for the
/// message), and on success reconstructs `Box<T>` from the returned pointer. The `unsafe` for
/// `Box::from_raw` is contained here because the null check is the only meaningful validation.
///
/// Use this for Java callbacks that produce a freshly-boxed Diplomat opaque (scan, execute,
/// `create_file_opener`, etc.).
///
/// ```ignore
/// let plan = do_returning_upcall::<DfExecutionPlan>(
///     "Java scan callback failed",
///     Box::new(|err| self.inner.scan(session_addr, ..., err.addr(), err.cap())),
/// )?;
/// ```
pub(crate) fn do_returning_upcall<'a, T>(
    context: &str,
    f: Box<dyn FnOnce(&ErrorBuffer) -> usize + 'a>,
) -> Result<Box<T>, DataFusionError> {
    let err = ErrorBuffer::new();
    let ptr = f(&err);
    if ptr == 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(unsafe { Box::from_raw(ptr as *mut T) })
}

/// Upcall that returns a raw pointer where `0` may be either "not found" or an error.
///
/// Disambiguates with the error buffer: if `f` returns `0` and the buffer is empty, the result is
/// `Ok(None)`; if the buffer is populated, it's `Err`. Use this for lookup-style trait methods
/// like `SchemaProvider::table()` where absence and failure are both valid outcomes.
pub(crate) fn do_option_returning_upcall<'a, T>(
    context: &str,
    f: Box<dyn FnOnce(&ErrorBuffer) -> usize + 'a>,
) -> Result<Option<Box<T>>, DataFusionError> {
    let err = ErrorBuffer::new();
    let ptr = f(&err);
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

/// Upcall that returns a raw pointer with no error channel: non-zero is the boxed value, `0`
/// means absent. No `ErrorBuffer` allocated. Use for trait methods that genuinely cannot fail
/// from the Rust side's perspective.
pub(crate) fn do_option_upcall<T>(f: impl FnOnce() -> usize) -> Option<Box<T>> {
    let ptr = f();
    if ptr == 0 {
        return None;
    }
    Some(unsafe { Box::from_raw(ptr as *mut T) })
}

/// Upcall that returns `0` on success, non-zero on error, with no value to reconstruct. Use for
/// "do this side-effect" trait methods (e.g., `return_field` writing into a Java-allocated
/// schema buffer).
pub(crate) fn do_upcall(
    context: &str,
    f: impl FnOnce(&ErrorBuffer) -> i32,
) -> Result<(), DataFusionError> {
    let err = ErrorBuffer::new();
    let result = f(&err);
    if result != 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(())
}

/// Upcall that returns a non-negative count on success, negative on error. Use when the trait
/// method writes some number of items to a Rust-provided buffer and reports how many it wrote
/// (e.g., `coerce_types`, `state_fields`).
pub(crate) fn do_counted_upcall(
    context: &str,
    f: impl FnOnce(&ErrorBuffer) -> i32,
) -> Result<usize, DataFusionError> {
    let err = ErrorBuffer::new();
    let result = f(&err);
    if result < 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(result as usize)
}

/// Upcall with a tri-state result for optional output writes:
/// negative = error, `0` = absent (nothing written), positive = present (output written). Use
/// when the trait method may write into a Java-provided buffer but is allowed to skip the write
/// (e.g., `VarProvider::get_type` returning "no type known").
pub(crate) fn do_optional_upcall(
    context: &str,
    f: impl FnOnce(&ErrorBuffer) -> i32,
) -> Result<bool, DataFusionError> {
    let err = ErrorBuffer::new();
    let result = f(&err);
    if result < 0 {
        return Err(DataFusionError::External(
            format!("{}: {}", context, err.read()).into(),
        ));
    }
    Ok(result > 0)
}
