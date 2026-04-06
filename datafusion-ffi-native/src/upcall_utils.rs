//! Error buffer and upcall helpers for Java FFI callbacks.

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

pub(crate) fn do_option_upcall<T>(f: impl FnOnce() -> usize) -> Option<Box<T>> {
    let ptr = f();
    if ptr == 0 {
        return None;
    }
    Some(unsafe { Box::from_raw(ptr as *mut T) })
}

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
