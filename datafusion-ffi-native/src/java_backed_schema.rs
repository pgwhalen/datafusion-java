//! Rust SchemaProvider implementation that calls back into Java.

use crate::error::set_error;
use crate::java_backed_provider::JavaBackedTableProvider;
use crate::java_provider::{JavaSchemaProviderCallbacks, JavaTableProviderCallbacks};
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use std::any::Any;
use std::ffi::{c_char, CStr};
use std::sync::Arc;

/// A SchemaProvider that calls back into Java.
pub struct JavaBackedSchemaProvider {
    callbacks: *mut JavaSchemaProviderCallbacks,
}

impl std::fmt::Debug for JavaBackedSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JavaBackedSchemaProvider")
    }
}

impl JavaBackedSchemaProvider {
    /// Create a new JavaBackedSchemaProvider from callback pointers.
    ///
    /// # Safety
    /// The callbacks pointer must be valid and point to a properly initialized struct.
    pub unsafe fn new(callbacks: *mut JavaSchemaProviderCallbacks) -> Self {
        Self { callbacks }
    }
}

unsafe impl Send for JavaBackedSchemaProvider {}
unsafe impl Sync for JavaBackedSchemaProvider {}

#[async_trait]
impl SchemaProvider for JavaBackedSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        unsafe {
            let cb = &*self.callbacks;

            let mut names_out: *mut *mut c_char = std::ptr::null_mut();
            let mut names_len: usize = 0;
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (cb.table_names_fn)(
                cb.java_object,
                &mut names_out,
                &mut names_len,
                &mut error_out,
            );

            if result != 0 {
                // Log error but return empty list
                if !error_out.is_null() {
                    crate::datafusion_free_string(error_out);
                }
                return vec![];
            }

            if names_out.is_null() || names_len == 0 {
                return vec![];
            }

            // Convert C strings to Rust strings
            let mut names = Vec::with_capacity(names_len);
            for i in 0..names_len {
                let s = *names_out.add(i);
                if !s.is_null() {
                    if let Ok(name) = CStr::from_ptr(s).to_str() {
                        names.push(name.to_string());
                    }
                }
            }

            // Free the string array
            crate::datafusion_free_string_array(names_out, names_len);

            names
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        unsafe {
            let cb = &*self.callbacks;

            let c_name = match std::ffi::CString::new(name) {
                Ok(s) => s,
                Err(_) => return Ok(None),
            };

            let mut table_out: *mut JavaTableProviderCallbacks = std::ptr::null_mut();
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (cb.table_fn)(
                cb.java_object,
                c_name.as_ptr(),
                &mut table_out,
                &mut error_out,
            );

            if result != 0 {
                let msg = if !error_out.is_null() {
                    let s = CStr::from_ptr(error_out).to_string_lossy().to_string();
                    crate::datafusion_free_string(error_out);
                    s
                } else {
                    format!("Failed to get table '{}' from Java SchemaProvider", name)
                };
                return Err(datafusion::error::DataFusionError::Execution(msg));
            }

            if table_out.is_null() {
                return Ok(None);
            }

            let provider = JavaBackedTableProvider::new(table_out)?;
            Ok(Some(Arc::new(provider)))
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        unsafe {
            let cb = &*self.callbacks;

            let c_name = match std::ffi::CString::new(name) {
                Ok(s) => s,
                Err(_) => return false,
            };

            let result = (cb.table_exists_fn)(cb.java_object, c_name.as_ptr());
            result == 1
        }
    }
}

impl Drop for JavaBackedSchemaProvider {
    fn drop(&mut self) {
        unsafe {
            let callbacks = &*self.callbacks;
            (callbacks.release_fn)(callbacks.java_object);
            // Free the callbacks struct itself
            drop(Box::from_raw(self.callbacks));
        }
    }
}

/// Allocate a JavaSchemaProviderCallbacks struct.
///
/// # Safety
/// The returned pointer must be freed by Rust when the provider is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_schema_provider_callbacks() -> *mut JavaSchemaProviderCallbacks {
    Box::into_raw(Box::new(JavaSchemaProviderCallbacks {
        java_object: std::ptr::null_mut(),
        table_names_fn: dummy_table_names_fn,
        table_fn: dummy_table_fn,
        table_exists_fn: dummy_table_exists_fn,
        release_fn: dummy_release_fn,
    }))
}

// Dummy functions for initialization - Java will set the actual function pointers
unsafe extern "C" fn dummy_table_names_fn(
    _java_object: *mut std::ffi::c_void,
    _names_out: *mut *mut *mut c_char,
    _names_len_out: *mut usize,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error(error_out, "SchemaProvider callbacks not initialized");
    -1
}

unsafe extern "C" fn dummy_table_fn(
    _java_object: *mut std::ffi::c_void,
    _name: *const c_char,
    _table_out: *mut *mut JavaTableProviderCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error(error_out, "SchemaProvider callbacks not initialized");
    -1
}

unsafe extern "C" fn dummy_table_exists_fn(
    _java_object: *mut std::ffi::c_void,
    _name: *const c_char,
) -> i32 {
    0 // false
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut std::ffi::c_void) {
    // Do nothing
}
