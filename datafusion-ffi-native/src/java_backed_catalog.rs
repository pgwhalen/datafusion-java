//! Rust CatalogProvider implementation that calls back into Java.

use crate::error::{clear_error, set_error};
use crate::java_backed_schema::JavaBackedSchemaProvider;
use crate::java_provider::{JavaCatalogProviderCallbacks, JavaSchemaProviderCallbacks};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::execution::context::SessionContext;
use std::any::Any;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;

/// A CatalogProvider that calls back into Java.
pub struct JavaBackedCatalogProvider {
    callbacks: *mut JavaCatalogProviderCallbacks,
}

impl std::fmt::Debug for JavaBackedCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JavaBackedCatalogProvider")
    }
}

impl JavaBackedCatalogProvider {
    /// Create a new JavaBackedCatalogProvider from callback pointers.
    ///
    /// # Safety
    /// The callbacks pointer must be valid and point to a properly initialized struct.
    pub unsafe fn new(callbacks: *mut JavaCatalogProviderCallbacks) -> Self {
        Self { callbacks }
    }
}

unsafe impl Send for JavaBackedCatalogProvider {}
unsafe impl Sync for JavaBackedCatalogProvider {}

impl CatalogProvider for JavaBackedCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        unsafe {
            let cb = &*self.callbacks;

            let mut names_out: *mut *mut c_char = std::ptr::null_mut();
            let mut names_len: usize = 0;
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (cb.schema_names_fn)(
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

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        unsafe {
            let cb = &*self.callbacks;

            let c_name = match std::ffi::CString::new(name) {
                Ok(s) => s,
                Err(_) => return None,
            };

            let mut schema_out: *mut JavaSchemaProviderCallbacks = std::ptr::null_mut();
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (cb.schema_fn)(
                cb.java_object,
                c_name.as_ptr(),
                &mut schema_out,
                &mut error_out,
            );

            if result != 0 {
                if !error_out.is_null() {
                    crate::datafusion_free_string(error_out);
                }
                return None;
            }

            if schema_out.is_null() {
                return None;
            }

            let provider = JavaBackedSchemaProvider::new(schema_out);
            Some(Arc::new(provider))
        }
    }
}

impl Drop for JavaBackedCatalogProvider {
    fn drop(&mut self) {
        unsafe {
            let callbacks = &*self.callbacks;
            (callbacks.release_fn)(callbacks.java_object);
            // Free the callbacks struct itself
            drop(Box::from_raw(self.callbacks));
        }
    }
}

/// Allocate a JavaCatalogProviderCallbacks struct.
///
/// # Safety
/// The returned pointer must be freed by Rust when the provider is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_catalog_provider_callbacks() -> *mut JavaCatalogProviderCallbacks {
    Box::into_raw(Box::new(JavaCatalogProviderCallbacks {
        java_object: std::ptr::null_mut(),
        schema_names_fn: dummy_schema_names_fn,
        schema_fn: dummy_schema_fn,
        release_fn: dummy_release_fn,
    }))
}

/// Register a catalog with the session context.
///
/// # Arguments
/// * `ctx` - Pointer to the SessionContext
/// * `name` - Catalog name (null-terminated C string)
/// * `callbacks` - Pointer to JavaCatalogProviderCallbacks (takes ownership)
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid. The callbacks struct ownership is transferred to Rust.
#[no_mangle]
pub unsafe extern "C" fn datafusion_context_register_catalog(
    ctx: *mut c_void,
    name: *const c_char,
    callbacks: *mut JavaCatalogProviderCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if ctx.is_null() || name.is_null() || callbacks.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    let context = &*(ctx as *mut SessionContext);

    // Get catalog name
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s.to_string(),
        Err(e) => {
            set_error(error_out, &format!("Invalid catalog name: {}", e));
            return -1;
        }
    };

    // Create the Java-backed catalog provider
    let provider = JavaBackedCatalogProvider::new(callbacks);

    // Register the catalog (returns the old catalog if one was registered with the same name)
    context.register_catalog(&name_str, Arc::new(provider));

    0
}

// Dummy functions for initialization - Java will set the actual function pointers
unsafe extern "C" fn dummy_schema_names_fn(
    _java_object: *mut std::ffi::c_void,
    _names_out: *mut *mut *mut c_char,
    _names_len_out: *mut usize,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error(error_out, "CatalogProvider callbacks not initialized");
    -1
}

unsafe extern "C" fn dummy_schema_fn(
    _java_object: *mut std::ffi::c_void,
    _name: *const c_char,
    _schema_out: *mut *mut JavaSchemaProviderCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error(error_out, "CatalogProvider callbacks not initialized");
    -1
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut std::ffi::c_void) {
    // Do nothing
}
