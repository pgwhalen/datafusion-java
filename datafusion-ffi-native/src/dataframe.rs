use datafusion::dataframe::DataFrame;
use std::ffi::{c_char, c_void};
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error};

/// Destroy a DataFrame.
///
/// # Safety
/// The pointer must have been created by `datafusion_context_sql`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_destroy(df: *mut c_void) {
    if !df.is_null() {
        drop(Box::from_raw(df as *mut DataFrame));
    }
}

/// Execute a DataFrame and return a RecordBatchStream.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to RecordBatchStream on success, null on error.
///
/// # Safety
/// All pointers must be valid.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_execute_stream(
    rt: *mut c_void,
    df: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        set_error(error_out, "Null pointer argument");
        return std::ptr::null_mut();
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);

    runtime.block_on(async {
        match dataframe.clone().execute_stream().await {
            Ok(stream) => Box::into_raw(Box::new(stream)) as *mut c_void,
            Err(e) => {
                set_error(error_out, &format!("Execute stream failed: {}", e));
                std::ptr::null_mut()
            }
        }
    })
}
