use arrow::array::{Array, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::TryStreamExt;
use std::ffi::{c_char, c_void};
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error};

/// Destroy a RecordBatchStream.
///
/// # Safety
/// The pointer must have been created by `datafusion_dataframe_execute_stream`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_stream_destroy(stream: *mut c_void) {
    if !stream.is_null() {
        drop(Box::from_raw(stream as *mut SendableRecordBatchStream));
    }
}

/// Get the schema of a RecordBatchStream.
///
/// # Arguments
/// * `stream` - Pointer to the RecordBatchStream
/// * `schema_out` - Pointer to FFI_ArrowSchema to receive the schema
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
///
/// # Safety
/// All pointers must be valid. The schema_out will be filled with the schema.
#[no_mangle]
pub unsafe extern "C" fn datafusion_stream_schema(
    stream: *mut c_void,
    schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if stream.is_null() || schema_out.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    let stream = &*(stream as *mut SendableRecordBatchStream);
    let schema = stream.schema();

    match FFI_ArrowSchema::try_from(&*schema) {
        Ok(ffi_schema) => {
            std::ptr::write(schema_out, ffi_schema);
            0
        }
        Err(e) => {
            set_error(error_out, &format!("Failed to export schema: {}", e));
            -1
        }
    }
}

/// Get the next record batch from a stream.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `stream` - Pointer to the RecordBatchStream
/// * `array_out` - Pointer to FFI_ArrowArray to receive the batch data
/// * `schema_out` - Pointer to FFI_ArrowSchema to receive the batch schema
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 1 if a batch was returned, 0 if end of stream, -1 on error.
///
/// # Safety
/// All pointers must be valid. The array_out and schema_out will be filled on success.
#[no_mangle]
pub unsafe extern "C" fn datafusion_stream_next(
    rt: *mut c_void,
    stream: *mut c_void,
    array_out: *mut FFI_ArrowArray,
    schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || stream.is_null() || array_out.is_null() || schema_out.is_null() {
        set_error(error_out, "Null pointer argument");
        return -1;
    }

    let runtime = &*(rt as *mut Runtime);
    let stream = &mut *(stream as *mut SendableRecordBatchStream);

    runtime.block_on(async {
        match stream.try_next().await {
            Ok(Some(batch)) => {
                // Convert RecordBatch to StructArray for FFI export
                let struct_array: StructArray = batch.into();
                let (ffi_array, ffi_schema) = match to_ffi(&struct_array.to_data()) {
                    Ok(result) => result,
                    Err(e) => {
                        set_error(error_out, &format!("Failed to export batch: {}", e));
                        return -1;
                    }
                };
                std::ptr::write(array_out, ffi_array);
                std::ptr::write(schema_out, ffi_schema);
                1
            }
            Ok(None) => {
                // End of stream
                0
            }
            Err(e) => {
                set_error(error_out, &format!("Stream error: {}", e));
                -1
            }
        }
    })
}
