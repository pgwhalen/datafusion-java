//! Size helpers for FFI_RecordBatchStream and related types used by
//! RecordBatchReaderHandle on the Java side.

use abi_stable::std_types::ROption;
use async_ffi::FfiPoll;
use datafusion_ffi::arrow_wrappers::{WrappedArray, WrappedSchema};
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use datafusion_ffi::util::FFIResult;

/// Return sizeof(FfiPoll<ROption<FFIResult<WrappedArray>>>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_poll_result_size() -> usize {
    std::mem::size_of::<FfiPoll<ROption<FFIResult<WrappedArray>>>>()
}

/// Return alignof(FfiPoll<ROption<FFIResult<WrappedArray>>>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_poll_result_align() -> usize {
    std::mem::align_of::<FfiPoll<ROption<FFIResult<WrappedArray>>>>()
}

/// Return sizeof(WrappedSchema) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_wrapped_schema_size() -> usize {
    std::mem::size_of::<WrappedSchema>()
}

/// Return sizeof(FFI_RecordBatchStream) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_record_batch_stream_size() -> usize {
    std::mem::size_of::<FFI_RecordBatchStream>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_batch_reader_sizes() {
        // FFI_RecordBatchStream: 3 fn ptrs + private_data = 4 * 8 = 32 bytes
        assert_eq!(
            std::mem::size_of::<FFI_RecordBatchStream>(),
            32,
            "FFI_RecordBatchStream size"
        );

        // WrappedSchema = FFI_ArrowSchema = 72 bytes
        assert_eq!(
            std::mem::size_of::<WrappedSchema>(),
            72,
            "WrappedSchema size"
        );

        // poll_next return type
        let poll_size = std::mem::size_of::<FfiPoll<ROption<FFIResult<WrappedArray>>>>();
        assert_eq!(poll_size, 176, "poll_next return type size");

        let poll_align = std::mem::align_of::<FfiPoll<ROption<FFIResult<WrappedArray>>>>();
        assert_eq!(poll_align, 8, "poll_next return type alignment");
    }
}
