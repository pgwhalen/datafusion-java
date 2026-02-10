//! Helpers for Java to directly construct `datafusion-ffi` FFI structs.
//!
//! Java constructs `FFI_RecordBatchStream` and `FFI_ExecutionPlan` directly
//! in Java arena memory, using upcall stubs as function pointers. This module
//! provides:
//!
//! - **Size helpers**: Java validates struct sizes at runtime to catch mismatches.
//! - **RString/RVec helpers**: Java can't construct `abi_stable` types directly;
//!   Rust helpers do it and write the result to Java-provided buffers.
//! - **FFI_PlanProperties helper**: Constructs the function-pointer-based
//!   `FFI_PlanProperties` from simple integer values + Arrow schema.
//! - **Library marker**: Prevents the `TryFrom<&FFI_ExecutionPlan>` fast-path.

use std::ffi::c_void;
use std::sync::Arc;

use abi_stable::std_types::{ROption, RString, RVec};
use arrow::datatypes::Schema;
use arrow::ffi::FFI_ArrowSchema;
use async_ffi::FfiPoll;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::PlanProperties;
use datafusion_ffi::arrow_wrappers::{WrappedArray, WrappedSchema};
use datafusion_ffi::execution::FFI_TaskContext;
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::plan_properties::FFI_PlanProperties;
use datafusion_ffi::record_batch_stream::FFI_RecordBatchStream;
use datafusion_ffi::util::FFIResult;

// ============================================================================
// Library marker — prevents the `TryFrom<&FFI_ExecutionPlan>` impl from
// taking the "local" fast-path (which would try to downcast private_data
// to an `ExecutionPlanPrivateData`, but ours is a Java-backed plan).
// ============================================================================

/// Library marker function for Java-backed FFI_ExecutionPlan structs.
///
/// Java looks up this symbol and stores the function pointer in the
/// `library_marker_id` field of FFI_ExecutionPlan.
#[no_mangle]
pub extern "C" fn datafusion_java_marker_id() -> usize {
    static MARKER: u8 = 0;
    &MARKER as *const u8 as usize
}

// ============================================================================
// Size and layout helpers — Java calls these to validate struct sizes at
// runtime, and to construct RString for error returns.
// ============================================================================

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

/// Return sizeof(RString) for Java to know error payload size.
#[no_mangle]
pub extern "C" fn datafusion_rstring_size() -> usize {
    std::mem::size_of::<RString>()
}

/// Return sizeof(FFI_ExecutionPlan) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_execution_plan_size() -> usize {
    std::mem::size_of::<FFI_ExecutionPlan>()
}

/// Return sizeof(FFI_PlanProperties) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_plan_properties_size() -> usize {
    std::mem::size_of::<FFI_PlanProperties>()
}

/// Return sizeof(RVec<FFI_ExecutionPlan>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_rvec_plan_size() -> usize {
    std::mem::size_of::<RVec<FFI_ExecutionPlan>>()
}

/// Return sizeof(FFIResult<FFI_RecordBatchStream>) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_result_stream_size() -> usize {
    std::mem::size_of::<FFIResult<FFI_RecordBatchStream>>()
}

/// Return sizeof(FFI_TaskContext) for Java validation.
#[no_mangle]
pub extern "C" fn datafusion_ffi_task_context_size() -> usize {
    std::mem::size_of::<FFI_TaskContext>()
}

// ============================================================================
// Construction helpers — Java calls these to create abi_stable types that
// it cannot construct directly.
// ============================================================================

/// Write an RString into the output buffer. Called by Java for error returns
/// in `poll_next` and `execute`. Java provides UTF-8 bytes; Rust constructs
/// an `RString` and writes it into the output.
///
/// # Safety
/// - `ptr` must point to `len` valid UTF-8 bytes.
/// - `out` must point to a buffer of at least `size_of::<RString>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_rstring(
    ptr: *const u8,
    len: usize,
    out: *mut c_void,
) {
    let bytes = std::slice::from_raw_parts(ptr, len);
    let s = std::str::from_utf8_unchecked(bytes);
    let rstring = RString::from(s);
    std::ptr::write(out as *mut RString, rstring);
}

/// Write an empty `RVec<FFI_ExecutionPlan>` into the output buffer.
/// Java-backed plans are always leaf nodes with no children.
///
/// # Safety
/// `out` must point to a buffer of at least `size_of::<RVec<FFI_ExecutionPlan>>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_empty_rvec_plan(out: *mut c_void) {
    let empty: RVec<FFI_ExecutionPlan> = RVec::new();
    std::ptr::write(out as *mut RVec<FFI_ExecutionPlan>, empty);
}

/// Construct an `FFI_PlanProperties` from simple values and an Arrow schema.
///
/// This builds a DataFusion `PlanProperties` from the provided integer values,
/// then converts it to the function-pointer-based `FFI_PlanProperties` struct
/// that `ForeignExecutionPlan` expects.
///
/// The `schema_ptr` must point to a valid `FFI_ArrowSchema` with a non-null
/// release callback. After this function returns, the schema's release callback
/// will have been called and the schema is invalidated.
///
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// - `schema_ptr` must point to a valid `FFI_ArrowSchema`.
/// - `out` must point to a buffer of at least `size_of::<FFI_PlanProperties>()` bytes.
#[no_mangle]
pub unsafe extern "C" fn datafusion_create_ffi_plan_properties(
    partitioning_count: i32,
    emission_type: i32,
    boundedness: i32,
    schema_ptr: *mut FFI_ArrowSchema,
    out: *mut c_void,
) -> i32 {
    // Take ownership of the FFI_ArrowSchema via ptr::read so its Drop
    // impl runs when we're done, releasing the Arrow memory.
    let schema_ffi = std::ptr::read(schema_ptr);
    let schema = match Schema::try_from(&schema_ffi) {
        Ok(s) => Arc::new(s),
        Err(_) => return -1,
    };
    // schema_ffi drops here → release callback fires

    // Build PlanProperties from simple values
    let partitioning = if partitioning_count <= 1 {
        Partitioning::UnknownPartitioning(1)
    } else {
        Partitioning::UnknownPartitioning(partitioning_count as usize)
    };

    let emission = match emission_type {
        1 => EmissionType::Final,
        2 => EmissionType::Both,
        _ => EmissionType::Incremental,
    };

    let bounded = match boundedness {
        1 => Boundedness::Unbounded {
            requires_infinite_memory: false,
        },
        _ => Boundedness::Bounded,
    };

    let properties = PlanProperties::new(
        EquivalenceProperties::new(schema),
        partitioning,
        emission,
        bounded,
    );

    // Convert to FFI_PlanProperties and write to output buffer
    let ffi_props = FFI_PlanProperties::from(&properties);
    std::ptr::write(out as *mut FFI_PlanProperties, ffi_props);

    0
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::std_types::RResult;

    #[test]
    fn test_ffi_type_sizes() {
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

        // RString size (for error returns)
        let rstring_size = std::mem::size_of::<RString>();
        assert!(rstring_size > 0, "RString should have non-zero size");

        // FFI_ExecutionPlan: 6 fn ptrs + private_data + library_marker_id = 8 * 8 = 64 bytes
        assert_eq!(
            std::mem::size_of::<FFI_ExecutionPlan>(),
            64,
            "FFI_ExecutionPlan size"
        );

        // FFI_PlanProperties: 6 fn ptrs + private_data + library_marker_id = 8 * 8 = 64 bytes
        assert_eq!(
            std::mem::size_of::<FFI_PlanProperties>(),
            64,
            "FFI_PlanProperties size"
        );

        // RVec<FFI_ExecutionPlan>: ptr + len + cap + vtable = 4 * 8 = 32 bytes
        assert_eq!(
            std::mem::size_of::<RVec<FFI_ExecutionPlan>>(),
            32,
            "RVec<FFI_ExecutionPlan> size"
        );

        // FFIResult<FFI_RecordBatchStream>: discriminant(1) + pad(7) + max(32, 24) = 40 bytes
        assert_eq!(
            std::mem::size_of::<FFIResult<FFI_RecordBatchStream>>(),
            40,
            "FFIResult<FFI_RecordBatchStream> size"
        );

        // FFI_TaskContext: 7 fn ptrs + private_data + library_marker_id = 9 * 8 = 72 bytes
        assert_eq!(
            std::mem::size_of::<FFI_TaskContext>(),
            72,
            "FFI_TaskContext size"
        );
    }

    #[test]
    fn test_poll_result_discriminant_offsets() {
        // Verify the discriminant layout by constructing each variant and
        // examining the raw bytes.

        // Ready(RNone) — end of stream
        let end_of_stream: FfiPoll<ROption<FFIResult<WrappedArray>>> =
            FfiPoll::Ready(ROption::RNone);
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &end_of_stream as *const _ as *const u8,
                std::mem::size_of_val(&end_of_stream),
            )
        };
        assert_eq!(bytes[0], 0, "FfiPoll::Ready discriminant should be 0");
        assert_eq!(bytes[8], 1, "ROption::RNone discriminant should be 1");

        // Ready(RSome(ROk(...))) — batch available
        let batch: FfiPoll<ROption<FFIResult<WrappedArray>>> = FfiPoll::Ready(ROption::RSome(
            RResult::ROk(WrappedArray {
                array: unsafe { std::mem::zeroed() },
                schema: WrappedSchema(unsafe { std::mem::zeroed() }),
            }),
        ));
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &batch as *const _ as *const u8,
                std::mem::size_of_val(&batch),
            )
        };
        assert_eq!(bytes[0], 0, "FfiPoll::Ready discriminant should be 0");
        assert_eq!(bytes[8], 0, "ROption::RSome discriminant should be 0");
        assert_eq!(bytes[16], 0, "RResult::ROk discriminant should be 0");

        // Ready(RSome(RErr(...))) — error
        let error: FfiPoll<ROption<FFIResult<WrappedArray>>> = FfiPoll::Ready(ROption::RSome(
            RResult::RErr(RString::from("test error")),
        ));
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &error as *const _ as *const u8,
                std::mem::size_of_val(&error),
            )
        };
        assert_eq!(bytes[0], 0, "FfiPoll::Ready discriminant should be 0");
        assert_eq!(bytes[8], 0, "ROption::RSome discriminant should be 0");
        assert_eq!(bytes[16], 1, "RResult::RErr discriminant should be 1");
    }

    #[test]
    fn test_ffi_result_stream_discriminant_offsets() {
        // FFIResult<FFI_RecordBatchStream> = RResult<FFI_RecordBatchStream, RString>
        // discriminant at offset 0, payload at offset 8

        // For the OK variant, construct via MaybeUninit since FFI_RecordBatchStream
        // contains non-nullable function pointers that can't be zeroed.
        unsafe {
            let mut buf =
                std::mem::MaybeUninit::<FFIResult<FFI_RecordBatchStream>>::uninit();
            let ptr = buf.as_mut_ptr() as *mut u8;
            // Write ROk discriminant (0) at offset 0
            *ptr = 0;
            let ok = buf.assume_init_ref();
            let bytes = std::slice::from_raw_parts(
                ok as *const _ as *const u8,
                std::mem::size_of_val(ok),
            );
            assert_eq!(bytes[0], 0, "RResult::ROk discriminant should be 0");
        }

        let err: FFIResult<FFI_RecordBatchStream> = RResult::RErr(RString::from("error"));
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &err as *const _ as *const u8,
                std::mem::size_of_val(&err),
            )
        };
        assert_eq!(bytes[0], 1, "RResult::RErr discriminant should be 1");
    }

    #[test]
    fn test_rstring_helper() {
        let msg = "hello world";
        let mut out: std::mem::MaybeUninit<RString> = std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_rstring(
                msg.as_ptr(),
                msg.len(),
                out.as_mut_ptr() as *mut c_void,
            );
            let rstring = out.assume_init();
            assert_eq!(rstring.as_str(), "hello world");
        }
    }

    #[test]
    fn test_empty_rvec_plan_helper() {
        let mut out: std::mem::MaybeUninit<RVec<FFI_ExecutionPlan>> =
            std::mem::MaybeUninit::uninit();
        unsafe {
            datafusion_create_empty_rvec_plan(out.as_mut_ptr() as *mut c_void);
            let rvec = out.assume_init();
            assert_eq!(rvec.len(), 0);
        }
    }
}
