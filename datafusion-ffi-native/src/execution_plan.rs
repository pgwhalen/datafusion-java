//! Size helpers and construction helpers for FFI_ExecutionPlan and related types.
//!
//! Java constructs `FFI_ExecutionPlan` and `FFI_PlanProperties` directly in
//! Java arena memory. This module provides size validation and construction
//! helpers for those types.

use std::ffi::c_void;
use std::sync::Arc;

use abi_stable::std_types::RVec;
use arrow::datatypes::Schema;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::PlanProperties;
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
// Size and layout helpers
// ============================================================================

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

// FFI_Partitioning, FFI_EmissionType, FFI_Boundedness, and FFI_PhysicalSortExpr
// are in pub(crate) modules of datafusion-ffi and cannot be named from outside
// the crate. We use type inference on FFI_PlanProperties function pointer fields
// to extract their return type sizes without naming the types directly.

/// Extract the size of function pointer return type `R` via type inference.
fn fn_return_size<A, R>(_: unsafe extern "C" fn(&A) -> R) -> usize {
    std::mem::size_of::<R>()
}

/// Create a dummy `FFI_PlanProperties` for extracting callback return type sizes.
/// The caller must ensure it is properly dropped (it implements Drop via release).
fn make_dummy_ffi_plan_properties(
    emission: EmissionType,
    bounded: Boundedness,
    partitioning_count: usize,
) -> FFI_PlanProperties {
    let schema = Arc::new(Schema::empty());
    let eq = EquivalenceProperties::new(schema);
    let props = PlanProperties::new(
        eq,
        Partitioning::UnknownPartitioning(partitioning_count),
        emission,
        bounded,
    );
    FFI_PlanProperties::from(&props)
}

/// Return sizeof(FFI_Partitioning) for Java validation.
///
/// Uses type inference on `FFI_PlanProperties::output_partitioning` return type.
#[no_mangle]
pub extern "C" fn datafusion_ffi_partitioning_size() -> usize {
    let props = make_dummy_ffi_plan_properties(
        EmissionType::Incremental,
        Boundedness::Bounded,
        1,
    );
    fn_return_size(props.output_partitioning)
    // props drops here -> release callback fires
}

/// Return sizeof(FFI_EmissionType) for Java validation.
///
/// Uses type inference on `FFI_PlanProperties::emission_type` return type.
#[no_mangle]
pub extern "C" fn datafusion_ffi_emission_type_size() -> usize {
    let props = make_dummy_ffi_plan_properties(
        EmissionType::Incremental,
        Boundedness::Bounded,
        1,
    );
    fn_return_size(props.emission_type)
}

/// Return sizeof(FFI_Boundedness) for Java validation.
///
/// Uses type inference on `FFI_PlanProperties::boundedness` return type.
#[no_mangle]
pub extern "C" fn datafusion_ffi_boundedness_size() -> usize {
    let props = make_dummy_ffi_plan_properties(
        EmissionType::Incremental,
        Boundedness::Bounded,
        1,
    );
    fn_return_size(props.boundedness)
}

/// Return sizeof(ROption<RVec<FFI_PhysicalSortExpr>>) for Java validation.
///
/// Uses type inference on `FFI_PlanProperties::output_ordering` return type.
#[no_mangle]
pub extern "C" fn datafusion_roption_rvec_sort_expr_size() -> usize {
    let props = make_dummy_ffi_plan_properties(
        EmissionType::Incremental,
        Boundedness::Bounded,
        1,
    );
    fn_return_size(props.output_ordering)
}

// ============================================================================
// Construction helpers
// ============================================================================

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use abi_stable::std_types::{ROption, RResult, RString};
    use async_ffi::FfiPoll;
    use datafusion_ffi::arrow_wrappers::{WrappedArray, WrappedSchema};

    #[test]
    fn test_ffi_execution_plan_sizes() {
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
    fn test_plan_properties_callback_return_sizes() {
        let props = make_dummy_ffi_plan_properties(
            EmissionType::Incremental,
            Boundedness::Bounded,
            1,
        );

        // FFI_Partitioning: repr(C) enum, tag(4) + pad(4) + largest payload = 48
        assert_eq!(
            fn_return_size(props.output_partitioning),
            48,
            "FFI_Partitioning size"
        );

        // FFI_EmissionType: repr(C) unit enum = c_int = 4 bytes
        assert_eq!(
            fn_return_size(props.emission_type),
            4,
            "FFI_EmissionType size"
        );

        // FFI_Boundedness: repr(C) enum, tag(4) + bool(1) + pad(3) = 8 bytes
        assert_eq!(
            fn_return_size(props.boundedness),
            8,
            "FFI_Boundedness size"
        );

        // ROption<RVec<FFI_PhysicalSortExpr>>: disc(1) + pad(7) + RVec(32) = 40 bytes
        assert_eq!(
            fn_return_size(props.output_ordering),
            40,
            "ROption<RVec<FFI_PhysicalSortExpr>> size"
        );
    }

    #[test]
    fn test_ffi_partitioning_layout() {
        let props = make_dummy_ffi_plan_properties(
            EmissionType::Incremental,
            Boundedness::Bounded,
            42,
        );
        let part = unsafe { (props.output_partitioning)(&props) };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &part as *const _ as *const u8,
                std::mem::size_of_val(&part),
            )
        };
        // Discriminant is a c_int (i32) at offset 0
        let disc = i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(disc, 2, "UnknownPartitioning discriminant should be 2");

        // Payload (usize) at offset 8
        let payload_bytes: [u8; 8] = bytes[8..16].try_into().unwrap();
        let payload = usize::from_ne_bytes(payload_bytes);
        assert_eq!(payload, 42, "UnknownPartitioning payload should be 42");
    }

    #[test]
    fn test_ffi_emission_type_discriminants() {
        let check = |emission: EmissionType, expected: i32, name: &str| {
            let props = make_dummy_ffi_plan_properties(emission, Boundedness::Bounded, 1);
            let et = unsafe { (props.emission_type)(&props) };
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    &et as *const _ as *const u8,
                    std::mem::size_of_val(&et),
                )
            };
            let disc = i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            assert_eq!(disc, expected, "{name} discriminant");
        };

        check(EmissionType::Incremental, 0, "Incremental");
        check(EmissionType::Final, 1, "Final");
        check(EmissionType::Both, 2, "Both");
    }

    #[test]
    fn test_ffi_boundedness_layout() {
        // Bounded: discriminant = 0
        let props =
            make_dummy_ffi_plan_properties(EmissionType::Incremental, Boundedness::Bounded, 1);
        let bounded = unsafe { (props.boundedness)(&props) };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &bounded as *const _ as *const u8,
                std::mem::size_of_val(&bounded),
            )
        };
        let disc = i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(disc, 0, "Bounded discriminant should be 0");

        // Unbounded: discriminant = 1, requires_infinite_memory (bool) at offset 4
        let props = make_dummy_ffi_plan_properties(
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
            1,
        );
        let unbounded = unsafe { (props.boundedness)(&props) };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &unbounded as *const _ as *const u8,
                std::mem::size_of_val(&unbounded),
            )
        };
        let disc = i32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(disc, 1, "Unbounded discriminant should be 1");
        assert_eq!(bytes[4], 0, "requires_infinite_memory=false should be 0");
    }

    #[test]
    fn test_output_ordering_rnone() {
        let props =
            make_dummy_ffi_plan_properties(EmissionType::Incremental, Boundedness::Bounded, 1);
        let ordering = unsafe { (props.output_ordering)(&props) };
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &ordering as *const _ as *const u8,
                std::mem::size_of_val(&ordering),
            )
        };
        // ROption is #[repr(u8)]: RSome=0, RNone=1
        assert_eq!(bytes[0], 1, "ROption::RNone discriminant should be 1");
    }

    #[test]
    fn test_ffi_result_stream_discriminant_offsets() {
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

        let err: FFIResult<FFI_RecordBatchStream> =
            abi_stable::std_types::RResult::RErr(abi_stable::std_types::RString::from("error"));
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &err as *const _ as *const u8,
                std::mem::size_of_val(&err),
            )
        };
        assert_eq!(bytes[0], 1, "RResult::RErr discriminant should be 1");
    }

    #[test]
    fn test_poll_result_discriminant_offsets() {
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
