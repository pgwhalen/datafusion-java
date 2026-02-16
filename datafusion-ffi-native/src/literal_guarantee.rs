//! FFI bindings for LiteralGuarantee analysis.

use datafusion::common::ScalarValue;
use datafusion::physical_expr::utils::{Guarantee, LiteralGuarantee};
use datafusion::physical_plan::PhysicalExpr;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::{clear_error, set_error_return, set_error_return_null};
use crate::scalar_value::scalar_to_proto_bytes;
use crate::table_reference::table_reference_to_proto_bytes;

/// Holds the results of LiteralGuarantee::analyze() for indexed access from Java.
struct GuaranteesHandle {
    guarantees: Vec<LiteralGuarantee>,
    /// Indexed literals for each guarantee (deterministic ordering).
    indexed_literals: Vec<Vec<ScalarValue>>,
    /// Pre-serialized proto bytes for each literal in each guarantee.
    serialized_literals: Vec<Vec<Vec<u8>>>,
    /// Pre-serialized proto bytes for each guarantee's table reference.
    table_ref_protos: Vec<Vec<u8>>,
}

/// Analyze a physical expression for literal guarantees.
///
/// Returns a GuaranteesHandle pointer and writes the count to `count_out`.
///
/// # Safety
/// - `expr` must be a valid pointer to an `Arc<dyn PhysicalExpr>`
/// - `count_out` must be a valid pointer to a usize
#[no_mangle]
pub unsafe extern "C" fn datafusion_literal_guarantee_analyze(
    expr: *mut c_void,
    count_out: *mut usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if expr.is_null() {
        return set_error_return_null(error_out, "PhysicalExpr is null");
    }
    if count_out.is_null() {
        return set_error_return_null(error_out, "count_out is null");
    }

    let physical_expr = &*(expr as *const Arc<dyn PhysicalExpr>);
    let guarantees = LiteralGuarantee::analyze(physical_expr);

    let count = guarantees.len();
    *count_out = count;

    if count == 0 {
        // Return a non-null but empty handle
        let handle = Box::new(GuaranteesHandle {
            guarantees: Vec::new(),
            indexed_literals: Vec::new(),
            serialized_literals: Vec::new(),
            table_ref_protos: Vec::new(),
        });
        return Box::into_raw(handle) as *mut c_void;
    }

    // Build indexed literals for deterministic access
    let indexed_literals: Vec<Vec<ScalarValue>> = guarantees
        .iter()
        .map(|g| {
            let mut v: Vec<ScalarValue> = g.literals.iter().cloned().collect();
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            v
        })
        .collect();

    // Pre-serialize all literals to proto bytes
    let serialized_literals: Vec<Vec<Vec<u8>>> = indexed_literals
        .iter()
        .map(|literals| {
            literals
                .iter()
                .map(|sv| {
                    scalar_to_proto_bytes(sv)
                        .unwrap_or_else(|_| Vec::new())
                })
                .collect()
        })
        .collect();

    // Pre-serialize all table references to proto bytes
    let table_ref_protos: Vec<Vec<u8>> = guarantees
        .iter()
        .map(|g| table_reference_to_proto_bytes(&g.column.relation))
        .collect();

    let handle = Box::new(GuaranteesHandle {
        guarantees,
        indexed_literals,
        serialized_literals,
        table_ref_protos,
    });
    Box::into_raw(handle) as *mut c_void
}

/// Get information about a specific guarantee.
///
/// # Safety
/// - `handle` must be a valid GuaranteesHandle pointer
/// - All output pointers must be valid
#[no_mangle]
pub unsafe extern "C" fn datafusion_guarantee_get_info(
    handle: *mut c_void,
    idx: usize,
    name_out: *mut *const u8,
    name_len_out: *mut usize,
    rel_proto_out: *mut *const u8,
    rel_proto_len_out: *mut usize,
    guarantee_type_out: *mut i32,
    literal_count_out: *mut usize,
    spans_count_out: *mut usize,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if handle.is_null() {
        return set_error_return(error_out, "GuaranteesHandle is null");
    }

    let h = &*(handle as *const GuaranteesHandle);
    if idx >= h.guarantees.len() {
        return set_error_return(error_out, &format!("Index {} out of bounds", idx));
    }

    let g = &h.guarantees[idx];

    // Column name (borrowed from the guarantee)
    let name = &g.column.name;
    *name_out = name.as_ptr();
    *name_len_out = name.len();

    // Table reference (pre-serialized proto bytes, borrowed from handle)
    let proto_bytes = &h.table_ref_protos[idx];
    if proto_bytes.is_empty() {
        *rel_proto_out = std::ptr::null();
        *rel_proto_len_out = 0;
    } else {
        *rel_proto_out = proto_bytes.as_ptr();
        *rel_proto_len_out = proto_bytes.len();
    }

    // Guarantee type
    *guarantee_type_out = match g.guarantee {
        Guarantee::In => 0,
        Guarantee::NotIn => 1,
    };

    // Literal count
    *literal_count_out = h.indexed_literals[idx].len();

    // Spans count
    *spans_count_out = g.column.spans.0.len();

    0
}

/// Get a span for a specific guarantee.
///
/// # Safety
/// - `handle` must be a valid GuaranteesHandle pointer
/// - All output pointers must be valid
#[no_mangle]
pub unsafe extern "C" fn datafusion_guarantee_get_span(
    handle: *mut c_void,
    g_idx: usize,
    s_idx: usize,
    start_line_out: *mut u64,
    start_col_out: *mut u64,
    end_line_out: *mut u64,
    end_col_out: *mut u64,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if handle.is_null() {
        return set_error_return(error_out, "GuaranteesHandle is null");
    }

    let h = &*(handle as *const GuaranteesHandle);
    if g_idx >= h.guarantees.len() {
        return set_error_return(error_out, &format!("Guarantee index {} out of bounds", g_idx));
    }

    let spans = &h.guarantees[g_idx].column.spans.0;
    if s_idx >= spans.len() {
        return set_error_return(error_out, &format!("Span index {} out of bounds", s_idx));
    }

    let span = &spans[s_idx];
    *start_line_out = span.start.line as u64;
    *start_col_out = span.start.column as u64;
    *end_line_out = span.end.line as u64;
    *end_col_out = span.end.column as u64;

    0
}

/// Get proto-serialized bytes for a literal value of a specific guarantee.
///
/// # Safety
/// - `handle` must be a valid GuaranteesHandle pointer
/// - `bytes_out` must be a valid pointer to write a `*const u8`
/// - `len_out` must be a valid pointer to write a `usize`
#[no_mangle]
pub unsafe extern "C" fn datafusion_guarantee_get_literal(
    handle: *mut c_void,
    g_idx: usize,
    l_idx: usize,
    bytes_out: *mut *const u8,
    len_out: *mut usize,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if handle.is_null() {
        return set_error_return(error_out, "GuaranteesHandle is null");
    }
    if bytes_out.is_null() {
        return set_error_return(error_out, "bytes_out is null");
    }
    if len_out.is_null() {
        return set_error_return(error_out, "len_out is null");
    }

    let h = &*(handle as *const GuaranteesHandle);
    if g_idx >= h.serialized_literals.len() {
        return set_error_return(error_out, &format!("Guarantee index {} out of bounds", g_idx));
    }

    let literals = &h.serialized_literals[g_idx];
    if l_idx >= literals.len() {
        return set_error_return(error_out, &format!("Literal index {} out of bounds", l_idx));
    }

    let bytes = &literals[l_idx];
    *bytes_out = bytes.as_ptr();
    *len_out = bytes.len();

    0
}

/// Destroy a GuaranteesHandle.
///
/// # Safety
/// The pointer must have been created by `datafusion_literal_guarantee_analyze`.
#[no_mangle]
pub unsafe extern "C" fn datafusion_guarantees_destroy(handle: *mut c_void) {
    if !handle.is_null() {
        drop(Box::from_raw(handle as *mut GuaranteesHandle));
    }
}
