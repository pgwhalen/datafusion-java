//! FFI bindings for LiteralGuarantee analysis.

use datafusion::common::ScalarValue;
use datafusion::physical_expr::utils::{Guarantee, LiteralGuarantee};
use datafusion::physical_plan::PhysicalExpr;
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::error::{clear_error, set_error_return, set_error_return_null};
use crate::scalar_value::{scalar_to_ffi, FFI_ScalarValue};
use crate::table_reference::write_table_reference;

/// Holds the results of LiteralGuarantee::analyze() for indexed access from Java.
struct GuaranteesHandle {
    guarantees: Vec<LiteralGuarantee>,
    /// Indexed literals for each guarantee (deterministic ordering).
    indexed_literals: Vec<Vec<ScalarValue>>,
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

    let handle = Box::new(GuaranteesHandle {
        guarantees,
        indexed_literals,
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
    rel_type_out: *mut i32,
    rel_strs_out: *mut *const c_char,
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

    // Table reference
    write_table_reference(&g.column.relation, rel_type_out, rel_strs_out);

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

/// Get a literal value for a specific guarantee.
///
/// # Safety
/// - `handle` must be a valid GuaranteesHandle pointer
/// - `scalar_out` must point to an FFI_ScalarValue-sized allocation
#[no_mangle]
pub unsafe extern "C" fn datafusion_guarantee_get_literal(
    handle: *mut c_void,
    g_idx: usize,
    l_idx: usize,
    scalar_out: *mut FFI_ScalarValue,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if handle.is_null() {
        return set_error_return(error_out, "GuaranteesHandle is null");
    }
    if scalar_out.is_null() {
        return set_error_return(error_out, "scalar_out is null");
    }

    let h = &*(handle as *const GuaranteesHandle);
    if g_idx >= h.indexed_literals.len() {
        return set_error_return(error_out, &format!("Guarantee index {} out of bounds", g_idx));
    }

    let literals = &h.indexed_literals[g_idx];
    if l_idx >= literals.len() {
        return set_error_return(error_out, &format!("Literal index {} out of bounds", l_idx));
    }

    scalar_to_ffi(&literals[l_idx], &mut *scalar_out);
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
