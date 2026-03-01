use arrow::datatypes::Schema;
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::JoinType;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_proto::logical_plan::from_proto::{parse_exprs, parse_sorts};
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::{LogicalExprList, SortExprNodeCollection};
use prost::Message;
use std::ffi::{c_char, c_void, CStr};
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::error::{clear_error, set_error_return, set_error_return_null};

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
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);

    runtime.block_on(async {
        match dataframe.clone().execute_stream().await {
            Ok(stream) => Box::into_raw(Box::new(stream)) as *mut c_void,
            Err(e) => set_error_return_null(error_out, &format!("Execute stream failed: {}", e)),
        }
    })
}

// ============================================================================
// Expression deserialization helper
// ============================================================================

/// Deserialize a protobuf-encoded LogicalExprList into a Vec<Expr>.
///
/// Uses the SessionContext's registry for UDF resolution.
unsafe fn deserialize_exprs(
    ctx: *mut c_void,
    bytes: *const u8,
    len: usize,
) -> Result<Vec<Expr>, String> {
    if bytes.is_null() || len == 0 {
        return Ok(vec![]);
    }

    let slice = std::slice::from_raw_parts(bytes, len);
    let proto_list =
        LogicalExprList::decode(slice).map_err(|e| format!("Failed to decode proto: {}", e))?;

    let context = &*(ctx as *mut datafusion::execution::context::SessionContext);
    let codec = DefaultLogicalExtensionCodec {};

    parse_exprs(proto_list.expr.iter(), context, &codec)
        .map_err(|e| format!("Failed to parse expressions: {}", e))
}

/// Deserialize sort expressions from a SortExprNodeCollection protobuf.
unsafe fn deserialize_sort_exprs(
    ctx: *mut c_void,
    bytes: *const u8,
    len: usize,
) -> Result<Vec<SortExpr>, String> {
    if bytes.is_null() || len == 0 {
        return Ok(vec![]);
    }

    let slice = std::slice::from_raw_parts(bytes, len);
    let collection = SortExprNodeCollection::decode(slice)
        .map_err(|e| format!("Failed to decode sort expressions: {}", e))?;

    let context = &*(ctx as *mut datafusion::execution::context::SessionContext);
    let codec = DefaultLogicalExtensionCodec {};

    parse_sorts(collection.sort_expr_nodes.iter(), context, &codec)
        .map_err(|e| format!("Failed to parse sort expressions: {}", e))
}

// ============================================================================
// Phase 2: DataFrame transformations
// ============================================================================

/// Filter a DataFrame with a predicate expression.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `ctx` - Pointer to the SessionContext (for expr deserialization)
/// * `predicate_bytes` - Protobuf-encoded LogicalExprList (single expr)
/// * `predicate_len` - Length of the protobuf bytes
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_filter(
    rt: *mut c_void,
    df: *mut c_void,
    ctx: *mut c_void,
    predicate_bytes: *const u8,
    predicate_len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    let exprs = match deserialize_exprs(ctx, predicate_bytes, predicate_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    if exprs.is_empty() {
        return set_error_return_null(error_out, "Filter requires exactly one predicate expression");
    }

    match dataframe.clone().filter(exprs[0].clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Filter failed: {}", e)),
    }
}

/// Select expressions from a DataFrame.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `ctx` - Pointer to the SessionContext (for expr deserialization)
/// * `expr_bytes` - Protobuf-encoded LogicalExprList
/// * `expr_len` - Length of the protobuf bytes
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_select(
    rt: *mut c_void,
    df: *mut c_void,
    ctx: *mut c_void,
    expr_bytes: *const u8,
    expr_len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    let exprs = match deserialize_exprs(ctx, expr_bytes, expr_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    match dataframe.clone().select(exprs) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Select failed: {}", e)),
    }
}

/// Aggregate a DataFrame with grouping and aggregate expressions.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `ctx` - Pointer to the SessionContext
/// * `group_bytes` - Protobuf-encoded group-by expressions
/// * `group_len` - Length of group bytes
/// * `aggr_bytes` - Protobuf-encoded aggregate expressions
/// * `aggr_len` - Length of aggregate bytes
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_aggregate(
    rt: *mut c_void,
    df: *mut c_void,
    ctx: *mut c_void,
    group_bytes: *const u8,
    group_len: usize,
    aggr_bytes: *const u8,
    aggr_len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    let group_exprs = match deserialize_exprs(ctx, group_bytes, group_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    let aggr_exprs = match deserialize_exprs(ctx, aggr_bytes, aggr_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    match dataframe.clone().aggregate(group_exprs, aggr_exprs) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Aggregate failed: {}", e)),
    }
}

/// Sort a DataFrame by sort expressions.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `ctx` - Pointer to the SessionContext
/// * `sort_bytes` - Protobuf-encoded sort expressions
/// * `sort_len` - Length of sort bytes
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_sort(
    rt: *mut c_void,
    df: *mut c_void,
    ctx: *mut c_void,
    sort_bytes: *const u8,
    sort_len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    let sort_exprs = match deserialize_sort_exprs(ctx, sort_bytes, sort_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    match dataframe.clone().sort(sort_exprs) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Sort failed: {}", e)),
    }
}

/// Limit the number of rows in a DataFrame.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `skip` - Number of rows to skip
/// * `fetch` - Maximum number of rows to return (-1 for no limit)
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_limit(
    rt: *mut c_void,
    df: *mut c_void,
    skip: usize,
    fetch: i64,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let fetch_option = if fetch < 0 { None } else { Some(fetch as usize) };

    match dataframe.clone().limit(skip, fetch_option) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Limit failed: {}", e)),
    }
}

/// Execute a DataFrame and collect all results, then return a new DataFrame.
///
/// # Returns
/// Pointer to RecordBatchStream (like execute_stream, but all results are materialized).
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_collect(
    rt: *mut c_void,
    df: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);

    runtime.block_on(async {
        // Collect all batches
        match dataframe.clone().collect().await {
            Ok(batches) => {
                // Create an in-memory stream from the collected batches.
                // Must return a SendableRecordBatchStream (Pin<Box<dyn RecordBatchStream + Send>>)
                // to match what datafusion_stream_* functions expect.
                let schema = if batches.is_empty() {
                    Arc::new(dataframe.schema().as_arrow().as_ref().clone())
                } else {
                    batches[0].schema()
                };
                let stream = futures::stream::iter(batches.into_iter().map(Ok));
                let pinned: std::pin::Pin<
                    Box<dyn futures::Stream<Item = Result<arrow::record_batch::RecordBatch, datafusion::error::DataFusionError>> + Send>,
                > = Box::pin(stream);
                let adapter = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    schema,
                    pinned,
                );
                let sendable: SendableRecordBatchStream = Box::pin(adapter);
                Box::into_raw(Box::new(sendable)) as *mut c_void
            }
            Err(e) => set_error_return_null(error_out, &format!("Collect failed: {}", e)),
        }
    })
}

/// Execute a DataFrame, collect results, and print to stdout.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_show(
    rt: *mut c_void,
    df: *mut c_void,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);

    runtime.block_on(async {
        match dataframe.clone().show().await {
            Ok(()) => 0,
            Err(e) => set_error_return(error_out, &format!("Show failed: {}", e)),
        }
    })
}

/// Execute a DataFrame and return the row count.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `count_out` - Pointer to receive the row count
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_count(
    rt: *mut c_void,
    df: *mut c_void,
    count_out: *mut u64,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || count_out.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);

    runtime.block_on(async {
        match dataframe.clone().count().await {
            Ok(count) => {
                *count_out = count as u64;
                0
            }
            Err(e) => set_error_return(error_out, &format!("Count failed: {}", e)),
        }
    })
}

/// Get the schema of a DataFrame as an FFI_ArrowSchema.
///
/// The caller must free the returned schema by calling its release callback.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_schema(
    df: *mut c_void,
    schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if df.is_null() || schema_out.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let df_schema = dataframe.schema();
    let arrow_schema: Schema = df_schema.as_arrow().as_ref().clone();

    match FFI_ArrowSchema::try_from(&arrow_schema) {
        Ok(ffi_schema) => {
            std::ptr::write(schema_out, ffi_schema);
            0
        }
        Err(e) => set_error_return(error_out, &format!("Schema export failed: {}", e)),
    }
}

// ============================================================================
// Phase 3: Joins and set operations
// ============================================================================

/// Join two DataFrames on column names.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `left` - Pointer to the left DataFrame
/// * `right` - Pointer to the right DataFrame
/// * `join_type` - Join type (0=Inner, 1=Left, 2=Right, 3=Full, 4=LeftSemi, 5=LeftAnti, 6=RightSemi, 7=RightAnti)
/// * `left_cols` - Array of C string pointers for left join columns
/// * `right_cols` - Array of C string pointers for right join columns
/// * `num_cols` - Number of join columns
/// * `filter_bytes` - Optional protobuf-encoded filter expression (NULL if none)
/// * `filter_len` - Length of filter bytes (0 if none)
/// * `ctx` - Pointer to the SessionContext
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// Pointer to new DataFrame on success, null on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_join(
    rt: *mut c_void,
    left: *mut c_void,
    right: *mut c_void,
    join_type: i32,
    left_cols: *const *const c_char,
    right_cols: *const *const c_char,
    num_cols: usize,
    filter_bytes: *const u8,
    filter_len: usize,
    ctx: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || left.is_null() || right.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let left_df = &*(left as *mut DataFrame);
    let right_df = &*(right as *mut DataFrame);

    let jt = match int_to_join_type(join_type) {
        Some(jt) => jt,
        None => return set_error_return_null(error_out, &format!("Invalid join type: {}", join_type)),
    };

    // Read column name arrays
    let left_col_strs = match read_string_array(left_cols, num_cols) {
        Ok(v) => v,
        Err(e) => return set_error_return_null(error_out, &e),
    };
    let right_col_strs = match read_string_array(right_cols, num_cols) {
        Ok(v) => v,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    let left_refs: Vec<&str> = left_col_strs.iter().map(|s| s.as_str()).collect();
    let right_refs: Vec<&str> = right_col_strs.iter().map(|s| s.as_str()).collect();

    // Parse optional filter
    let filter = if !filter_bytes.is_null() && filter_len > 0 {
        match deserialize_exprs(ctx, filter_bytes, filter_len) {
            Ok(exprs) if !exprs.is_empty() => Some(exprs[0].clone()),
            Ok(_) => None,
            Err(e) => return set_error_return_null(error_out, &e),
        }
    } else {
        None
    };

    match left_df
        .clone()
        .join(right_df.clone(), jt, &left_refs, &right_refs, filter)
    {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Join failed: {}", e)),
    }
}

/// Join two DataFrames on arbitrary expressions.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_join_on(
    rt: *mut c_void,
    left: *mut c_void,
    right: *mut c_void,
    join_type: i32,
    on_bytes: *const u8,
    on_len: usize,
    ctx: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || left.is_null() || right.is_null() || ctx.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let left_df = &*(left as *mut DataFrame);
    let right_df = &*(right as *mut DataFrame);

    let jt = match int_to_join_type(join_type) {
        Some(jt) => jt,
        None => return set_error_return_null(error_out, &format!("Invalid join type: {}", join_type)),
    };

    let on_exprs = match deserialize_exprs(ctx, on_bytes, on_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    match left_df.clone().join_on(right_df.clone(), jt, on_exprs) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Join on failed: {}", e)),
    }
}

/// Union of two DataFrames.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_union(
    rt: *mut c_void,
    df: *mut c_void,
    other: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || other.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let other_df = &*(other as *mut DataFrame);

    match dataframe.clone().union(other_df.clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Union failed: {}", e)),
    }
}

/// Union distinct of two DataFrames.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_union_distinct(
    rt: *mut c_void,
    df: *mut c_void,
    other: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || other.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let other_df = &*(other as *mut DataFrame);

    match dataframe.clone().union_distinct(other_df.clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Union distinct failed: {}", e)),
    }
}

/// Intersect of two DataFrames.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_intersect(
    rt: *mut c_void,
    df: *mut c_void,
    other: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || other.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let other_df = &*(other as *mut DataFrame);

    match dataframe.clone().intersect(other_df.clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Intersect failed: {}", e)),
    }
}

/// Except (set difference) of two DataFrames.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_except(
    rt: *mut c_void,
    df: *mut c_void,
    other: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || other.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let other_df = &*(other as *mut DataFrame);

    match dataframe.clone().except(other_df.clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Except failed: {}", e)),
    }
}

/// Distinct rows of a DataFrame.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_distinct(
    rt: *mut c_void,
    df: *mut c_void,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    match dataframe.clone().distinct() {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Distinct failed: {}", e)),
    }
}

// ============================================================================
// Phase 5: Advanced features
// ============================================================================

/// Add or replace a column in a DataFrame.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_with_column(
    rt: *mut c_void,
    df: *mut c_void,
    ctx: *mut c_void,
    name: *const c_char,
    expr_bytes: *const u8,
    expr_len: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || ctx.is_null() || name.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return_null(error_out, &format!("Invalid column name: {}", e)),
    };

    let exprs = match deserialize_exprs(ctx, expr_bytes, expr_len) {
        Ok(e) => e,
        Err(e) => return set_error_return_null(error_out, &e),
    };

    if exprs.is_empty() {
        return set_error_return_null(error_out, "with_column requires exactly one expression");
    }

    match dataframe.clone().with_column(name_str, exprs[0].clone()) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("with_column failed: {}", e)),
    }
}

/// Rename a column in a DataFrame.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_with_column_renamed(
    rt: *mut c_void,
    df: *mut c_void,
    old_name: *const c_char,
    new_name: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || old_name.is_null() || new_name.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);
    let old_str = match CStr::from_ptr(old_name).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return_null(error_out, &format!("Invalid old name: {}", e)),
    };
    let new_str = match CStr::from_ptr(new_name).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return_null(error_out, &format!("Invalid new name: {}", e)),
    };

    match dataframe.clone().with_column_renamed(old_str, new_str) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("with_column_renamed failed: {}", e)),
    }
}

/// Drop columns from a DataFrame.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_drop_columns(
    rt: *mut c_void,
    df: *mut c_void,
    col_names: *const *const c_char,
    num_cols: usize,
    error_out: *mut *mut c_char,
) -> *mut c_void {
    clear_error(error_out);

    if rt.is_null() || df.is_null() {
        return set_error_return_null(error_out, "Null pointer argument");
    }

    let dataframe = &*(df as *mut DataFrame);

    let col_strs = match read_string_array(col_names, num_cols) {
        Ok(v) => v,
        Err(e) => return set_error_return_null(error_out, &e),
    };
    let col_refs: Vec<&str> = col_strs.iter().map(|s| s.as_str()).collect();

    match dataframe.clone().drop_columns(&col_refs) {
        Ok(new_df) => Box::into_raw(Box::new(new_df)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("drop_columns failed: {}", e)),
    }
}

// ============================================================================
// Write operations
// ============================================================================

/// Write DataFrame results to a Parquet file.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `path` - C string path to write the Parquet file to
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_write_parquet(
    rt: *mut c_void,
    df: *mut c_void,
    path: *const c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || path.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);
    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return(error_out, &format!("Invalid path: {}", e)),
    };

    runtime.block_on(async {
        match dataframe
            .clone()
            .write_parquet(path_str, Default::default(), None)
            .await
        {
            Ok(_) => 0,
            Err(e) => set_error_return(error_out, &format!("Write parquet failed: {}", e)),
        }
    })
}

/// Write DataFrame results to a CSV file.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `path` - C string path to write the CSV file to
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_write_csv(
    rt: *mut c_void,
    df: *mut c_void,
    path: *const c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || path.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);
    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return(error_out, &format!("Invalid path: {}", e)),
    };

    runtime.block_on(async {
        match dataframe
            .clone()
            .write_csv(path_str, Default::default(), None)
            .await
        {
            Ok(_) => 0,
            Err(e) => set_error_return(error_out, &format!("Write CSV failed: {}", e)),
        }
    })
}

/// Write DataFrame results to a JSON file.
///
/// # Arguments
/// * `rt` - Pointer to the Tokio runtime
/// * `df` - Pointer to the DataFrame
/// * `path` - C string path to write the JSON file to
/// * `error_out` - Pointer to receive error message
///
/// # Returns
/// 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_write_json(
    rt: *mut c_void,
    df: *mut c_void,
    path: *const c_char,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);

    if rt.is_null() || df.is_null() || path.is_null() {
        return set_error_return(error_out, "Null pointer argument");
    }

    let runtime = &*(rt as *mut Runtime);
    let dataframe = &*(df as *mut DataFrame);
    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(e) => return set_error_return(error_out, &format!("Invalid path: {}", e)),
    };

    runtime.block_on(async {
        match dataframe
            .clone()
            .write_json(path_str, Default::default(), None)
            .await
        {
            Ok(_) => 0,
            Err(e) => set_error_return(error_out, &format!("Write JSON failed: {}", e)),
        }
    })
}

// ============================================================================
// Phase 4: SessionContext readers (table reference)
// These are in session_context.rs
// ============================================================================

// ============================================================================
// Helpers
// ============================================================================

fn int_to_join_type(value: i32) -> Option<JoinType> {
    match value {
        0 => Some(JoinType::Inner),
        1 => Some(JoinType::Left),
        2 => Some(JoinType::Right),
        3 => Some(JoinType::Full),
        4 => Some(JoinType::LeftSemi),
        5 => Some(JoinType::LeftAnti),
        6 => Some(JoinType::RightSemi),
        7 => Some(JoinType::RightAnti),
        _ => None,
    }
}

unsafe fn read_string_array(ptrs: *const *const c_char, len: usize) -> Result<Vec<String>, String> {
    if len == 0 {
        return Ok(vec![]);
    }
    if ptrs.is_null() {
        return Err("Null string array pointer".to_string());
    }

    let slice = std::slice::from_raw_parts(ptrs, len);
    let mut result = Vec::with_capacity(len);
    for (i, &ptr) in slice.iter().enumerate() {
        if ptr.is_null() {
            return Err(format!("Null string at index {}", i));
        }
        match CStr::from_ptr(ptr).to_str() {
            Ok(s) => result.push(s.to_string()),
            Err(e) => return Err(format!("Invalid string at index {}: {}", i, e)),
        }
    }
    Ok(result)
}
