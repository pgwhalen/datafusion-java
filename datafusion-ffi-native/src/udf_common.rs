//! UDF / UDAF upcall plumbing built on top of the Arrow C Data Interface helpers in
//! `arrow_ffi_util`. These helpers are specialized to the field-and-type-shaped trait methods
//! shared by `ScalarUDFImpl` and `AggregateUDFImpl` (`return_field`, `coerce_types`, …); they
//! combine the upcall-error pattern from `upcall_utils` with the Arrow-FFI export/import idioms.

use crate::arrow_ffi_util::{
    export_field_refs_as_ffi_schemas, export_types_as_ffi_schemas, import_field_from_ffi_schema,
};
use crate::upcall_utils::{do_counted_upcall, do_upcall, ErrorBuffer};
use arrow::datatypes::{DataType, FieldRef};
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::Volatility;
use prost::Message;
use std::sync::Arc;

/// Map a Diplomat `DfVolatility` to `Volatility`.
pub(crate) fn volatility_from_df(v: crate::bridge::ffi::DfVolatility) -> Volatility {
    match v {
        crate::bridge::ffi::DfVolatility::Immutable => Volatility::Immutable,
        crate::bridge::ffi::DfVolatility::Stable => Volatility::Stable,
        crate::bridge::ffi::DfVolatility::Volatile => Volatility::Volatile,
    }
}

/// Perform a "return_field"-style upcall: pass arg fields, receive a single field.
///
/// The `upcall` closure receives (arg_types_addr, arg_types_len, out_schema_addr, err_addr,
/// err_cap) and returns 0 on success, non-zero on error.
pub(crate) fn upcall_return_field<F>(
    arg_fields: &[FieldRef],
    context: &str,
    upcall: F,
) -> DFResult<FieldRef>
where
    F: FnOnce(usize, usize, usize, &ErrorBuffer) -> i32,
{
    let ffi_schemas = export_field_refs_as_ffi_schemas(arg_fields)?;
    let arg_types_addr = ffi_schemas.as_ptr() as usize;
    let arg_types_len = ffi_schemas.len();

    let mut out_schema = FFI_ArrowSchema::empty();
    let out_schema_addr = &mut out_schema as *mut FFI_ArrowSchema as usize;

    do_upcall(context, |err: &ErrorBuffer| {
        upcall(arg_types_addr, arg_types_len, out_schema_addr, err)
    })?;

    Ok(Arc::new(import_field_from_ffi_schema(&out_schema)?))
}

/// Perform a "coerce_types"-style upcall: pass arg types, receive coerced types.
///
/// The `upcall` closure receives (arg_types_addr, arg_types_len, result_addr, result_cap,
/// err_addr, err_cap) and returns the count written on success, negative on error.
pub(crate) fn upcall_coerce_types<F>(
    arg_types: &[DataType],
    context: &str,
    upcall: F,
) -> DFResult<Vec<DataType>>
where
    F: FnOnce(usize, usize, usize, usize, &ErrorBuffer) -> i32,
{
    let ffi_schemas = export_types_as_ffi_schemas(arg_types)?;
    let arg_types_addr = ffi_schemas.as_ptr() as usize;
    let arg_types_len = ffi_schemas.len();

    let result_cap = arg_types.len();
    let mut result_schemas: Vec<FFI_ArrowSchema> =
        (0..result_cap).map(|_| FFI_ArrowSchema::empty()).collect();
    let result_addr = result_schemas.as_mut_ptr() as usize;

    let count = do_counted_upcall(context, |err: &ErrorBuffer| {
        upcall(arg_types_addr, arg_types_len, result_addr, result_cap, err)
    })?;

    result_schemas[..count]
        .iter()
        .map(|schema| import_field_from_ffi_schema(schema).map(|f| f.data_type().clone()))
        .collect()
}

/// Decode a single `ScalarValue` from protobuf bytes.
pub(crate) fn decode_scalar_value(bytes: &[u8]) -> DFResult<ScalarValue> {
    let proto = datafusion_proto::protobuf::ScalarValue::decode(bytes).map_err(|e| {
        DataFusionError::External(format!("Failed to decode ScalarValue proto: {}", e).into())
    })?;
    (&proto).try_into().map_err(
        |e: datafusion_proto::protobuf::FromProtoError| {
            DataFusionError::External(
                format!("Failed to convert ScalarValue from proto: {}", e).into(),
            )
        },
    )
}
