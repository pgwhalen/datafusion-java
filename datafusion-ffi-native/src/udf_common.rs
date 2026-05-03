//! Shared helpers for UDF and UDAF FFI implementations.

use crate::upcall_utils::{do_counted_upcall, do_upcall, ErrorBuffer};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema};
use arrow::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::Volatility;
use prost::Message;
use std::sync::Arc;

/// Read a function name via a `name_to(buf_addr, buf_cap) -> bytes_written` callback.
pub(crate) fn read_name_via_upcall<F>(callback: F, fallback: &str) -> String
where
    F: FnOnce(usize, usize) -> i64,
{
    let cap: usize = 1024;
    let mut buf = vec![0u8; cap];
    let written = callback(buf.as_mut_ptr() as usize, cap);
    if written > 0 {
        String::from_utf8_lossy(&buf[..written as usize]).to_string()
    } else {
        fallback.to_string()
    }
}

/// Map integer volatility (0=Immutable, 1=Stable, 2=Volatile) to `Volatility`.
pub(crate) fn volatility_from_i32(v: i32) -> Volatility {
    match v {
        1 => Volatility::Stable,
        2 => Volatility::Volatile,
        _ => Volatility::Immutable,
    }
}

/// Export a single field as an `FFI_ArrowSchema`, wrapped in a parent Schema (format "+s")
/// so the Java side can import it uniformly via `Data.importSchema`.
pub(crate) fn export_field_as_ffi_schema(field: &Field) -> DFResult<FFI_ArrowSchema> {
    let schema = ArrowSchema::new(vec![field.clone()]);
    FFI_ArrowSchema::try_from(&schema)
        .map_err(|e| DataFusionError::External(format!("Failed to export field: {}", e).into()))
}

/// Export a list of field refs as a vector of `FFI_ArrowSchema`s.
pub(crate) fn export_field_refs_as_ffi_schemas(
    fields: &[FieldRef],
) -> DFResult<Vec<FFI_ArrowSchema>> {
    fields
        .iter()
        .map(|f| export_field_as_ffi_schema(f.as_ref()))
        .collect()
}

/// Export a list of data types as `FFI_ArrowSchema`s by wrapping each in an anonymous field.
pub(crate) fn export_types_as_ffi_schemas(types: &[DataType]) -> DFResult<Vec<FFI_ArrowSchema>> {
    types
        .iter()
        .map(|dt| export_field_as_ffi_schema(&Field::new("", dt.clone(), true)))
        .collect()
}

/// Import a `Field` from an `FFI_ArrowSchema` (unwrapping the Schema wrapper).
pub(crate) fn import_field_from_ffi_schema(ffi: &FFI_ArrowSchema) -> DFResult<Field> {
    let schema = ArrowSchema::try_from(ffi)
        .map_err(|e| DataFusionError::External(format!("Failed to import field: {}", e).into()))?;
    Ok(schema.field(0).clone())
}

/// Export Arrow arrays as `(FFI_ArrowArray, FFI_ArrowSchema)` pairs for FFI.
pub(crate) fn export_arrays_as_ffi(
    arrays: &[ArrayRef],
) -> DFResult<Vec<(FFI_ArrowArray, FFI_ArrowSchema)>> {
    arrays
        .iter()
        .map(|array| {
            to_ffi(&array.to_data()).map_err(|e| {
                DataFusionError::External(format!("Failed to export array: {}", e).into())
            })
        })
        .collect()
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
