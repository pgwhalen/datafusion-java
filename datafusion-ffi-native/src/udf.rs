use crate::bridge::ffi::DfScalarUdfTrait;
use crate::udf_common::{
    export_field_as_ffi_schema, export_field_refs_as_ffi_schemas, read_name_via_upcall,
    upcall_coerce_types, upcall_return_field, volatility_from_i32,
};
use crate::upcall_utils::{do_upcall, ErrorBuffer};
use arrow::array::StructArray;
use arrow::datatypes::{DataType, FieldRef};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use std::fmt;
use std::sync::Arc;

// ── ForeignDfUdf ──

pub struct ForeignDfUdf<T: DfScalarUdfTrait> {
    inner: T,
    name: String,
    signature: Signature,
}

impl<T: DfScalarUdfTrait> ForeignDfUdf<T> {
    pub fn new(inner: T) -> Self {
        let name = read_name_via_upcall(|addr, cap| inner.name_to(addr, cap), "unknown_udf");
        let volatility = volatility_from_i32(inner.volatility());
        let signature = Signature::variadic_any(volatility);

        Self {
            inner,
            name,
            signature,
        }
    }
}

impl<T: DfScalarUdfTrait> fmt::Debug for ForeignDfUdf<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfUdf")
            .field("name", &self.name)
            .finish()
    }
}

impl<T: DfScalarUdfTrait> PartialEq for ForeignDfUdf<T> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<T: DfScalarUdfTrait> Eq for ForeignDfUdf<T> {}

impl<T: DfScalarUdfTrait> std::hash::Hash for ForeignDfUdf<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

unsafe impl<T: DfScalarUdfTrait> Send for ForeignDfUdf<T> {}
unsafe impl<T: DfScalarUdfTrait> Sync for ForeignDfUdf<T> {}

impl<T: DfScalarUdfTrait + 'static> ScalarUDFImpl for ForeignDfUdf<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Err(DataFusionError::Internal(
            "return_type should not be called; return_field_from_args is implemented".into(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        upcall_return_field(
            args.arg_fields,
            "Java return_field failed",
            |arg_addr, arg_len, out_addr, err| {
                self.inner
                    .return_field(arg_addr, arg_len, out_addr, err.addr(), err.cap())
            },
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let num_rows = args.number_rows;

        // Convert ColumnarValue args to arrays and export via FFI
        let arrays: Vec<arrow::array::ArrayRef> = args
            .args
            .iter()
            .map(|cv| match cv {
                ColumnarValue::Array(a) => Arc::clone(a),
                ColumnarValue::Scalar(s) => s.to_array_of_size(num_rows).unwrap(),
            })
            .collect();

        let num_args = arrays.len();
        let ffi_pairs = crate::udf_common::export_arrays_as_ffi(&arrays)?;
        let args_addr = ffi_pairs.as_ptr() as usize;

        let ffi_arg_schemas = export_field_refs_as_ffi_schemas(&args.arg_fields)?;
        let arg_fields_addr = ffi_arg_schemas.as_ptr() as usize;

        let ffi_return = export_field_as_ffi_schema(args.return_field.as_ref())?;
        let return_field_addr = &ffi_return as *const FFI_ArrowSchema as usize;

        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let out_array_addr = &mut out_array as *mut FFI_ArrowArray as usize;
        let out_schema_addr = &mut out_schema as *mut FFI_ArrowSchema as usize;

        do_upcall("Java invoke failed", |err: &ErrorBuffer| {
            self.inner.invoke(
                args_addr,
                num_args,
                num_rows,
                arg_fields_addr,
                return_field_addr,
                out_array_addr,
                out_schema_addr,
                err.addr(),
                err.cap(),
            )
        })?;

        // Import result (wrapped in a struct, extract column 0)
        let array_data = unsafe { from_ffi(out_array, &out_schema) }.map_err(|e| {
            DataFusionError::External(format!("Failed to import result array: {}", e).into())
        })?;
        let struct_array = StructArray::from(array_data);
        Ok(ColumnarValue::Array(struct_array.column(0).clone()))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        upcall_coerce_types(
            arg_types,
            "Java coerce_types failed",
            |arg_addr, arg_len, res_addr, res_cap, err| {
                self.inner.coerce_types(
                    arg_addr,
                    arg_len,
                    res_addr,
                    res_cap,
                    err.addr(),
                    err.cap(),
                )
            },
        )
    }
}
