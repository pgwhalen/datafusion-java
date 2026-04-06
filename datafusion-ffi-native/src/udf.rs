use crate::bridge::ffi::DfScalarUdfTrait;
use crate::upcall_utils::{do_counted_upcall, do_upcall, ErrorBuffer};
use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema as ArrowSchema};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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
        // Read name from the trait
        let cap: usize = 1024;
        let mut buf = vec![0u8; cap];
        let buf_addr = buf.as_mut_ptr() as usize;
        let written = inner.name_to(buf_addr, cap);
        let name = if written > 0 {
            String::from_utf8_lossy(&buf[..written as usize]).to_string()
        } else {
            "unknown_udf".to_string()
        };

        // Map volatility
        let volatility = match inner.volatility() {
            1 => Volatility::Stable,
            2 => Volatility::Volatile,
            _ => Volatility::Immutable,
        };

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
        // Delegate to return_field_from_args
        Err(DataFusionError::Internal(
            "return_type should not be called; return_field_from_args is implemented".into(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        // Export each arg field as FFI_ArrowSchema
        let arg_fields: Vec<&Field> = args.arg_fields.iter().map(|f| f.as_ref()).collect();
        let mut ffi_schemas: Vec<FFI_ArrowSchema> = Vec::with_capacity(arg_fields.len());
        for field in &arg_fields {
            let schema = ArrowSchema::new(vec![(*field).clone()]);
            let ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|e| {
                DataFusionError::External(format!("Failed to export arg field: {}", e).into())
            })?;
            ffi_schemas.push(ffi_schema);
        }
        let arg_types_addr = ffi_schemas.as_ptr() as usize;
        let arg_types_len = ffi_schemas.len();

        // Allocate output schema
        let mut out_schema = FFI_ArrowSchema::empty();
        let out_schema_addr = &mut out_schema as *mut FFI_ArrowSchema as usize;

        do_upcall("Java return_field failed", |err: &ErrorBuffer| {
            self.inner
                .return_field(arg_types_addr, arg_types_len, out_schema_addr, err.addr(), err.cap())
        })?;

        // Import the result schema
        let schema = ArrowSchema::try_from(&out_schema).map_err(|e| {
            DataFusionError::External(format!("Failed to import return field: {}", e).into())
        })?;
        let field = schema.field(0).clone();
        Ok(Arc::new(field))
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

        // Export each array as (FFI_ArrowArray, FFI_ArrowSchema) pair
        let mut ffi_pairs: Vec<(FFI_ArrowArray, FFI_ArrowSchema)> = Vec::with_capacity(num_args);
        for array in &arrays {
            let data = array.to_data();
            let (ffi_array, ffi_schema) = to_ffi(&data).map_err(|e| {
                DataFusionError::External(format!("Failed to export array: {}", e).into())
            })?;
            ffi_pairs.push((ffi_array, ffi_schema));
        }
        let args_addr = ffi_pairs.as_ptr() as usize;

        // Export arg fields
        let mut ffi_arg_schemas: Vec<FFI_ArrowSchema> = Vec::with_capacity(num_args);
        for field in &args.arg_fields {
            let schema = ArrowSchema::new(vec![field.as_ref().clone()]);
            let ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|e| {
                DataFusionError::External(format!("Failed to export arg field: {}", e).into())
            })?;
            ffi_arg_schemas.push(ffi_schema);
        }
        let arg_fields_addr = ffi_arg_schemas.as_ptr() as usize;

        // Export return field
        let return_schema = ArrowSchema::new(vec![args.return_field.as_ref().clone()]);
        let ffi_return = FFI_ArrowSchema::try_from(&return_schema).map_err(|e| {
            DataFusionError::External(format!("Failed to export return field: {}", e).into())
        })?;
        let return_field_addr = &ffi_return as *const FFI_ArrowSchema as usize;

        // Allocate output
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

        // Import result
        let array_data = unsafe { from_ffi(out_array, &out_schema) }.map_err(|e| {
            DataFusionError::External(format!("Failed to import result array: {}", e).into())
        })?;
        let struct_array = StructArray::from(array_data);
        // The result is wrapped in a struct, extract the first column
        let result_array = struct_array.column(0).clone();
        Ok(ColumnarValue::Array(result_array))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        // Export each arg type as FFI_ArrowSchema
        let mut ffi_schemas: Vec<FFI_ArrowSchema> = Vec::with_capacity(arg_types.len());
        for dt in arg_types {
            let field = Field::new("", dt.clone(), true);
            let schema = ArrowSchema::new(vec![field]);
            let ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|e| {
                DataFusionError::External(format!("Failed to export arg type: {}", e).into())
            })?;
            ffi_schemas.push(ffi_schema);
        }
        let arg_types_addr = ffi_schemas.as_ptr() as usize;
        let arg_types_len = ffi_schemas.len();

        // Result buffer for FFI_ArrowSchema
        let result_cap = arg_types.len();
        let mut result_schemas: Vec<FFI_ArrowSchema> =
            (0..result_cap).map(|_| FFI_ArrowSchema::empty()).collect();
        let result_addr = result_schemas.as_mut_ptr() as usize;

        let count = do_counted_upcall("Java coerce_types failed", |err: &ErrorBuffer| {
            self.inner
                .coerce_types(arg_types_addr, arg_types_len, result_addr, result_cap, err.addr(), err.cap())
        })?;
        let mut coerced = Vec::with_capacity(count);
        for schema in &result_schemas[..count] {
            let arrow_schema = ArrowSchema::try_from(schema).map_err(|e| {
                DataFusionError::External(
                    format!("Failed to import coerced type: {}", e).into(),
                )
            })?;
            coerced.push(arrow_schema.field(0).data_type().clone());
        }
        Ok(coerced)
    }
}
