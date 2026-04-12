use crate::bridge::ffi::{DfExprBytes, DfStringArray, DfVarProviderTrait};
use crate::upcall_utils::{do_optional_upcall, do_returning_upcall, ErrorBuffer};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::variable::VarProvider;
use prost::Message;
use std::fmt;

// ── ForeignVarProvider ──

pub struct ForeignVarProvider<T: DfVarProviderTrait> {
    inner: T,
}

impl<T: DfVarProviderTrait> ForeignVarProvider<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfVarProviderTrait> fmt::Debug for ForeignVarProvider<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignVarProvider").finish()
    }
}

unsafe impl<T: DfVarProviderTrait> Send for ForeignVarProvider<T> {}
unsafe impl<T: DfVarProviderTrait> Sync for ForeignVarProvider<T> {}

impl<T: DfVarProviderTrait + 'static> VarProvider for ForeignVarProvider<T> {
    fn get_value(&self, var_names: Vec<String>) -> DFResult<ScalarValue> {
        // Pass var_names to Java as DfStringArray raw ptr (Java takes ownership)
        let arr = Box::new(DfStringArray {
            strings: var_names,
        });
        let var_names_ptr = Box::into_raw(arr) as usize;

        // Java returns a DfExprBytes raw pointer containing proto bytes
        let boxed = do_returning_upcall::<DfExprBytes>(
            "Java VarProvider.getValue() failed",
            Box::new(|err: &ErrorBuffer| {
                self.inner
                    .get_value(var_names_ptr, err.addr(), err.cap())
            }),
        )?;
        let bytes = DfExprBytes::take_from_raw(Box::into_raw(boxed) as usize);

        // Deserialize proto bytes to ScalarValue
        let proto = datafusion_proto::protobuf::ScalarValue::decode(bytes.as_slice()).map_err(
            |e| {
                DataFusionError::External(
                    format!("Failed to decode ScalarValue proto: {}", e).into(),
                )
            },
        )?;
        let scalar: ScalarValue = (&proto).try_into().map_err(
            |e: datafusion_proto::protobuf::FromProtoError| {
                DataFusionError::External(
                    format!("Failed to convert ScalarValue from proto: {}", e).into(),
                )
            },
        )?;
        Ok(scalar)
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        // Pass var_names to Java as DfStringArray raw ptr (Java takes ownership)
        let arr = Box::new(DfStringArray {
            strings: var_names.to_vec(),
        });
        let var_names_ptr = Box::into_raw(arr) as usize;

        // Allocate FFI_ArrowSchema for output
        let mut out_schema = FFI_ArrowSchema::empty();
        let out_schema_addr = &mut out_schema as *mut FFI_ArrowSchema as usize;

        let found = do_optional_upcall(
            "Java VarProvider.getType() failed",
            |err: &ErrorBuffer| {
                self.inner
                    .get_type(var_names_ptr, out_schema_addr, err.addr(), err.cap())
            },
        )
        .ok()?;

        if !found {
            return None;
        }

        // Import schema and extract DataType from first field
        let schema = ArrowSchema::try_from(&out_schema).ok()?;
        Some(schema.field(0).data_type().clone())
    }
}
