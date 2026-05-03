use crate::bridge::ffi::{DfAggregateUdfTrait, DfExprBytes, DfStringArray};
use crate::udf_common::{
    decode_scalar_value, export_arrays_as_ffi, export_field_as_ffi_schema, import_field_from_ffi_schema,
    upcall_coerce_types, upcall_return_field, volatility_from_i32,
};
use crate::upcall_utils::{do_counted_upcall, do_returning_upcall, do_upcall, ErrorBuffer};
use arrow::datatypes::{DataType, FieldRef};
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion_proto::protobuf::{logical_expr_node::ExprType, LogicalExprList};
use prost::Message;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfUdaf ──

pub struct ForeignDfUdaf<T: DfAggregateUdfTrait> {
    inner: Arc<T>,
    name: String,
    signature: Signature,
}

impl<T: DfAggregateUdfTrait> ForeignDfUdaf<T> {
    pub fn new(inner: T) -> Self {
        let name = DfStringArray::take_from_raw(inner.name_raw())
            .into_iter()
            .next()
            .unwrap_or_else(|| "unknown_udaf".to_string());
        let volatility = volatility_from_i32(inner.volatility());
        let signature = Signature::variadic_any(volatility);

        Self {
            inner: Arc::new(inner),
            name,
            signature,
        }
    }
}

impl<T: DfAggregateUdfTrait> fmt::Debug for ForeignDfUdaf<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfUdaf")
            .field("name", &self.name)
            .finish()
    }
}

impl<T: DfAggregateUdfTrait> PartialEq for ForeignDfUdaf<T> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl<T: DfAggregateUdfTrait> Eq for ForeignDfUdaf<T> {}

impl<T: DfAggregateUdfTrait> std::hash::Hash for ForeignDfUdaf<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

unsafe impl<T: DfAggregateUdfTrait> Send for ForeignDfUdaf<T> {}
unsafe impl<T: DfAggregateUdfTrait> Sync for ForeignDfUdaf<T> {}

impl<T: DfAggregateUdfTrait + 'static> AggregateUDFImpl for ForeignDfUdaf<T> {
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
            "return_type should not be called; return_field is implemented".into(),
        ))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> DFResult<FieldRef> {
        upcall_return_field(
            arg_fields,
            "Java UDAF return_field failed",
            |arg_addr, arg_len, out_addr, err| {
                self.inner
                    .return_field(arg_addr, arg_len, out_addr, err.addr(), err.cap())
            },
        )
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        // Java consumes the return-field schema on each upcall (the import clears the
        // source's release callback), so we re-export it for the second pass.
        let count_schema = export_field_as_ffi_schema(args.return_field.as_ref())?;
        let count = do_counted_upcall(
            "Java UDAF state_fields_count failed",
            |err: &ErrorBuffer| {
                self.inner.state_fields_count(
                    &count_schema as *const FFI_ArrowSchema as usize,
                    err.addr(),
                    err.cap(),
                )
            },
        )?;

        let mut result_schemas: Vec<FFI_ArrowSchema> =
            (0..count).map(|_| FFI_ArrowSchema::empty()).collect();
        let result_addr = result_schemas.as_mut_ptr() as usize;

        let fill_schema = export_field_as_ffi_schema(args.return_field.as_ref())?;
        do_counted_upcall("Java UDAF state_fields failed", |err: &ErrorBuffer| {
            self.inner.state_fields(
                &fill_schema as *const FFI_ArrowSchema as usize,
                result_addr,
                count,
                err.addr(),
                err.cap(),
            )
        })?;

        result_schemas
            .iter()
            .map(|schema| import_field_from_ffi_schema(schema).map(|f| Arc::new(f) as FieldRef))
            .collect()
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let err = ErrorBuffer::new();
        let acc_id = self.inner.accumulator_create(err.addr(), err.cap());
        if acc_id < 0 {
            return Err(DataFusionError::External(
                format!("Java UDAF accumulator_create failed: {}", err.read()).into(),
            ));
        }

        Ok(Box::new(ForeignAccumulator {
            inner: Arc::clone(&self.inner),
            acc_id,
        }))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        upcall_coerce_types(
            arg_types,
            "Java UDAF coerce_types failed",
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

// ── ForeignAccumulator ──

struct ForeignAccumulator<T: DfAggregateUdfTrait> {
    inner: Arc<T>,
    acc_id: i64,
}

impl<T: DfAggregateUdfTrait> fmt::Debug for ForeignAccumulator<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignAccumulator")
            .field("acc_id", &self.acc_id)
            .finish()
    }
}

unsafe impl<T: DfAggregateUdfTrait> Send for ForeignAccumulator<T> {}
unsafe impl<T: DfAggregateUdfTrait> Sync for ForeignAccumulator<T> {}

impl<T: DfAggregateUdfTrait> Drop for ForeignAccumulator<T> {
    fn drop(&mut self) {
        self.inner.accumulator_drop(self.acc_id);
    }
}

impl<T: DfAggregateUdfTrait + 'static> Accumulator for ForeignAccumulator<T> {
    fn update_batch(&mut self, values: &[arrow::array::ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }
        let ffi_pairs = export_arrays_as_ffi(values)?;
        let args_addr = ffi_pairs.as_ptr() as usize;
        let num_args = ffi_pairs.len();

        do_upcall("Java accumulator update_batch failed", |err: &ErrorBuffer| {
            self.inner
                .accumulator_update(self.acc_id, args_addr, num_args, err.addr(), err.cap())
        })
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let boxed = do_returning_upcall::<DfExprBytes>(
            "Java accumulator evaluate failed",
            Box::new(|err: &ErrorBuffer| {
                self.inner
                    .accumulator_evaluate(self.acc_id, err.addr(), err.cap())
            }),
        )?;
        let bytes = DfExprBytes::take_from_raw(Box::into_raw(boxed) as usize);
        decode_scalar_value(bytes.as_slice())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let boxed = do_returning_upcall::<DfExprBytes>(
            "Java accumulator state failed",
            Box::new(|err: &ErrorBuffer| {
                self.inner
                    .accumulator_state(self.acc_id, err.addr(), err.cap())
            }),
        )?;
        let bytes = DfExprBytes::take_from_raw(Box::into_raw(boxed) as usize);
        decode_scalar_value_list(&bytes)
    }

    fn merge_batch(&mut self, states: &[arrow::array::ArrayRef]) -> DFResult<()> {
        if states.is_empty() {
            return Ok(());
        }
        let ffi_pairs = export_arrays_as_ffi(states)?;
        let states_addr = ffi_pairs.as_ptr() as usize;
        let num_states = ffi_pairs.len();

        do_upcall("Java accumulator merge_batch failed", |err: &ErrorBuffer| {
            self.inner.accumulator_merge(
                self.acc_id,
                states_addr,
                num_states,
                err.addr(),
                err.cap(),
            )
        })
    }

    fn size(&self) -> usize {
        self.inner.accumulator_size(self.acc_id)
    }
}

/// Decode a `LogicalExprList` of literal expressions into a `Vec<ScalarValue>`.
/// Java wraps each accumulator state value as `LogicalExprNode { literal: ScalarValue }`
/// so we can reuse the existing proto schema instead of inventing a wire format.
fn decode_scalar_value_list(bytes: &[u8]) -> DFResult<Vec<ScalarValue>> {
    let proto_list = LogicalExprList::decode(bytes).map_err(|e| {
        DataFusionError::External(format!("Failed to decode LogicalExprList: {}", e).into())
    })?;
    proto_list
        .expr
        .into_iter()
        .map(|node| match node.expr_type {
            Some(ExprType::Literal(scalar)) => (&scalar).try_into().map_err(
                |e: datafusion_proto::protobuf::FromProtoError| {
                    DataFusionError::External(
                        format!("Failed to convert ScalarValue from proto: {}", e).into(),
                    )
                },
            ),
            _ => Err(DataFusionError::External(
                "accumulator state: expected literal expression".into(),
            )),
        })
        .collect()
}
