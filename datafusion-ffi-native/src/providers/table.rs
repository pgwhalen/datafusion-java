use crate::bridge::ffi::{DfExecutionPlan, DfTableTrait};
use super::{read_error, ExecutionPlanBridge, TableProviderBridge};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_proto::logical_plan::to_proto::serialize_exprs;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprList;
use prost::Message;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfTable ──

pub struct ForeignDfTable<T: DfTableTrait> {
    inner: T,
    /// Cached schema (imported once from Java)
    schema: Arc<ArrowSchema>,
}

impl<T: DfTableTrait> ForeignDfTable<T> {
    pub fn new(inner: T) -> Self {
        // Import the schema from the address provided by Java.
        // Use a reference (not ptr::read) so Rust doesn't take ownership of the FFI struct
        // and Java can safely close its ArrowSchema after this call.
        let schema_addr = inner.schema_address();
        let schema = if schema_addr != 0 {
            unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                ArrowSchema::try_from(ffi_schema).unwrap_or_else(|_| ArrowSchema::empty())
            }
        } else {
            ArrowSchema::empty()
        };
        Self {
            inner,
            schema: Arc::new(schema),
        }
    }
}

impl<T: DfTableTrait> fmt::Debug for ForeignDfTable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfTable").finish()
    }
}

unsafe impl<T: DfTableTrait> Send for ForeignDfTable<T> {}
unsafe impl<T: DfTableTrait> Sync for ForeignDfTable<T> {}

impl<T: DfTableTrait + 'static> TableProviderBridge for ForeignDfTable<T> {}

#[async_trait]
impl<T: DfTableTrait + 'static> TableProvider for ForeignDfTable<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        match self.inner.table_type() {
            1 => TableType::View,
            2 => TableType::Temporary,
            _ => TableType::Base,
        }
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projections: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Session address: box the trait object reference and pass as usize
        // We pass the raw pointer to &dyn Session so Java can call back into it
        let session_addr = state as *const dyn datafusion::catalog::Session as *const () as usize;

        // Serialize filters to protobuf bytes
        let filter_bytes: Vec<u8> = if filters.is_empty() {
            Vec::new()
        } else {
            let codec = DefaultLogicalExtensionCodec {};
            let serialized = serialize_exprs(filters, &codec)?;
            let proto_list = LogicalExprList { expr: serialized };
            proto_list.encode_to_vec()
        };

        // Projection: convert usize indices to u32
        let proj_u32: Vec<u32> = match projections {
            Some(proj) => proj.iter().map(|i| *i as u32).collect(),
            None => Vec::new(),
        };

        // Limit: -1 for None, else the value
        let limit_val: i64 = match limit {
            Some(l) => l as i64,
            None => -1,
        };

        // Error buffer
        let error_cap: usize = 32768;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let ptr = self.inner.scan(
            session_addr,
            filter_bytes.as_ptr() as usize,
            filter_bytes.len(),
            proj_u32.as_ptr() as usize,
            proj_u32.len(),
            limit_val,
            error_addr,
            error_cap,
        );

        if ptr == 0 {
            let msg = unsafe { read_error(error_addr, error_cap) };
            return Err(DataFusionError::External(
                format!("Java scan callback failed: {}", msg).into(),
            ));
        }

        // Reconstruct the DfExecutionPlan from the raw pointer
        let boxed = unsafe { Box::from_raw(ptr as *mut DfExecutionPlan) };
        let bridge: Box<dyn ExecutionPlanBridge> = boxed.0;
        let arc: Arc<dyn ExecutionPlanBridge> = Arc::from(bridge);
        Ok(arc as Arc<dyn ExecutionPlan>)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        // Serialize filters to protobuf bytes
        let codec = DefaultLogicalExtensionCodec {};
        let owned_filters: Vec<Expr> = filters.iter().map(|f| (*f).clone()).collect();
        let serialized = serialize_exprs(&owned_filters, &codec)?;
        let proto_list = LogicalExprList { expr: serialized };
        let filter_bytes = proto_list.encode_to_vec();

        // Result buffer for i32 discriminants
        let result_cap = filters.len();
        let mut result_buf = vec![0i32; result_cap];
        let result_addr = result_buf.as_mut_ptr() as usize;

        // Error buffer
        let error_cap: usize = 32768;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let count = self.inner.supports_filters_pushdown(
            filter_bytes.as_ptr() as usize,
            filter_bytes.len(),
            result_addr,
            result_cap,
            error_addr,
            error_cap,
        );

        if count < 0 {
            let msg = unsafe { read_error(error_addr, error_cap) };
            return Err(DataFusionError::External(
                format!("Java supports_filters_pushdown failed: {}", msg).into(),
            ));
        }

        let count = count as usize;
        Ok(result_buf[..count]
            .iter()
            .map(|&d| match d {
                2 => TableProviderFilterPushDown::Exact,
                1 => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }
}
