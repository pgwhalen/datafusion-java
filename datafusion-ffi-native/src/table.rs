use crate::bridge::ffi::DfTableTrait;
use crate::plan::ffi::DfExecutionPlan;
use crate::bridge::{ffi::DfLazyRecordBatchStream, ffi::DfStringArray, LazyStreamState};
use crate::bridge::{import_schema, ExecutionPlanBridge, TableProviderBridge};
use crate::upcall_utils::{do_counted_upcall, do_returning_upcall};
use arrow::datatypes::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::{dml::InsertOp, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_proto::logical_plan::to_proto::serialize_exprs;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprList;
use prost::Message;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Serialize expressions to protobuf bytes. Returns an empty Vec for empty input.
fn encode_exprs(exprs: &[Expr]) -> datafusion::error::Result<Vec<u8>> {
    if exprs.is_empty() {
        return Ok(Vec::new());
    }
    let codec = DefaultLogicalExtensionCodec {};
    let serialized = serialize_exprs(exprs, &codec)?;
    let proto_list = LogicalExprList { expr: serialized };
    Ok(proto_list.encode_to_vec())
}

// ── ForeignDfTable ──

pub struct ForeignDfTable<T: DfTableTrait> {
    inner: T,
    /// Cached schema (imported once from Java)
    schema: Arc<ArrowSchema>,
}

impl<T: DfTableTrait> ForeignDfTable<T> {
    pub fn new(inner: T) -> Self {
        let schema = import_schema(inner.schema_address());
        Self { inner, schema }
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

        let filter_bytes = encode_exprs(filters)?;

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

        let plan = do_returning_upcall::<DfExecutionPlan>(
            "Java scan callback failed",
            Box::new(|ea, ec| {
                self.inner.scan(
                    session_addr,
                    filter_bytes.as_ptr() as usize,
                    filter_bytes.len(),
                    proj_u32.as_ptr() as usize,
                    proj_u32.len(),
                    limit_val,
                    ea,
                    ec,
                )
            }),
        )?;
        let arc: Arc<dyn ExecutionPlanBridge> = Arc::from(plan.0);
        Ok(arc as Arc<dyn ExecutionPlan>)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        let owned_filters: Vec<Expr> = filters.iter().map(|f| (*f).clone()).collect();
        let filter_bytes = encode_exprs(&owned_filters)?;

        // Result buffer for i32 discriminants
        let result_cap = filters.len();
        let mut result_buf = vec![0i32; result_cap];
        let result_addr = result_buf.as_mut_ptr() as usize;

        let count = do_counted_upcall("Java supports_filters_pushdown failed", |ea, ec| {
            self.inner.supports_filters_pushdown(
                filter_bytes.as_ptr() as usize,
                filter_bytes.len(),
                result_addr,
                result_cap,
                ea,
                ec,
            )
        })?;
        Ok(result_buf[..count]
            .iter()
            .map(|&d| match d {
                2 => TableProviderFilterPushDown::Exact,
                1 => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn insert_into(
        &self,
        state: &dyn datafusion::catalog::Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session_addr =
            state as *const dyn datafusion::catalog::Session as *const () as usize;

        // Execute the input plan to get a SendableRecordBatchStream
        let task_ctx = state.task_ctx();
        let stream = input.execute(0, task_ctx)?;
        let schema = Arc::new(input.schema().as_ref().clone());

        // Wrap in DfLazyRecordBatchStream with rt=None (re-entrant path uses block_in_place)
        let lazy_stream = DfLazyRecordBatchStream {
            inner: LazyStreamState {
                stream: std::sync::Mutex::new(Some(stream)),
                rt: None,
            },
            schema,
        };
        let stream_ptr = Box::into_raw(Box::new(lazy_stream)) as usize;

        // Convert InsertOp to i32 discriminant
        let insert_op_i32: i32 = match insert_op {
            InsertOp::Append => 0,
            InsertOp::Overwrite => 1,
            InsertOp::Replace => 2,
        };

        let plan = do_returning_upcall::<DfExecutionPlan>(
            "Java insert_into callback failed",
            Box::new(|ea, ec| {
                self.inner.insert_into(session_addr, stream_ptr, insert_op_i32, ea, ec)
            }),
        )?;
        let arc: Arc<dyn ExecutionPlanBridge> = Arc::from(plan.0);
        Ok(arc as Arc<dyn ExecutionPlan>)
    }

    async fn delete_from(
        &self,
        state: &dyn datafusion::catalog::Session,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session_addr =
            state as *const dyn datafusion::catalog::Session as *const () as usize;

        let filter_bytes = encode_exprs(&filters)?;

        let plan = do_returning_upcall::<DfExecutionPlan>(
            "Java delete_from callback failed",
            Box::new(|ea, ec| {
                self.inner.delete_from(
                    session_addr,
                    filter_bytes.as_ptr() as usize,
                    filter_bytes.len(),
                    ea,
                    ec,
                )
            }),
        )?;
        let arc: Arc<dyn ExecutionPlanBridge> = Arc::from(plan.0);
        Ok(arc as Arc<dyn ExecutionPlan>)
    }

    async fn update(
        &self,
        state: &dyn datafusion::catalog::Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session_addr =
            state as *const dyn datafusion::catalog::Session as *const () as usize;

        // Split assignments into column names and expressions
        let (col_names, assign_exprs): (Vec<String>, Vec<Expr>) =
            assignments.into_iter().unzip();

        // Create DfStringArray for column names (Java takes ownership via Box::from_raw)
        let col_names_array = Box::new(DfStringArray { strings: col_names });
        let col_names_ptr = Box::into_raw(col_names_array) as usize;

        let assign_bytes = encode_exprs(&assign_exprs)?;
        let filter_bytes = encode_exprs(&filters)?;

        let plan = do_returning_upcall::<DfExecutionPlan>(
            "Java update callback failed",
            Box::new(|ea, ec| {
                self.inner.update(
                    session_addr,
                    col_names_ptr,
                    assign_bytes.as_ptr() as usize,
                    assign_bytes.len(),
                    filter_bytes.as_ptr() as usize,
                    filter_bytes.len(),
                    ea,
                    ec,
                )
            }),
        )?;
        let arc: Arc<dyn ExecutionPlanBridge> = Arc::from(plan.0);
        Ok(arc as Arc<dyn ExecutionPlan>)
    }
}
