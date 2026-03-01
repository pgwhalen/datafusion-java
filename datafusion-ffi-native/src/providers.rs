//! DataFusion trait wrappers that dispatch through Diplomat trait vtables.
//!
//! Each Foreign* struct stores a Diplomat-generated trait struct (vtable + data pointer)
//! and implements the corresponding DataFusion trait by calling through the vtable.

use crate::bridge::ffi::{
    DfCatalogTrait, DfExecutionPlan, DfExecutionPlanTrait, DfRecordBatchReader,
    DfRecordBatchReaderTrait, DfSchemaProvider, DfSchemaTrait, DfTableProvider, DfTableTrait,
};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use datafusion_proto::logical_plan::to_proto::serialize_exprs;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprList;
use futures::stream;
use prost::Message;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ── Bridge traits (marker traits for type erasure) ──

pub trait SchemaProviderBridge: SchemaProvider + Send + Sync {}
pub trait TableProviderBridge: TableProvider + Send + Sync {}
pub trait ExecutionPlanBridge: ExecutionPlan + Send + Sync {}
pub trait RecordBatchReaderBridge: Send + Sync {
    fn schema(&self) -> Arc<ArrowSchema>;
    fn next_batch(&self) -> Result<Option<RecordBatch>, DataFusionError>;
}

/// Read an error message from the error buffer. Returns empty string if addr is 0.
unsafe fn read_error(addr: usize, cap: usize) -> String {
    if addr == 0 || cap == 0 {
        return String::new();
    }
    // Scan for actual content (non-zero bytes)
    let slice = std::slice::from_raw_parts(addr as *const u8, cap);
    // Find the end of the message (first zero byte or end of slice)
    let len = slice.iter().position(|&b| b == 0).unwrap_or(cap);
    if len == 0 {
        return String::new();
    }
    String::from_utf8_lossy(&slice[..len]).into_owned()
}

// ── ForeignDfCatalog ──

pub struct ForeignDfCatalog<T: DfCatalogTrait> {
    inner: T,
}

impl<T: DfCatalogTrait> ForeignDfCatalog<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfCatalogTrait> fmt::Debug for ForeignDfCatalog<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfCatalog").finish()
    }
}

unsafe impl<T: DfCatalogTrait> Send for ForeignDfCatalog<T> {}
unsafe impl<T: DfCatalogTrait> Sync for ForeignDfCatalog<T> {}

impl<T: DfCatalogTrait + 'static> CatalogProvider for ForeignDfCatalog<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // Allocate a buffer for the names
        let cap: usize = 65536;
        let mut buf = vec![0u8; cap];
        let buf_addr = buf.as_mut_ptr() as usize;
        let written = self.inner.schema_names_to(buf_addr, cap);
        if written <= 0 {
            return Vec::new();
        }
        let written = written as usize;
        let s = String::from_utf8_lossy(&buf[..written]);
        s.split('\0')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let name_bytes = name.as_bytes();
        let name_addr = name_bytes.as_ptr() as usize;
        let name_len = name_bytes.len();
        let ptr = self.inner.schema(name_addr, name_len);
        if ptr == 0 {
            return None;
        }
        // Reconstruct the DfSchemaProvider from the raw pointer
        let boxed = unsafe { Box::from_raw(ptr as *mut DfSchemaProvider) };
        // Extract the inner SchemaProviderBridge and convert to Arc<dyn SchemaProvider>
        let bridge: Box<dyn SchemaProviderBridge> = boxed.0;
        let arc: Arc<dyn SchemaProviderBridge> = Arc::from(bridge);
        Some(arc as Arc<dyn SchemaProvider>)
    }
}

// ── ForeignDfSchema ──

pub struct ForeignDfSchema<T: DfSchemaTrait> {
    inner: T,
}

impl<T: DfSchemaTrait> ForeignDfSchema<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfSchemaTrait> fmt::Debug for ForeignDfSchema<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfSchema").finish()
    }
}

unsafe impl<T: DfSchemaTrait> Send for ForeignDfSchema<T> {}
unsafe impl<T: DfSchemaTrait> Sync for ForeignDfSchema<T> {}

impl<T: DfSchemaTrait + 'static> SchemaProviderBridge for ForeignDfSchema<T> {}

#[async_trait]
impl<T: DfSchemaTrait + 'static> SchemaProvider for ForeignDfSchema<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let cap: usize = 65536;
        let mut buf = vec![0u8; cap];
        let buf_addr = buf.as_mut_ptr() as usize;
        let written = self.inner.table_names_to(buf_addr, cap);
        if written <= 0 {
            return Vec::new();
        }
        let written = written as usize;
        let s = String::from_utf8_lossy(&buf[..written]);
        s.split('\0')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let name_bytes = name.as_bytes();
        let name_addr = name_bytes.as_ptr() as usize;
        let name_len = name_bytes.len();
        self.inner.table_exists(name_addr, name_len)
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let name_bytes = name.as_bytes();
        let name_addr = name_bytes.as_ptr() as usize;
        let name_len = name_bytes.len();
        let ptr = self.inner.table(name_addr, name_len);
        if ptr == 0 {
            return Ok(None);
        }
        // Reconstruct the DfTableProvider from the raw pointer
        let boxed = unsafe { Box::from_raw(ptr as *mut DfTableProvider) };
        let bridge: Box<dyn TableProviderBridge> = boxed.0;
        let arc: Arc<dyn TableProviderBridge> = Arc::from(bridge);
        Ok(Some(arc as Arc<dyn TableProvider>))
    }
}

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
        let (filter_addr, filter_len, _filter_buf) = if filters.is_empty() {
            (0usize, 0usize, Vec::new())
        } else {
            let codec = DefaultLogicalExtensionCodec {};
            let serialized = serialize_exprs(filters, &codec)?;
            let proto_list = LogicalExprList { expr: serialized };
            let bytes = proto_list.encode_to_vec();
            let len = bytes.len();
            let addr = bytes.as_ptr() as usize;
            (addr, len, bytes)
        };

        // Projection array
        let (proj_addr, proj_len) = match projections {
            Some(proj) => {
                (proj.as_ptr() as usize, proj.len())
            }
            None => (0usize, 0usize),
        };

        // Limit: -1 for None, else the value
        let limit_val: i64 = match limit {
            Some(l) => l as i64,
            None => -1,
        };

        // Error buffer
        let error_cap: usize = 4096;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let ptr = self.inner.scan(
            session_addr,
            filter_addr,
            filter_len,
            proj_addr,
            proj_len,
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
        let bytes = proto_list.encode_to_vec();
        let filter_addr = bytes.as_ptr() as usize;
        let filter_len = bytes.len();

        // Result buffer for i32 discriminants
        let result_cap = filters.len();
        let mut result_buf = vec![0i32; result_cap];
        let result_addr = result_buf.as_mut_ptr() as usize;

        // Error buffer
        let error_cap: usize = 4096;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let count = self.inner.supports_filters_pushdown(
            filter_addr,
            filter_len,
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

// ── ForeignDfPlan ──

pub struct ForeignDfPlan<T: DfExecutionPlanTrait> {
    inner: T,
    /// Cached properties
    properties: PlanProperties,
}

impl<T: DfExecutionPlanTrait> ForeignDfPlan<T> {
    pub fn new(inner: T) -> Self {
        // Import the schema via reference (Java owns the FFI struct and closes it after createRaw)
        let schema_addr = inner.schema_address();
        let schema = if schema_addr != 0 {
            unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                Arc::new(
                    ArrowSchema::try_from(ffi_schema).unwrap_or_else(|_| ArrowSchema::empty()),
                )
            }
        } else {
            Arc::new(ArrowSchema::empty())
        };

        // Build properties from trait callbacks
        let partitions = inner.output_partitioning();
        let emission = match inner.emission_type() {
            1 => EmissionType::Final,
            2 => EmissionType::Both,
            _ => EmissionType::Incremental,
        };
        let bounded = match inner.boundedness() {
            1 => Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
            _ => Boundedness::Bounded,
        };

        let eq_props =
            datafusion::physical_expr::EquivalenceProperties::new(Arc::clone(&schema));
        let properties = PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(partitions as usize),
            emission,
            bounded,
        );

        Self {
            inner,
            properties,
        }
    }
}

impl<T: DfExecutionPlanTrait> fmt::Debug for ForeignDfPlan<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfPlan").finish()
    }
}

impl<T: DfExecutionPlanTrait> DisplayAs for ForeignDfPlan<T> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ForeignDfPlan")
    }
}

unsafe impl<T: DfExecutionPlanTrait> Send for ForeignDfPlan<T> {}
unsafe impl<T: DfExecutionPlanTrait> Sync for ForeignDfPlan<T> {}

impl<T: DfExecutionPlanTrait + 'static> ExecutionPlanBridge for ForeignDfPlan<T> {}

impl<T: DfExecutionPlanTrait + 'static> ExecutionPlan for ForeignDfPlan<T> {
    fn name(&self) -> &str {
        "ForeignDfPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Error buffer
        let error_cap: usize = 4096;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let ptr = self.inner.execute(partition as i32, error_addr, error_cap);
        if ptr == 0 {
            let msg = unsafe { read_error(error_addr, error_cap) };
            return Err(DataFusionError::External(
                format!("Java execute callback failed: {}", msg).into(),
            ));
        }

        // Reconstruct the DfRecordBatchReader from the raw pointer
        let boxed = unsafe { Box::from_raw(ptr as *mut DfRecordBatchReader) };
        let reader_bridge = boxed.0;
        let reader_schema = reader_bridge.schema();

        // Create a stream from the reader
        let batch_stream = stream::unfold(reader_bridge, |reader| async move {
            match reader.next_batch() {
                Ok(Some(batch)) => Some((Ok(batch), reader)),
                Ok(None) => None,
                Err(e) => Some((Err(e), reader)),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            reader_schema,
            batch_stream,
        )))
    }
}

// ── ForeignDfStream ──

pub struct ForeignDfStream<T: DfRecordBatchReaderTrait> {
    inner: T,
    schema: Arc<ArrowSchema>,
}

impl<T: DfRecordBatchReaderTrait> ForeignDfStream<T> {
    pub fn new(inner: T) -> Self {
        // Import the schema via reference (Java owns the FFI struct and closes it after createRaw)
        let schema_addr = inner.schema_address();
        let schema = if schema_addr != 0 {
            unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                Arc::new(
                    ArrowSchema::try_from(ffi_schema).unwrap_or_else(|_| ArrowSchema::empty()),
                )
            }
        } else {
            Arc::new(ArrowSchema::empty())
        };
        Self { inner, schema }
    }
}

impl<T: DfRecordBatchReaderTrait> fmt::Debug for ForeignDfStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfStream").finish()
    }
}

unsafe impl<T: DfRecordBatchReaderTrait> Send for ForeignDfStream<T> {}
unsafe impl<T: DfRecordBatchReaderTrait> Sync for ForeignDfStream<T> {}

impl<T: DfRecordBatchReaderTrait> RecordBatchReaderBridge for ForeignDfStream<T> {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::clone(&self.schema)
    }

    fn next_batch(&self) -> Result<Option<RecordBatch>, DataFusionError> {
        // Allocate FFI structs for the Arrow data
        let mut ffi_array = FFI_ArrowArray::empty();
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let array_addr = &mut ffi_array as *mut FFI_ArrowArray as usize;
        let schema_addr = &mut ffi_schema as *mut FFI_ArrowSchema as usize;

        // Error buffer
        let error_cap: usize = 4096;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let result = self
            .inner
            .next(array_addr, schema_addr, error_addr, error_cap);

        match result {
            1 => {
                // Data available - import from FFI
                let array_data = unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(|e| {
                    DataFusionError::External(
                        format!("Failed to import Arrow array from Java: {}", e).into(),
                    )
                })?;
                let struct_array = arrow::array::StructArray::from(array_data);
                let batch = RecordBatch::from(struct_array);
                Ok(Some(batch))
            }
            0 => {
                // End of stream
                Ok(None)
            }
            _ => {
                // Error
                let msg = unsafe { read_error(error_addr, error_cap) };
                Err(DataFusionError::External(
                    format!("Java next() callback failed: {}", msg).into(),
                ))
            }
        }
    }
}
