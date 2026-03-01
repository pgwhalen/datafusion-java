#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use arrow::array::StructArray;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::DFSchema;
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion_proto::logical_plan::to_proto::serialize_exprs;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
    use datafusion_proto::protobuf::LogicalExprList;
    use diplomat_runtime::{DiplomatStr, DiplomatWrite};
    use prost::Message;
    use std::fmt::Write;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    // ── Diplomat traits for the catalog provider chain ──

    /// Catalog provider trait: returns schema names and schema providers.
    pub trait DfCatalogTrait {
        /// Write null-separated schema names to buf at buf_addr (cap buf_cap).
        /// Returns bytes written, or -1 on error.
        fn schema_names_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Returns DfSchemaProvider raw ptr (via createRaw downcall), or 0 for not found.
        fn schema(&self, name_addr: usize, name_len: usize) -> usize;
    }

    /// Schema provider trait: returns table names and table providers.
    pub trait DfSchemaTrait {
        /// Write null-separated table names to buf at buf_addr (cap buf_cap).
        /// Returns bytes written, or -1 on error.
        fn table_names_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Returns true if table exists.
        fn table_exists(&self, name_addr: usize, name_len: usize) -> bool;
        /// Returns DfTableProvider raw ptr, or 0 for not found.
        fn table(&self, name_addr: usize, name_len: usize) -> usize;
    }

    /// Table provider trait: returns schema, type, and creates execution plans.
    pub trait DfTableTrait {
        /// Returns FFI_ArrowSchema address for this table's schema.
        fn schema_address(&self) -> usize;
        /// Returns table type: 0=BASE, 1=VIEW, 2=TEMPORARY.
        fn table_type(&self) -> i32;
        /// Scan with session handle, filters, projection, limit, and error buffer.
        /// Returns DfExecutionPlan raw ptr, or 0 on error (check error buffer).
        fn scan(
            &self,
            session_addr: usize,
            filter_addr: usize,
            filter_len: usize,
            projection_addr: usize,
            projection_len: usize,
            limit: i64,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
        /// Returns count of FilterPushDown discriminants written to result_addr,
        /// or -1 on error (check error buffer).
        fn supports_filters_pushdown(
            &self,
            filter_addr: usize,
            filter_len: usize,
            result_addr: usize,
            result_cap: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
    }

    /// Execution plan trait: returns properties and executes partitions.
    pub trait DfExecutionPlanTrait {
        /// Returns FFI_ArrowSchema address for this plan's output schema.
        fn schema_address(&self) -> usize;
        /// Returns number of output partitions.
        fn output_partitioning(&self) -> i32;
        /// Returns emission type: 0=Incremental, 1=Final, 2=Both.
        fn emission_type(&self) -> i32;
        /// Returns boundedness: 0=Bounded, 1=Unbounded.
        fn boundedness(&self) -> i32;
        /// Returns DfRecordBatchReader raw ptr, or 0 on error (check error buffer).
        fn execute(&self, partition: i32, error_addr: usize, error_cap: usize) -> usize;
    }

    /// Record batch reader trait: iterates Arrow record batches.
    pub trait DfRecordBatchReaderTrait {
        /// Returns FFI_ArrowSchema address for this reader's schema.
        fn schema_address(&self) -> usize;
        /// Writes Arrow FFI data to provided addresses. Returns 1=data, 0=end, -1=error.
        fn next(
            &self,
            array_out_addr: usize,
            schema_out_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
    }

    // ── Opaque wrappers with create_raw factories ──

    /// Opaque wrapper for a schema provider backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfSchemaProvider(pub(crate) Box<dyn crate::providers::SchemaProviderBridge>);

    impl DfSchemaProvider {
        /// Create from a DfSchemaTrait impl and return the raw pointer address.
        /// Java uses this in re-entrant downcalls during upcalls.
        pub fn create_raw(t: impl DfSchemaTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfSchema::new(t);
            let boxed: Box<dyn crate::providers::SchemaProviderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfSchemaProvider(boxed)));
            ptr as usize
        }
    }

    /// Opaque wrapper for a table provider backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfTableProvider(pub(crate) Box<dyn crate::providers::TableProviderBridge>);

    impl DfTableProvider {
        /// Create from a DfTableTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfTableTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfTable::new(t);
            let boxed: Box<dyn crate::providers::TableProviderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfTableProvider(boxed)));
            ptr as usize
        }
    }

    /// Opaque wrapper for an execution plan backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfExecutionPlan(pub(crate) Box<dyn crate::providers::ExecutionPlanBridge>);

    impl DfExecutionPlan {
        /// Create from a DfExecutionPlanTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfExecutionPlanTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfPlan::new(t);
            let boxed: Box<dyn crate::providers::ExecutionPlanBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfExecutionPlan(boxed)));
            ptr as usize
        }
    }

    /// Opaque wrapper for a record batch reader backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfRecordBatchReader(pub(crate) Box<dyn crate::providers::RecordBatchReaderBridge>);

    impl DfRecordBatchReader {
        /// Create from a DfRecordBatchReaderTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfRecordBatchReaderTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfStream::new(t);
            let boxed: Box<dyn crate::providers::RecordBatchReaderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfRecordBatchReader(boxed)));
            ptr as usize
        }
    }

    #[diplomat::opaque]
    #[diplomat::attr(auto, error)]
    pub struct DfError(pub(super) Box<str>);

    impl DfError {
        pub fn to_display(&self, write: &mut DiplomatWrite) {
            let _ = write!(write, "{}", self.0);
        }
    }

    #[diplomat::opaque]
    pub struct DfArrowBatch {
        pub(super) schema: Arc<ArrowSchema>,
        pub(super) batch: RecordBatch,
    }

    impl DfArrowBatch {
        /// Create from raw FFI_ArrowSchema/FFI_ArrowArray addresses.
        /// Java calls this after Data.exportVectorSchemaRoot().
        pub fn from_addresses(
            schema_addr: usize,
            array_addr: usize,
        ) -> Result<Box<DfArrowBatch>, Box<DfError>> {
            if schema_addr == 0 || array_addr == 0 {
                return Err(Box::new(DfError(
                    "Null address for Arrow FFI struct".into(),
                )));
            }
            unsafe {
                let ffi_schema = std::ptr::read(schema_addr as *mut FFI_ArrowSchema);
                let ffi_array = std::ptr::read(array_addr as *mut FFI_ArrowArray);
                let array_data = from_ffi(ffi_array, &ffi_schema)
                    .map_err(|e| Box::new(DfError(format!("Failed to import Arrow array: {}", e).into())))?;
                let struct_array = StructArray::from(array_data);
                let schema = Arc::new(ArrowSchema::new(struct_array.fields().clone()));
                let batch = RecordBatch::from(struct_array);
                Ok(Box::new(DfArrowBatch { schema, batch }))
            }
        }
    }

    #[diplomat::opaque]
    pub struct DfArrowSchema {
        pub(super) schema: Arc<ArrowSchema>,
    }

    impl DfArrowSchema {
        /// Create from a raw FFI_ArrowSchema address.
        /// Java calls this after Data.exportSchema().
        pub fn from_address(addr: usize) -> Result<Box<DfArrowSchema>, Box<DfError>> {
            if addr == 0 {
                return Err(Box::new(DfError(
                    "Null address for Arrow FFI schema".into(),
                )));
            }
            unsafe {
                let ffi_schema = std::ptr::read(addr as *mut FFI_ArrowSchema);
                let schema = ArrowSchema::try_from(&ffi_schema)
                    .map_err(|e| Box::new(DfError(format!("Failed to import Arrow schema: {}", e).into())))?;
                Ok(Box::new(DfArrowSchema {
                    schema: Arc::new(schema),
                }))
            }
        }
    }

    /// Opaque wrapper for protobuf-serialized expression bytes.
    /// Workaround for Diplomat not supporting `&[u8]` returns.
    #[diplomat::opaque]
    pub struct DfExprBytes {
        pub(super) bytes: Vec<u8>,
    }

    impl DfExprBytes {
        pub fn len(&self) -> usize {
            self.bytes.len()
        }

        /// Copy bytes to a Java-allocated buffer at the given address.
        pub fn copy_to(&self, dest_addr: usize, dest_len: usize) {
            let copy_len = std::cmp::min(self.bytes.len(), dest_len);
            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.bytes.as_ptr(),
                    dest_addr as *mut u8,
                    copy_len,
                );
            }
        }
    }

    #[diplomat::opaque]
    pub struct DfSessionContext {
        pub(super) ctx: SessionContext,
        pub(super) rt: Arc<Runtime>,
    }

    impl DfSessionContext {
        #[diplomat::attr(auto, constructor)]
        pub fn new() -> Box<DfSessionContext> {
            let rt = Runtime::new().expect("Failed to create Tokio runtime");
            let ctx = SessionContext::new();
            Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            })
        }

        pub fn sql(&self, query: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let sql_str = std::str::from_utf8(query)
                .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))?;
            let df = self
                .rt
                .block_on(self.ctx.sql(sql_str))
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            Ok(Box::new(DfDataFrame {
                df,
                rt: Arc::clone(&self.rt),
            }))
        }

        pub fn session_id(&self, write: &mut DiplomatWrite) {
            let _ = write!(write, "{}", self.ctx.session_id());
        }

        pub fn session_start_time_millis(&self) -> i64 {
            self.ctx.session_start_time().timestamp_millis()
        }

        /// Register Arrow data as a named table.
        pub fn register_table(
            &self,
            name: &DiplomatStr,
            batch: &DfArrowBatch,
        ) -> Result<(), Box<DfError>> {
            let name_str = std::str::from_utf8(name)
                .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))?;
            let table = MemTable::try_new(
                Arc::clone(&batch.schema),
                vec![vec![batch.batch.clone()]],
            )
            .map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create memory table: {}", e).into(),
                ))
            })?;
            self.ctx
                .register_table(name_str, Arc::new(table))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to register table: {}", e).into(),
                    ))
                })?;
            Ok(())
        }

        /// Register a catalog backed by a Java-implemented DfCatalogTrait.
        pub fn register_catalog(
            &self,
            name: &DiplomatStr,
            catalog: impl DfCatalogTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            let name_str = std::str::from_utf8(name)
                .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))?;
            let foreign = crate::providers::ForeignDfCatalog::new(catalog);
            self.ctx
                .register_catalog(name_str, Arc::new(foreign));
            Ok(())
        }

        /// Parse a SQL expression against a schema, returning serialized protobuf bytes.
        pub fn parse_sql_expr(
            &self,
            sql: &DiplomatStr,
            schema: &DfArrowSchema,
        ) -> Result<Box<DfExprBytes>, Box<DfError>> {
            let sql_str = std::str::from_utf8(sql)
                .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))?;
            let df_schema = DFSchema::try_from(schema.schema.as_ref().clone()).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create DFSchema: {}", e).into(),
                ))
            })?;
            let state = self.ctx.state();
            let expr = state
                .create_logical_expr(sql_str, &df_schema)
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to parse SQL expression: {}", e).into(),
                    ))
                })?;
            let codec = DefaultLogicalExtensionCodec {};
            let serialized = serialize_exprs(&[expr], &codec).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to serialize expression: {}", e).into(),
                ))
            })?;
            let proto_list = LogicalExprList { expr: serialized };
            let bytes = proto_list.encode_to_vec();
            Ok(Box::new(DfExprBytes { bytes }))
        }
    }

    #[diplomat::opaque]
    pub struct DfDataFrame {
        pub(super) df: datafusion::dataframe::DataFrame,
        pub(super) rt: Arc<Runtime>,
    }

    impl DfDataFrame {
        /// Collect all batches and format as a pretty-printed string.
        pub fn collect_to_string(&self, write: &mut DiplomatWrite) -> Result<(), Box<DfError>> {
            let df = self.df.clone();
            let batches = self
                .rt
                .block_on(df.collect())
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            let formatted = pretty_format_batches(&batches)
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            let _ = write!(write, "{}", formatted);
            Ok(())
        }
    }
}
