#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use arrow::array::{Array, StructArray};
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, JoinType};
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::SessionState;
    use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};
    use datafusion_proto::logical_plan::from_proto::{parse_exprs, parse_sorts};
    use datafusion_proto::logical_plan::to_proto::serialize_exprs;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
    use datafusion_proto::protobuf::{LogicalExprList, SortExprNodeCollection};
    use diplomat_runtime::{DiplomatStr, DiplomatWrite};
    use prost::Message;
    use std::collections::HashMap;
    use std::fmt::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    // ============================================================================
    // Deserialize helpers (safe, no raw pointers)
    // ============================================================================

    fn deserialize_exprs(ctx: &SessionContext, bytes: &[u8]) -> Result<Vec<Expr>, String> {
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        let proto_list =
            LogicalExprList::decode(bytes).map_err(|e| format!("Failed to decode proto: {}", e))?;
        let codec = DefaultLogicalExtensionCodec {};
        parse_exprs(proto_list.expr.iter(), ctx, &codec)
            .map_err(|e| format!("Failed to parse expressions: {}", e))
    }

    fn deserialize_sort_exprs(ctx: &SessionContext, bytes: &[u8]) -> Result<Vec<SortExpr>, String> {
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        let collection = SortExprNodeCollection::decode(bytes)
            .map_err(|e| format!("Failed to decode sort expressions: {}", e))?;
        let codec = DefaultLogicalExtensionCodec {};
        parse_sorts(collection.sort_expr_nodes.iter(), ctx, &codec)
            .map_err(|e| format!("Failed to parse sort expressions: {}", e))
    }

    fn diplomat_str(s: &DiplomatStr) -> Result<&str, Box<DfError>> {
        std::str::from_utf8(s)
            .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))
    }

    /// Parse session config from null-separated bytes: key\0value\0key\0value...
    fn build_session_config(options_addr: usize, options_len: usize) -> Result<SessionConfig, String> {
        let parts = super::parse_null_separated(options_addr, options_len);
        if parts.is_empty() {
            return Ok(SessionConfig::new());
        }
        let mut settings = HashMap::with_capacity(parts.len() / 2);
        for chunk in parts.chunks(2) {
            if chunk.len() == 2 {
                settings.insert(chunk[0].clone(), chunk[1].clone());
            }
        }
        SessionConfig::from_string_hash_map(&settings)
            .map_err(|e| format!("Failed to create SessionConfig: {}", e))
    }

    // ============================================================================
    // Diplomat enums
    // ============================================================================

    /// Join type for DataFrame join operations.
    pub enum DfJoinType {
        Inner,
        Left,
        Right,
        Full,
        LeftSemi,
        LeftAnti,
        RightSemi,
        RightAnti,
    }

    // ============================================================================
    // Diplomat traits
    // ============================================================================

    // -- Scalar UDF --

    /// Scalar UDF trait: user-defined function callbacks.
    pub trait DfScalarUdfTrait {
        /// Write function name to buf at buf_addr (cap buf_cap). Returns bytes written.
        fn name_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Returns volatility: 0=Immutable, 1=Stable, 2=Volatile.
        fn volatility(&self) -> i32;
        /// Compute return field from arg types. Writes FFI_ArrowSchema to out_schema_addr.
        /// arg_types_addr points to an array of FFI_ArrowSchema, arg_types_len is count.
        /// Returns 0 on success, -1 on error (check error buffer).
        fn return_field(
            &self,
            arg_types_addr: usize,
            arg_types_len: usize,
            out_schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
        /// Invoke the function. args_addr points to array of {FFI_ArrowArray, FFI_ArrowSchema} pairs.
        /// Writes result to out_array_addr (FFI_ArrowArray) and out_schema_addr (FFI_ArrowSchema).
        /// Returns 0 on success, -1 on error.
        fn invoke(
            &self,
            args_addr: usize,
            num_args: usize,
            num_rows: usize,
            arg_fields_addr: usize,
            return_field_addr: usize,
            out_array_addr: usize,
            out_schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
        /// Coerce argument types. Reads FFI_ArrowSchema array at arg_types_addr,
        /// writes coerced schemas to result_addr. Returns count on success, -1 on error.
        fn coerce_types(
            &self,
            arg_types_addr: usize,
            arg_types_len: usize,
            result_addr: usize,
            result_cap: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
    }

    // -- File format chain --

    /// File format trait: creates file sources.
    pub trait DfFileFormatTrait {
        /// Write file extension to buf. Returns bytes written.
        fn extension_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Create a file source. schema_addr is FFI_ArrowSchema address.
        /// Returns DfFileSource raw ptr, or 0 on error (check error buffer).
        fn file_source(
            &self,
            schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    /// File source trait: creates file openers.
    pub trait DfFileSourceTrait {
        /// Write file type identifier to buf. Returns bytes written.
        fn file_type_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Create a file opener with scan parameters.
        /// Returns DfFileOpener raw ptr, or 0 on error (check error buffer).
        fn create_file_opener(
            &self,
            schema_addr: usize,
            projection_addr: usize,
            projection_len: usize,
            limit: i64,
            batch_size: i64,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    /// File opener trait: opens files and returns record batch readers.
    pub trait DfFileOpenerTrait {
        /// Open a file and return a DfRecordBatchReader raw ptr, or 0 on error.
        fn open(
            &self,
            path_addr: usize,
            path_len: usize,
            file_size: u64,
            range_start: i64,
            range_end: i64,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    // -- Catalog provider chain --

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
        /// Returns DfTableProvider raw ptr, or 0 for not found/error (check error buffer).
        fn table(&self, name_addr: usize, name_len: usize, error_addr: usize, error_cap: usize) -> usize;
    }

    /// Table provider trait: returns schema, type, and creates execution plans.
    pub trait DfTableTrait {
        /// Returns FFI_ArrowSchema address for this table's schema.
        fn schema_address(&self) -> usize;
        /// Returns table type: 0=BASE, 1=VIEW, 2=TEMPORARY.
        fn table_type(&self) -> i32;
        /// Scan with session handle, protobuf-encoded filters, projection indices, limit,
        /// and error buffer. Returns DfExecutionPlan raw ptr, or 0 on error.
        fn scan(
            &self,
            session_addr: usize,
            filters_addr: usize,
            filters_len: usize,
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
            filters_addr: usize,
            filters_len: usize,
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

    // ============================================================================
    // Opaque wrappers with create_raw factories
    // ============================================================================

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

    // ── Opaque wrappers for file format chain ──

    /// Opaque wrapper for a file source backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfFileSource(pub(crate) Box<dyn crate::providers::FileSourceBridge>);

    impl DfFileSource {
        /// Create from a DfFileSourceTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfFileSourceTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfFileSource::new(t);
            let boxed: Box<dyn crate::providers::FileSourceBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfFileSource(boxed)));
            ptr as usize
        }
    }

    /// Opaque wrapper for a file opener backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfFileOpener(pub(crate) Box<dyn crate::providers::FileOpenerBridge>);

    impl DfFileOpener {
        /// Create from a DfFileOpenerTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfFileOpenerTrait + 'static) -> usize {
            let wrapper = crate::providers::ForeignDfFileOpener::new(t);
            let boxed: Box<dyn crate::providers::FileOpenerBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfFileOpener(boxed)));
            ptr as usize
        }
    }

    // ============================================================================
    // Other opaques
    // ============================================================================

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
                let array_data = from_ffi(ffi_array, &ffi_schema).map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to import Arrow array: {}", e).into(),
                    ))
                })?;
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
                let schema = ArrowSchema::try_from(&ffi_schema).map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to import Arrow schema: {}", e).into(),
                    ))
                })?;
                Ok(Box::new(DfArrowSchema {
                    schema: Arc::new(schema),
                }))
            }
        }

        /// Export this schema as FFI_ArrowSchema to a Java-provided address.
        pub fn export_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err(Box::new(DfError("Null output address".into())));
            }
            let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(|e| {
                Box::new(DfError(
                    format!("Schema export failed: {}", e).into(),
                ))
            })?;
            unsafe {
                std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(())
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

    // ============================================================================
    // New opaques: DfRuntimeEnv, DfRecordBatchStream, DfSessionState, DfLogicalPlan
    // ============================================================================

    /// Opaque wrapper for RuntimeEnv with optional memory pool configuration.
    #[diplomat::opaque]
    pub struct DfRuntimeEnv {
        pub(super) rt_env: Arc<RuntimeEnv>,
    }

    impl DfRuntimeEnv {
        /// Create a RuntimeEnv. If max_memory > 0, uses a memory pool with that limit.
        pub fn new_with_memory_limit(
            max_memory: usize,
            memory_fraction: f64,
        ) -> Result<Box<DfRuntimeEnv>, Box<DfError>> {
            use datafusion::execution::runtime_env::RuntimeEnvBuilder;
            let builder = if max_memory > 0 {
                RuntimeEnvBuilder::new().with_memory_limit(max_memory, memory_fraction)
            } else {
                RuntimeEnvBuilder::new()
            };
            let rt_env = builder.build_arc().map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create RuntimeEnv: {}", e).into(),
                ))
            })?;
            Ok(Box::new(DfRuntimeEnv { rt_env }))
        }
    }

    /// Opaque wrapper for collected record batches with iteration support.
    #[diplomat::opaque]
    pub struct DfRecordBatchStream {
        pub(super) batches: Vec<RecordBatch>,
        pub(super) schema: Arc<ArrowSchema>,
        pub(super) index: AtomicUsize,
    }

    impl DfRecordBatchStream {
        /// Get the stream's schema as a DfArrowSchema.
        pub fn schema(&self) -> Box<DfArrowSchema> {
            Box::new(DfArrowSchema {
                schema: Arc::clone(&self.schema),
            })
        }

        /// Export the stream's schema as FFI_ArrowSchema to a Java-provided address.
        pub fn schema_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err(Box::new(DfError("Null output address".into())));
            }
            let ffi_schema =
                FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(|e| {
                    Box::new(DfError(
                        format!("Schema export failed: {}", e).into(),
                    ))
                })?;
            unsafe {
                std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(())
        }

        /// Get the next batch. Writes FFI_ArrowArray and FFI_ArrowSchema to provided addresses.
        /// Returns 1 if data available, 0 if end of stream.
        pub fn next(
            &self,
            array_out_addr: usize,
            schema_out_addr: usize,
        ) -> Result<i32, Box<DfError>> {
            let idx = self.index.fetch_add(1, Ordering::SeqCst);
            if idx >= self.batches.len() {
                return Ok(0);
            }
            let batch = &self.batches[idx];
            let struct_array: StructArray = batch.clone().into();
            let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data()).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to export batch: {}", e).into(),
                ))
            })?;
            unsafe {
                std::ptr::write(array_out_addr as *mut FFI_ArrowArray, ffi_array);
                std::ptr::write(schema_out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(1)
        }
    }

    /// Opaque wrapper for a lazy record batch stream.
    ///
    /// Unlike DfRecordBatchStream (which collects all batches upfront), this holds the live
    /// async stream and fetches exactly one batch per next() call.
    ///
    /// The live stream state is stored in a `super::LazyStreamState` to avoid exposing
    /// dyn-trait fields inside the #[diplomat::bridge] macro.
    #[diplomat::opaque]
    pub struct DfLazyRecordBatchStream {
        pub(super) inner: super::LazyStreamState,
        pub(super) schema: Arc<ArrowSchema>,
    }

    impl DfLazyRecordBatchStream {
        /// Export the stream's schema as FFI_ArrowSchema to a Java-provided address.
        pub fn schema_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err(Box::new(DfError("Null output address".into())));
            }
            let ffi_schema =
                FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(|e| {
                    Box::new(DfError(
                        format!("Schema export failed: {}", e).into(),
                    ))
                })?;
            unsafe {
                std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(())
        }

        /// Fetch the next batch from the live async stream.
        /// Writes FFI_ArrowArray and FFI_ArrowSchema to provided addresses.
        /// Returns 1 if a batch was available, 0 at end of stream.
        pub fn next(
            &self,
            array_out_addr: usize,
            schema_out_addr: usize,
        ) -> Result<i32, Box<DfError>> {
            use futures::StreamExt;
            let mut guard = self.inner.stream.lock().map_err(|e| {
                Box::new(DfError(format!("Stream lock poisoned: {}", e).into()))
            })?;
            let stream = match guard.as_mut() {
                None => return Ok(0),
                Some(s) => s,
            };
            match self.inner.rt.block_on(stream.next()) {
                None => Ok(0),
                Some(Err(e)) => Err(Box::new(DfError(
                    format!("Stream error: {}", e).into(),
                ))),
                Some(Ok(batch)) => {
                    let struct_array: StructArray = batch.into();
                    let (ffi_array, ffi_schema) =
                        to_ffi(&struct_array.to_data()).map_err(|e| {
                            Box::new(DfError(
                                format!("Failed to export batch: {}", e).into(),
                            ))
                        })?;
                    unsafe {
                        std::ptr::write(array_out_addr as *mut FFI_ArrowArray, ffi_array);
                        std::ptr::write(
                            schema_out_addr as *mut FFI_ArrowSchema,
                            ffi_schema,
                        );
                    }
                    Ok(1)
                }
            }
        }
    }

    /// Opaque wrapper for SessionState with its own Tokio runtime.
    #[diplomat::opaque]
    pub struct DfSessionState {
        pub(super) state: SessionState,
        pub(super) rt: Runtime,
    }

    impl DfSessionState {
        /// Create a logical plan from a SQL string.
        pub fn create_logical_plan(
            &self,
            sql: &DiplomatStr,
        ) -> Result<Box<DfLogicalPlan>, Box<DfError>> {
            let sql_str = diplomat_str(sql)?;
            let plan = self
                .rt
                .block_on(async { self.state.create_logical_plan(sql_str).await })
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to create logical plan: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfLogicalPlan { plan }))
        }

        /// Create a physical expression from protobuf-encoded filter bytes and schema.
        /// The filter bytes are a LogicalExprList proto. Expressions are conjoined with AND.
        /// schema_addr points to an FFI_ArrowSchema (read by reference, not consumed).
        /// Returns the physical expression as a raw pointer address (caller must free).
        pub fn create_physical_expr(
            &self,
            filter_bytes: &[u8],
            schema_addr: usize,
        ) -> Result<usize, Box<DfError>> {
            if filter_bytes.is_empty() {
                return Err(Box::new(DfError("No filter bytes provided".into())));
            }
            if schema_addr == 0 {
                return Err(Box::new(DfError("Null schema address".into())));
            }

            let proto_list = LogicalExprList::decode(filter_bytes).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to decode filter protobuf: {}", e).into(),
                ))
            })?;

            // Use a temporary SessionContext for UDF resolution during parsing
            let temp_ctx = SessionContext::new_with_state(self.state.clone());
            let codec = DefaultLogicalExtensionCodec {};
            let exprs: Vec<Expr> =
                parse_exprs(proto_list.expr.iter(), &temp_ctx, &codec).map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to parse filter expressions: {}", e).into(),
                    ))
                })?;

            if exprs.is_empty() {
                return Err(Box::new(DfError("No filters after parsing".into())));
            }

            // Import schema from FFI (by reference)
            let schema = unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                Arc::new(ArrowSchema::try_from(ffi_schema).map_err(|e| {
                    Box::new(DfError(format!("Invalid schema: {}", e).into()))
                })?)
            };
            let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create DFSchema: {}", e).into(),
                ))
            })?;

            // Conjoin all filters with AND
            let combined = exprs.into_iter().reduce(|a, b| a.and(b)).unwrap();

            // Create physical expression
            let physical_expr =
                self.state
                    .create_physical_expr(combined, &df_schema)
                    .map_err(|e| {
                        Box::new(DfError(
                            format!("Failed to create physical expression: {}", e).into(),
                        ))
                    })?;

            Ok(Box::into_raw(Box::new(physical_expr)) as usize)
        }

        /// Destroy a physical expression created by create_physical_expr.
        pub fn destroy_physical_expr(addr: usize) {
            if addr != 0 {
                unsafe {
                    drop(Box::from_raw(
                        addr as *mut Arc<dyn datafusion::physical_plan::PhysicalExpr>,
                    ));
                }
            }
        }
    }

    /// Opaque wrapper for a LogicalPlan.
    #[diplomat::opaque]
    pub struct DfLogicalPlan {
        pub(super) plan: LogicalPlan,
    }

    impl DfLogicalPlan {
        /// Get a display string of the logical plan.
        pub fn to_display(&self, write: &mut DiplomatWrite) {
            let _ = write!(write, "{}", self.plan.display_indent());
        }
    }

    // ============================================================================
    // DfSessionContext
    // ============================================================================

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

        /// Create a SessionContext with configuration options.
        /// options_addr points to null-separated bytes: key\0value\0key\0value...
        pub fn new_with_config(
            options_addr: usize,
            options_len: usize,
        ) -> Result<Box<DfSessionContext>, Box<DfError>> {
            let config = build_session_config(options_addr, options_len).map_err(|e| Box::new(DfError(e.into())))?;
            let rt = Runtime::new().map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create runtime: {}", e).into(),
                ))
            })?;
            let ctx = SessionContext::new_with_config(config);
            Ok(Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            }))
        }

        /// Create a SessionContext with configuration options and a custom RuntimeEnv.
        /// options_addr points to null-separated bytes: key\0value\0key\0value...
        pub fn new_with_config_rt(
            options_addr: usize,
            options_len: usize,
            rt_env: &DfRuntimeEnv,
        ) -> Result<Box<DfSessionContext>, Box<DfError>> {
            let config = build_session_config(options_addr, options_len).map_err(|e| Box::new(DfError(e.into())))?;
            let rt = Runtime::new().map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create runtime: {}", e).into(),
                ))
            })?;
            let ctx =
                SessionContext::new_with_config_rt(config, Arc::clone(&rt_env.rt_env));
            Ok(Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            }))
        }

        pub fn sql(&self, query: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let sql_str = diplomat_str(query)?;
            let df = self
                .rt
                .block_on(self.ctx.sql(sql_str))
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            Ok(Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
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
            let name_str = diplomat_str(name)?;
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
            let name_str = diplomat_str(name)?;
            let foreign = crate::providers::ForeignDfCatalog::new(catalog);
            self.ctx.register_catalog(name_str, Arc::new(foreign));
            Ok(())
        }

        /// Parse a SQL expression against a schema, returning serialized protobuf bytes.
        pub fn parse_sql_expr(
            &self,
            sql: &DiplomatStr,
            schema: &DfArrowSchema,
        ) -> Result<Box<DfExprBytes>, Box<DfError>> {
            let sql_str = diplomat_str(sql)?;
            let df_schema =
                DFSchema::try_from(schema.schema.as_ref().clone()).map_err(|e| {
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

        /// Register a scalar UDF backed by a Java-implemented DfScalarUdfTrait.
        pub fn register_udf(
            &self,
            udf: impl DfScalarUdfTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            let foreign = crate::providers::ForeignDfUdf::new(udf);
            let scalar_udf =
                datafusion::logical_expr::ScalarUDF::new_from_impl(foreign);
            self.ctx.register_udf(scalar_udf);
            Ok(())
        }

        /// Register a listing table backed by a Java-implemented DfFileFormatTrait.
        pub fn register_listing_table(
            &self,
            name: &DiplomatStr,
            format: impl DfFileFormatTrait + 'static,
            urls_addr: usize,
            urls_len: usize,
            extension: &DiplomatStr,
            schema_addr: usize,
            collect_stat: i32,
            target_partitions: usize,
        ) -> Result<(), Box<DfError>> {
            use datafusion::datasource::file_format::FileFormat;
            use datafusion::datasource::listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            };

            let name_str = diplomat_str(name)?;

            let extension_str = diplomat_str(extension)?.to_string();

            // Import schema by reference. The caller (Java) must call the release callback
            // on the FFI_ArrowSchema after this function returns to free exported buffers.
            let arrow_schema = if schema_addr != 0 {
                unsafe {
                    let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                    Arc::new(ArrowSchema::try_from(ffi_schema).map_err(|e| {
                        Box::new(DfError(
                            format!("Failed to import schema: {}", e).into(),
                        ))
                    })?)
                }
            } else {
                return Err(Box::new(DfError("Null schema address".into())));
            };

            // Create the foreign file format
            let foreign_format = crate::providers::ForeignDfFileFormat::new(
                format,
                Arc::clone(&arrow_schema),
                extension_str.clone(),
            );
            let format_arc: Arc<dyn FileFormat> = Arc::new(foreign_format);

            // Build listing options
            let options = ListingOptions::new(format_arc)
                .with_file_extension(extension_str)
                .with_collect_stat(collect_stat != 0)
                .with_target_partitions(target_partitions);

            // Parse URLs from null-separated bytes
            let url_strs = super::parse_null_separated(urls_addr, urls_len);
            let mut table_urls = Vec::with_capacity(url_strs.len());
            for url_str in &url_strs {
                let table_url = ListingTableUrl::parse(url_str).map_err(|e| {
                    Box::new(DfError(format!("Failed to parse URL '{}': {}", url_str, e).into()))
                })?;
                table_urls.push(table_url);
            }

            let config = ListingTableConfig::new_with_multi_paths(table_urls)
                .with_listing_options(options)
                .with_schema(arrow_schema);

            let table = ListingTable::try_new(config).map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create listing table: {}", e).into(),
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

        /// Get a DataFrame for a registered table by name.
        pub fn table(&self, name: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let df = self
                .rt
                .block_on(self.ctx.table(name_str))
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            Ok(Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Read a Parquet file/directory into a DataFrame.
        pub fn read_parquet(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_parquet(path_str, Default::default()))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to read Parquet: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Read a CSV file/directory into a DataFrame.
        pub fn read_csv(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_csv(path_str, Default::default()))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to read CSV: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Read a JSON file/directory into a DataFrame.
        pub fn read_json(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_json(path_str, Default::default()))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Failed to read JSON: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Get a snapshot of the session state.
        pub fn state(&self) -> Result<Box<DfSessionState>, Box<DfError>> {
            let state = self.ctx.state();
            let rt = Runtime::new().map_err(|e| {
                Box::new(DfError(
                    format!("Failed to create runtime: {}", e).into(),
                ))
            })?;
            Ok(Box::new(DfSessionState { state, rt }))
        }
    }

    // ============================================================================
    // DfDataFrame
    // ============================================================================

    #[diplomat::opaque]
    pub struct DfDataFrame {
        pub(super) df: datafusion::dataframe::DataFrame,
        pub(super) ctx: SessionContext,
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
            let formatted = arrow::util::pretty::pretty_format_batches(&batches)
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            let _ = write!(write, "{}", formatted);
            Ok(())
        }

        // ── Execution methods ──

        /// Execute the DataFrame, collect all results, and return as a DfRecordBatchStream.
        pub fn collect_stream(&self) -> Result<Box<DfRecordBatchStream>, Box<DfError>> {
            let df = self.df.clone();
            let batches = self
                .rt
                .block_on(df.collect())
                .map_err(|e| Box::new(DfError(format!("Collect failed: {}", e).into())))?;
            let schema = if batches.is_empty() {
                Arc::new(self.df.schema().as_arrow().as_ref().clone())
            } else {
                batches[0].schema()
            };
            Ok(Box::new(DfRecordBatchStream {
                batches,
                schema,
                index: AtomicUsize::new(0),
            }))
        }

        /// Execute the DataFrame as a lazy stream, returning one batch at a time.
        ///
        /// Unlike collect_stream(), this does NOT wait for all batches before returning.
        /// Each subsequent call to DfLazyRecordBatchStream::next() fetches exactly one batch
        /// from the live async execution pipeline.
        pub fn execute_stream(&self) -> Result<Box<DfLazyRecordBatchStream>, Box<DfError>> {
            let df = self.df.clone();
            let schema = Arc::new(self.df.schema().as_arrow().as_ref().clone());
            let stream = self
                .rt
                .block_on(df.execute_stream())
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Execute stream failed: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfLazyRecordBatchStream {
                inner: super::LazyStreamState {
                    stream: std::sync::Mutex::new(Some(stream)),
                    rt: Arc::clone(&self.rt),
                },
                schema,
            }))
        }

        /// Print DataFrame results to stdout.
        pub fn show(&self) -> Result<(), Box<DfError>> {
            let df = self.df.clone();
            self.rt
                .block_on(df.show())
                .map_err(|e| Box::new(DfError(format!("Show failed: {}", e).into())))?;
            Ok(())
        }

        /// Get the row count.
        pub fn count(&self) -> Result<i64, Box<DfError>> {
            let df = self.df.clone();
            let count = self
                .rt
                .block_on(df.count())
                .map_err(|e| Box::new(DfError(format!("Count failed: {}", e).into())))?;
            Ok(count as i64)
        }

        /// Get the schema as a DfArrowSchema.
        pub fn schema(&self) -> Box<DfArrowSchema> {
            let arrow_schema = self.df.schema().as_arrow().as_ref().clone();
            Box::new(DfArrowSchema {
                schema: Arc::new(arrow_schema),
            })
        }

        /// Export the schema as FFI_ArrowSchema to a Java-provided address.
        pub fn schema_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err(Box::new(DfError("Null output address".into())));
            }
            let arrow_schema = self.df.schema().as_arrow().as_ref().clone();
            let ffi_schema = FFI_ArrowSchema::try_from(&arrow_schema).map_err(|e| {
                Box::new(DfError(
                    format!("Schema export failed: {}", e).into(),
                ))
            })?;
            unsafe {
                std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(())
        }

        // ── Transformation methods ──

        /// Filter rows by a protobuf-encoded predicate expression.
        pub fn filter_bytes(
            &self,
            bytes_addr: usize,
            bytes_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let bytes = if bytes_addr != 0 && bytes_len > 0 {
                unsafe { std::slice::from_raw_parts(bytes_addr as *const u8, bytes_len) }
            } else {
                &[]
            };
            let exprs = deserialize_exprs(&self.ctx, bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            if exprs.is_empty() {
                return Err(Box::new(DfError(
                    "Filter requires exactly one predicate expression".into(),
                )));
            }
            let new_df = self.df.clone().filter(exprs[0].clone()).map_err(|e| {
                Box::new(DfError(format!("Filter failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Project columns by protobuf-encoded expression list.
        pub fn select_bytes(
            &self,
            bytes_addr: usize,
            bytes_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let bytes = if bytes_addr != 0 && bytes_len > 0 {
                unsafe { std::slice::from_raw_parts(bytes_addr as *const u8, bytes_len) }
            } else {
                &[]
            };
            let exprs = deserialize_exprs(&self.ctx, bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            let new_df = self.df.clone().select(exprs).map_err(|e| {
                Box::new(DfError(format!("Select failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Aggregate with protobuf-encoded group-by and aggregate expression lists.
        pub fn aggregate_bytes(
            &self,
            group_addr: usize,
            group_len: usize,
            aggr_addr: usize,
            aggr_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let group_bytes = if group_addr != 0 && group_len > 0 {
                unsafe { std::slice::from_raw_parts(group_addr as *const u8, group_len) }
            } else {
                &[]
            };
            let aggr_bytes = if aggr_addr != 0 && aggr_len > 0 {
                unsafe { std::slice::from_raw_parts(aggr_addr as *const u8, aggr_len) }
            } else {
                &[]
            };
            let group_exprs = deserialize_exprs(&self.ctx, group_bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            let aggr_exprs = deserialize_exprs(&self.ctx, aggr_bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            let new_df = self
                .df
                .clone()
                .aggregate(group_exprs, aggr_exprs)
                .map_err(|e| {
                    Box::new(DfError(format!("Aggregate failed: {}", e).into()))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Sort by protobuf-encoded sort expressions.
        pub fn sort_bytes(
            &self,
            bytes_addr: usize,
            bytes_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let bytes = if bytes_addr != 0 && bytes_len > 0 {
                unsafe { std::slice::from_raw_parts(bytes_addr as *const u8, bytes_len) }
            } else {
                &[]
            };
            let sort_exprs = deserialize_sort_exprs(&self.ctx, bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            let new_df = self.df.clone().sort(sort_exprs).map_err(|e| {
                Box::new(DfError(format!("Sort failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Limit rows. fetch=-1 means no limit.
        pub fn limit(
            &self,
            skip: usize,
            fetch: i64,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let fetch_option = if fetch < 0 {
                None
            } else {
                Some(fetch as usize)
            };
            let new_df = self.df.clone().limit(skip, fetch_option).map_err(|e| {
                Box::new(DfError(format!("Limit failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        // ── Join and set operations ──

        /// Join on column names with optional filter.
        /// Join with null-separated column name lists and optional protobuf filter bytes.
        pub fn join(
            &self,
            right: &DfDataFrame,
            join_type: DfJoinType,
            left_addr: usize,
            left_len: usize,
            right_addr: usize,
            right_len: usize,
            filter_addr: usize,
            filter_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let jt: JoinType = match join_type {
                DfJoinType::Inner => JoinType::Inner,
                DfJoinType::Left => JoinType::Left,
                DfJoinType::Right => JoinType::Right,
                DfJoinType::Full => JoinType::Full,
                DfJoinType::LeftSemi => JoinType::LeftSemi,
                DfJoinType::LeftAnti => JoinType::LeftAnti,
                DfJoinType::RightSemi => JoinType::RightSemi,
                DfJoinType::RightAnti => JoinType::RightAnti,
            };

            let left_strs = super::parse_null_separated(left_addr, left_len);
            let right_strs = super::parse_null_separated(right_addr, right_len);
            let left_col_strs: Vec<&str> = left_strs.iter().map(|s| s.as_str()).collect();
            let right_col_strs: Vec<&str> = right_strs.iter().map(|s| s.as_str()).collect();

            let filter = if filter_addr == 0 || filter_len == 0 {
                None
            } else {
                let filter_bytes = unsafe { std::slice::from_raw_parts(filter_addr as *const u8, filter_len) };
                let exprs = deserialize_exprs(&self.ctx, filter_bytes)
                    .map_err(|e| Box::new(DfError(e.into())))?;
                exprs.into_iter().next()
            };

            let new_df = self
                .df
                .clone()
                .join(right.df.clone(), jt, &left_col_strs, &right_col_strs, filter)
                .map_err(|e| {
                    Box::new(DfError(format!("Join failed: {}", e).into()))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Join on arbitrary protobuf-encoded expressions.
        pub fn join_on_bytes(
            &self,
            right: &DfDataFrame,
            join_type: DfJoinType,
            on_addr: usize,
            on_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let jt: JoinType = match join_type {
                DfJoinType::Inner => JoinType::Inner,
                DfJoinType::Left => JoinType::Left,
                DfJoinType::Right => JoinType::Right,
                DfJoinType::Full => JoinType::Full,
                DfJoinType::LeftSemi => JoinType::LeftSemi,
                DfJoinType::LeftAnti => JoinType::LeftAnti,
                DfJoinType::RightSemi => JoinType::RightSemi,
                DfJoinType::RightAnti => JoinType::RightAnti,
            };

            let on_bytes = if on_addr != 0 && on_len > 0 {
                unsafe { std::slice::from_raw_parts(on_addr as *const u8, on_len) }
            } else {
                &[]
            };
            let on_exprs = deserialize_exprs(&self.ctx, on_bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;

            let new_df = self
                .df
                .clone()
                .join_on(right.df.clone(), jt, on_exprs)
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Join on failed: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Union of two DataFrames.
        pub fn union(&self, other: &DfDataFrame) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let new_df = self.df.clone().union(other.df.clone()).map_err(|e| {
                Box::new(DfError(format!("Union failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Union distinct of two DataFrames.
        pub fn union_distinct(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let new_df =
                self.df.clone().union_distinct(other.df.clone()).map_err(|e| {
                    Box::new(DfError(
                        format!("Union distinct failed: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Intersect of two DataFrames.
        pub fn intersect(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let new_df = self.df.clone().intersect(other.df.clone()).map_err(|e| {
                Box::new(DfError(
                    format!("Intersect failed: {}", e).into(),
                ))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Except (set difference) of two DataFrames.
        pub fn except_all(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let new_df = self.df.clone().except(other.df.clone()).map_err(|e| {
                Box::new(DfError(format!("Except failed: {}", e).into()))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Distinct rows.
        pub fn distinct(&self) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let new_df = self.df.clone().distinct().map_err(|e| {
                Box::new(DfError(
                    format!("Distinct failed: {}", e).into(),
                ))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        // ── Column manipulation ──

        /// Add or replace a column. expr is protobuf-encoded.
        pub fn with_column(
            &self,
            name: &DiplomatStr,
            bytes_addr: usize,
            bytes_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let bytes = if bytes_addr != 0 && bytes_len > 0 {
                unsafe { std::slice::from_raw_parts(bytes_addr as *const u8, bytes_len) }
            } else {
                &[]
            };
            let exprs = deserialize_exprs(&self.ctx, bytes)
                .map_err(|e| Box::new(DfError(e.into())))?;
            if exprs.is_empty() {
                return Err(Box::new(DfError(
                    "with_column requires exactly one expression".into(),
                )));
            }
            let new_df = self
                .df
                .clone()
                .with_column(name_str, exprs[0].clone())
                .map_err(|e| {
                    Box::new(DfError(
                        format!("with_column failed: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Rename a column.
        pub fn with_column_renamed(
            &self,
            old_name: &DiplomatStr,
            new_name: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let old_str = diplomat_str(old_name)?;
            let new_str = diplomat_str(new_name)?;
            let new_df = self
                .df
                .clone()
                .with_column_renamed(old_str, new_str)
                .map_err(|e| {
                    Box::new(DfError(
                        format!("with_column_renamed failed: {}", e).into(),
                    ))
                })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        /// Drop columns by name. names_addr points to null-separated UTF-8 column names.
        pub fn drop_columns(
            &self,
            names_addr: usize,
            names_len: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let col_strs = super::parse_null_separated(names_addr, names_len);
            let col_strs_ref: Vec<&str> = col_strs.iter().map(|s| s.as_str()).collect();
            let new_df = self.df.clone().drop_columns(&col_strs_ref).map_err(|e| {
                Box::new(DfError(
                    format!("drop_columns failed: {}", e).into(),
                ))
            })?;
            Ok(Box::new(DfDataFrame {
                df: new_df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            }))
        }

        // ── Write operations ──

        /// Write results to Parquet.
        pub fn write_parquet(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt
                .block_on(df.write_parquet(path_str, Default::default(), None))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Write parquet failed: {}", e).into(),
                    ))
                })?;
            Ok(())
        }

        /// Write results to CSV.
        pub fn write_csv(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt
                .block_on(df.write_csv(path_str, Default::default(), None))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Write CSV failed: {}", e).into(),
                    ))
                })?;
            Ok(())
        }

        /// Write results to JSON.
        pub fn write_json(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt
                .block_on(df.write_json(path_str, Default::default(), None))
                .map_err(|e| {
                    Box::new(DfError(
                        format!("Write JSON failed: {}", e).into(),
                    ))
                })?;
            Ok(())
        }
    }
}

/// Parse null-separated UTF-8 strings from a raw buffer (addr=0 or len=0 → empty vec).
fn parse_null_separated(addr: usize, len: usize) -> Vec<String> {
    if addr == 0 || len == 0 {
        return Vec::new();
    }
    // Safety: caller ensures addr points to valid memory of `len` bytes for this call
    let bytes = unsafe { std::slice::from_raw_parts(addr as *const u8, len) };
    bytes
        .split(|&b| b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| std::str::from_utf8(s).unwrap_or("").to_string())
        .collect()
}

/// Holds the live async stream for DfLazyRecordBatchStream.
///
/// Defined outside #[diplomat::bridge] because the macro cannot handle dyn-trait fields
/// (SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>).
pub(crate) struct LazyStreamState {
    pub(crate) stream: std::sync::Mutex<
        Option<datafusion::physical_plan::SendableRecordBatchStream>,
    >,
    pub(crate) rt: std::sync::Arc<tokio::runtime::Runtime>,
}
