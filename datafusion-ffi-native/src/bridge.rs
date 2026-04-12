#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use arrow::array::{Array, StructArray};
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, DataFusionError, JoinType};
    use datafusion::datasource::MemTable;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::context::SessionContext;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::execution::SessionState;
    use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SortExpr};
    use datafusion_proto::logical_plan::from_proto::{parse_exprs, parse_sorts};
    use datafusion_proto::logical_plan::to_proto::serialize_exprs;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
    use datafusion_proto::protobuf::{LogicalExprList, SortExprNodeCollection};
    use diplomat_runtime::{DiplomatStr, DiplomatStrSlice, DiplomatWrite};
    use prost::Message;
    use std::collections::HashMap;
    use std::fmt::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    // ============================================================================
    // Deserialize helpers (safe, no raw pointers)
    // ============================================================================

    fn deserialize_exprs(ctx: &SessionContext, bytes: &[u8]) -> Result<Vec<Expr>, Box<DfError>> {
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        let proto_list = LogicalExprList::decode(bytes)?;
        let codec = DefaultLogicalExtensionCodec {};
        Ok(parse_exprs(proto_list.expr.iter(), ctx, &codec)?)
    }

    fn deserialize_sort_exprs(ctx: &SessionContext, bytes: &[u8]) -> Result<Vec<SortExpr>, Box<DfError>> {
        if bytes.is_empty() {
            return Ok(vec![]);
        }
        let collection = SortExprNodeCollection::decode(bytes)?;
        let codec = DefaultLogicalExtensionCodec {};
        Ok(parse_sorts(collection.sort_expr_nodes.iter(), ctx, &codec)?)
    }

    fn diplomat_str(s: &DiplomatStr) -> Result<&str, Box<DfError>> {
        Ok(std::str::from_utf8(s)?)
    }

    /// Convert a slice of DiplomatStrSlice to Vec<String>.
    fn diplomat_str_slice_to_vec(slices: &[DiplomatStrSlice]) -> Result<Vec<String>, Box<DfError>> {
        slices
            .iter()
            .map(|s| {
                std::str::from_utf8(s)
                    .map(|s| s.to_string())
                    .map_err(|e| Box::new(DfError(e.to_string().into())))
            })
            .collect()
    }

    /// Parse session config from parallel key/value string slices.
    fn build_session_config(
        keys: &[DiplomatStrSlice],
        values: &[DiplomatStrSlice],
    ) -> Result<SessionConfig, Box<DfError>> {
        if keys.is_empty() {
            return Ok(SessionConfig::new());
        }
        let key_strs = diplomat_str_slice_to_vec(keys)?;
        let value_strs = diplomat_str_slice_to_vec(values)?;
        let mut settings = HashMap::with_capacity(key_strs.len());
        for (k, v) in key_strs.into_iter().zip(value_strs) {
            settings.insert(k, v);
        }
        Ok(SessionConfig::from_string_hash_map(&settings)?)
    }

    // ============================================================================
    // Diplomat enums
    // ============================================================================

    /// Join type for DataFrame join operations.
    #[allow(dead_code)] // Variants constructed from Java via Diplomat FFI
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

    /// Insert operation mode for DataFrame write operations.
    #[allow(dead_code)] // Variants constructed from Java via Diplomat FFI
    pub enum DfInsertOp {
        Append,
        Overwrite,
        Replace,
    }

    /// Guarantee type for literal guarantees.
    pub enum DfGuaranteeType {
        In,
        NotIn,
    }

    /// Table reference type for literal guarantees.
    pub enum DfTableRefType {
        None,
        Bare,
        Partial,
        Full,
    }

    /// Variable type for variable provider registration.
    #[allow(dead_code)] // Variants constructed from Java via Diplomat FFI
    pub enum DfVarType {
        System,
        UserDefined,
    }

    /// A source-code span with start/end line and column.
    #[diplomat::attr(auto, abi_compatible)]
    pub struct DfSpan {
        pub start_line: i64,
        pub start_col: i64,
        pub end_line: i64,
        pub end_col: i64,
    }

    /// A single literal guarantee item returned by get().
    pub struct DfLiteralGuarantee<'a> {
        pub column_name: DiplomatStrSlice<'a>,
        pub guarantee_type: DfGuaranteeType,
        pub table_ref_type: DfTableRefType,
        pub table_ref_table: DiplomatStrSlice<'a>,
        pub table_ref_schema: DiplomatStrSlice<'a>,
        pub table_ref_catalog: DiplomatStrSlice<'a>,
        pub literal_count: i64,
    }

    /// Options controlling how data is written from a DataFrame.
    pub struct DfWriteOptions {
        pub single_file_output: bool,
        pub insert_op: DfInsertOp,
    }

    // ============================================================================
    // Diplomat traits (only those used by DfSessionContext methods stay here;
    // the rest have moved to their respective providers/*.rs files)
    // ============================================================================

    // -- Scalar UDF --

    /// Scalar UDF trait: user-defined function callbacks.
    pub trait DfScalarUdfTrait {
        /// Write function name to buf at buf_addr (cap buf_cap). Returns bytes written.
        fn name_to(&self, buf_addr: usize, buf_cap: usize) -> i64;
        /// Returns volatility: 0=Immutable, 1=Stable, 2=Volatile.
        fn volatility(&self) -> i32;
        /// Compute return field from arg types. Writes FFI_ArrowSchema to out_schema_addr.
        fn return_field(
            &self,
            arg_types_addr: usize,
            arg_types_len: usize,
            out_schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
        /// Invoke the function.
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
        /// Coerce argument types.
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
        /// Create a file source. schema_addr is FFI_ArrowSchema address.
        /// Returns DfFileSource raw ptr, or 0 on error (check error buffer).
        fn file_source(
            &self,
            schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    // -- Table provider --

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
        /// Insert data into this table.
        fn insert_into(
            &self,
            session_addr: usize,
            input_stream_ptr: usize,
            insert_op: i32,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
        /// Delete rows matching the filter predicates.
        fn delete_from(
            &self,
            session_addr: usize,
            filters_addr: usize,
            filters_len: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
        /// Update rows matching the filter predicates with the given assignments.
        fn update(
            &self,
            session_addr: usize,
            col_names_ptr: usize,
            assign_exprs_addr: usize,
            assign_exprs_len: usize,
            filters_addr: usize,
            filters_len: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    // -- Catalog provider chain --

    /// Catalog provider trait: returns schema names and schema providers.
    pub trait DfCatalogTrait {
        /// Returns a raw pointer to a DfStringArray containing schema names.
        fn schema_names_raw(&self) -> usize;
        /// Returns DfSchemaProvider raw ptr (via createRaw downcall), or 0 for not found.
        fn schema(&self, name_addr: usize, name_len: usize) -> usize;
    }

    // -- Variable provider --

    /// Variable provider trait: resolves @user and @@system variables in SQL.
    pub trait DfVarProviderTrait {
        /// Get variable value. var_names_ptr is a DfStringArray raw pointer
        /// (Java takes ownership).
        /// Returns a raw pointer to a DfExprBytes containing proto-serialized
        /// ScalarValue, or 0 on error (after writing to error buffer).
        fn get_value(
            &self,
            var_names_ptr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;

        /// Get variable type. var_names_ptr is a DfStringArray raw pointer
        /// (Java takes ownership). Writes FFI_ArrowSchema to out_schema_addr
        /// if type is known.
        /// Returns 1 if type found, 0 if None, negative on error.
        fn get_type(
            &self,
            var_names_ptr: usize,
            out_schema_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> i32;
    }

    // ============================================================================
    // Opaque wrappers with create_raw factories
    // ============================================================================

    /// Opaque wrapper for a table provider backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfTableProvider(pub(crate) Box<dyn super::TableProviderBridge>);

    impl DfTableProvider {
        /// Create from a DfTableTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfTableTrait + 'static) -> usize {
            let wrapper = crate::table::ForeignDfTable::new(t);
            let boxed: Box<dyn super::TableProviderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfTableProvider(boxed)));
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

    // From impls so that `?` works directly with common error types
    impl From<DataFusionError> for Box<DfError> {
        fn from(e: DataFusionError) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<arrow::error::ArrowError> for Box<DfError> {
        fn from(e: arrow::error::ArrowError) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<prost::DecodeError> for Box<DfError> {
        fn from(e: prost::DecodeError) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<String> for Box<DfError> {
        fn from(e: String) -> Self {
            Box::new(DfError(e.into()))
        }
    }

    impl From<&str> for Box<DfError> {
        fn from(e: &str) -> Self {
            Box::new(DfError(e.into()))
        }
    }

    impl From<std::io::Error> for Box<DfError> {
        fn from(e: std::io::Error) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<std::str::Utf8Error> for Box<DfError> {
        fn from(e: std::str::Utf8Error) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<datafusion_proto::protobuf::FromProtoError> for Box<DfError> {
        fn from(e: datafusion_proto::protobuf::FromProtoError) -> Self {
            Box::new(DfError(e.to_string().into()))
        }
    }

    impl From<datafusion_proto::protobuf::ToProtoError> for Box<DfError> {
        fn from(e: datafusion_proto::protobuf::ToProtoError) -> Self {
            Box::new(DfError(e.to_string().into()))
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
                return Err("Null address for Arrow FFI struct".into());
            }
            unsafe {
                let ffi_schema = std::ptr::read(schema_addr as *mut FFI_ArrowSchema);
                let ffi_array = std::ptr::read(array_addr as *mut FFI_ArrowArray);
                let array_data = from_ffi(ffi_array, &ffi_schema)?;
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
                return Err("Null address for Arrow FFI schema".into());
            }
            unsafe {
                let ffi_schema = std::ptr::read(addr as *mut FFI_ArrowSchema);
                let schema = ArrowSchema::try_from(&ffi_schema)?;
                Ok(Box::new(DfArrowSchema {
                    schema: Arc::new(schema),
                }))
            }
        }

        /// Export this schema as FFI_ArrowSchema to a Java-provided address.
        pub fn export_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err("Null output address".into());
            }
            let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref())?;
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

        /// Create a DfExprBytes from a byte slice. Used by Java callbacks to
        /// return serialized data to Rust.
        pub fn new_from_slice(data: &[u8]) -> Box<DfExprBytes> {
            Box::new(DfExprBytes {
                bytes: data.to_vec(),
            })
        }

        /// Transfer ownership as a raw pointer. The caller must consume this
        /// via `take_from_raw` to avoid leaking memory.
        pub fn to_raw_ptr(&mut self) -> usize {
            let bytes = std::mem::take(&mut self.bytes);
            let boxed = Box::new(DfExprBytes { bytes });
            Box::into_raw(boxed) as usize
        }
    }

    /// Opaque wrapper for a list of strings, used where Diplomat cannot return
    /// slices of strings directly (return values, trait callbacks).
    #[diplomat::opaque]
    pub struct DfStringArray {
        pub(crate) strings: Vec<String>,
    }

    impl DfStringArray {
        /// Number of strings in the array.
        pub fn len(&self) -> usize {
            self.strings.len()
        }

        /// Get the string at the given index.
        /// Returns empty string if index is out of bounds.
        pub fn get(&self, idx: usize, write: &mut DiplomatWrite) {
            if let Some(s) = self.strings.get(idx) {
                let _ = write.write_str(s);
            }
        }

        /// Create an empty DfStringArray (for use in trait callbacks).
        pub fn new_empty() -> Box<DfStringArray> {
            Box::new(DfStringArray {
                strings: Vec::new(),
            })
        }

        /// Append a string to this array (for use in trait callbacks).
        pub fn push(&mut self, s: &DiplomatStr) {
            let string = std::str::from_utf8(s)
                .unwrap_or("")
                .to_string();
            self.strings.push(string);
        }

        /// Create a raw pointer copy of this array's data, transferring ownership
        /// to the caller. Used by trait callback implementations to return a
        /// DfStringArray across the FFI boundary as a usize.
        /// The caller must free via `from_raw_ptr`.
        pub fn to_raw_ptr(&mut self) -> usize {
            let strings = std::mem::take(&mut self.strings);
            let boxed = Box::new(DfStringArray { strings });
            Box::into_raw(boxed) as usize
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
            let rt_env = builder.build_arc()?;
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
                return Err("Null output address".into());
            }
            let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref())?;
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
            let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data())?;
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
        pub(crate) inner: super::LazyStreamState,
        pub(crate) schema: Arc<ArrowSchema>,
    }

    impl DfLazyRecordBatchStream {
        /// Export the stream's schema as FFI_ArrowSchema to a Java-provided address.
        pub fn schema_to(&self, out_addr: usize) -> Result<(), Box<DfError>> {
            if out_addr == 0 {
                return Err("Null output address".into());
            }
            let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref())?;
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
            let next_result = match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    // On a Tokio worker (re-entrant callback from insert_into) —
                    // block_in_place tells Tokio to spawn replacement workers as needed.
                    tokio::task::block_in_place(|| handle.block_on(stream.next()))
                }
                Err(_) => {
                    // Normal Java thread — use the stored runtime.
                    self.inner
                        .rt
                        .as_ref()
                        .expect("No runtime available outside async context")
                        .block_on(stream.next())
                }
            };
            match next_result {
                None => Ok(0),
                Some(Err(e)) => Err(e.into()),
                Some(Ok(batch)) => {
                    let struct_array: StructArray = batch.into();
                    let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data())?;
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
                .block_on(async { self.state.create_logical_plan(sql_str).await })?;
            Ok(Box::new(DfLogicalPlan { plan }))
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
    // DfLogicalPlanBuilder
    // ============================================================================

    /// Opaque wrapper for building LogicalPlans programmatically.
    #[diplomat::opaque]
    pub struct DfLogicalPlanBuilder {
        pub(super) plan: LogicalPlan,
        pub(super) ctx: SessionContext,
        pub(super) rt: Arc<Runtime>,
    }

    impl DfLogicalPlanBuilder {
        fn wrap(&self, plan: LogicalPlan) -> Box<DfLogicalPlanBuilder> {
            Box::new(DfLogicalPlanBuilder {
                plan,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            })
        }

        fn builder(&self) -> LogicalPlanBuilder {
            LogicalPlanBuilder::from(self.plan.clone())
        }

        /// Create an empty builder (no rows by default).
        pub fn empty(
            ctx: &DfSessionContext,
            produce_one_row: bool,
        ) -> Box<DfLogicalPlanBuilder> {
            let plan = LogicalPlanBuilder::empty(produce_one_row)
                .build()
                .expect("empty plan should not fail");
            Box::new(DfLogicalPlanBuilder {
                plan,
                ctx: ctx.ctx.clone(),
                rt: Arc::clone(&ctx.rt),
            })
        }

        /// Create a builder from an existing LogicalPlan.
        pub fn from_plan(
            ctx: &DfSessionContext,
            plan: &DfLogicalPlan,
        ) -> Box<DfLogicalPlanBuilder> {
            Box::new(DfLogicalPlanBuilder {
                plan: plan.plan.clone(),
                ctx: ctx.ctx.clone(),
                rt: Arc::clone(&ctx.rt),
            })
        }

        /// Project expressions (proto-encoded).
        pub fn project(&self, expr_bytes: &[u8]) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, expr_bytes)?;
            let plan = self.builder().project(exprs)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Filter by predicate (proto-encoded, single expr).
        pub fn filter(&self, expr_bytes: &[u8]) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, expr_bytes)?;
            if exprs.is_empty() {
                return Err("Filter requires exactly one predicate expression".into());
            }
            let plan = self.builder().filter(exprs[0].clone())?.build()?;
            Ok(self.wrap(plan))
        }

        /// Sort by proto-encoded sort expressions.
        pub fn sort(&self, sort_bytes: &[u8]) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let sort_exprs = deserialize_sort_exprs(&self.ctx, sort_bytes)?;
            let plan = self.builder().sort(sort_exprs)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Limit rows. fetch=-1 means no limit.
        pub fn limit(
            &self,
            skip: usize,
            fetch: i64,
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let fetch_option = if fetch < 0 { None } else { Some(fetch as usize) };
            let plan = self.builder().limit(skip, fetch_option)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Aggregate with proto-encoded group-by and aggregate expression lists.
        pub fn aggregate(
            &self,
            group_bytes: &[u8],
            aggr_bytes: &[u8],
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let group_exprs = deserialize_exprs(&self.ctx, group_bytes)?;
            let aggr_exprs = deserialize_exprs(&self.ctx, aggr_bytes)?;
            let plan = self.builder().aggregate(group_exprs, aggr_exprs)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Distinct rows.
        pub fn distinct(&self) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let plan = self.builder().distinct()?.build()?;
            Ok(self.wrap(plan))
        }

        /// Having clause (proto-encoded predicate).
        pub fn having(&self, expr_bytes: &[u8]) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, expr_bytes)?;
            if exprs.is_empty() {
                return Err("Having requires exactly one expression".into());
            }
            let plan = self.builder().having(exprs[0].clone())?.build()?;
            Ok(self.wrap(plan))
        }

        /// Window expressions (proto-encoded).
        pub fn window(
            &self,
            window_bytes: &[u8],
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, window_bytes)?;
            let plan = self.builder().window(exprs)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Subquery alias.
        pub fn alias(&self, name: &DiplomatStr) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let plan = self.builder().alias(name_str)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Explain plan.
        pub fn explain(
            &self,
            verbose: bool,
            analyze: bool,
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let plan = self.builder().explain(verbose, analyze)?.build()?;
            Ok(self.wrap(plan))
        }

        /// Join with another plan using column name pairs.
        pub fn join(
            &self,
            right: &DfLogicalPlan,
            join_type: DfJoinType,
            left_cols: &[DiplomatStrSlice],
            right_cols: &[DiplomatStrSlice],
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
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

            let left_strs = diplomat_str_slice_to_vec(left_cols)?;
            let right_strs = diplomat_str_slice_to_vec(right_cols)?;

            let keys: Vec<(String, String)> = left_strs
                .into_iter()
                .zip(right_strs)
                .collect();

            let plan = self.builder().join(
                right.plan.clone(),
                jt,
                (
                    keys.iter().map(|(l, _)| l.as_str()).collect::<Vec<_>>(),
                    keys.iter().map(|(_, r)| r.as_str()).collect::<Vec<_>>(),
                ),
                None,
            )?.build()?;
            Ok(self.wrap(plan))
        }

        /// Cross join with another plan.
        pub fn cross_join(
            &self,
            right: &DfLogicalPlan,
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let plan = self.builder().cross_join(right.plan.clone())?.build()?;
            Ok(self.wrap(plan))
        }

        /// Union with another plan.
        pub fn union(
            &self,
            other: &DfLogicalPlan,
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let plan = self.builder().union(other.plan.clone())?.build()?;
            Ok(self.wrap(plan))
        }

        /// Union distinct.
        pub fn union_distinct(
            &self,
            other: &DfLogicalPlan,
        ) -> Result<Box<DfLogicalPlanBuilder>, Box<DfError>> {
            let plan = self.builder().union_distinct(other.plan.clone())?.build()?;
            Ok(self.wrap(plan))
        }

        /// Intersect two plans.
        pub fn intersect(
            left: &DfLogicalPlan,
            right: &DfLogicalPlan,
            is_all: bool,
        ) -> Result<Box<DfLogicalPlan>, Box<DfError>> {
            let plan = LogicalPlanBuilder::intersect(
                left.plan.clone(),
                right.plan.clone(),
                is_all,
            )?;
            Ok(Box::new(DfLogicalPlan { plan }))
        }

        /// Except (set difference) two plans.
        pub fn except(
            left: &DfLogicalPlan,
            right: &DfLogicalPlan,
            is_all: bool,
        ) -> Result<Box<DfLogicalPlan>, Box<DfError>> {
            let plan = LogicalPlanBuilder::except(
                left.plan.clone(),
                right.plan.clone(),
                is_all,
            )?;
            Ok(Box::new(DfLogicalPlan { plan }))
        }

        /// Build the final LogicalPlan.
        pub fn build(&self) -> Result<Box<DfLogicalPlan>, Box<DfError>> {
            // The plan is already built (we store LogicalPlan, not LogicalPlanBuilder)
            Ok(Box::new(DfLogicalPlan {
                plan: self.plan.clone(),
            }))
        }
    }

    // ============================================================================
    // DfPhysicalExpr
    // ============================================================================

    /// Opaque wrapper for a physical expression.
    #[diplomat::opaque]
    pub struct DfPhysicalExpr {
        pub(super) expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    }

    impl DfPhysicalExpr {
        /// Create a physical expression from protobuf-encoded filter bytes and an Arrow schema.
        /// Uses a default SessionState for expression parsing and compilation.
        /// The filter bytes are a LogicalExprList proto. Expressions are conjoined with AND.
        /// schema_addr points to an FFI_ArrowSchema (read by reference, not consumed).
        pub fn from_proto_filters(
            filter_bytes: &[u8],
            schema_addr: usize,
        ) -> Result<Box<DfPhysicalExpr>, Box<DfError>> {
            if filter_bytes.is_empty() {
                return Err("No filter bytes provided".into());
            }
            if schema_addr == 0 {
                return Err("Null schema address".into());
            }

            let proto_list = LogicalExprList::decode(filter_bytes)?;

            // Use a default session state for parsing and physical expr creation
            let session_state = SessionContext::new().state();
            let codec = DefaultLogicalExtensionCodec {};
            let exprs: Vec<Expr> =
                parse_exprs(proto_list.expr.iter(), &session_state, &codec)?;

            if exprs.is_empty() {
                return Err("No filters after parsing".into());
            }

            // Import schema from FFI (by reference)
            let schema = unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                Arc::new(ArrowSchema::try_from(ffi_schema)?)
            };
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

            // Conjoin all filters with AND
            let combined = exprs.into_iter().reduce(|a, b| a.and(b)).unwrap();

            // Create physical expression
            let physical_expr =
                session_state.create_physical_expr(combined, &df_schema)?;

            Ok(Box::new(DfPhysicalExpr { expr: physical_expr }))
        }

        /// Analyze this physical expression for literal guarantees.
        pub fn analyze_guarantees(&self) -> Box<DfLiteralGuarantees> {
            use datafusion::common::TableReference;
            use datafusion::physical_expr::utils::LiteralGuarantee;

            let guarantees = LiteralGuarantee::analyze(&self.expr);
            let data: Vec<super::LiteralGuarantee> = guarantees
                .iter()
                .map(|g| {
                    let column_name = g.column.name.clone();
                    let guarantee_type = match g.guarantee {
                        datafusion::physical_expr::utils::Guarantee::In => DfGuaranteeType::In,
                        datafusion::physical_expr::utils::Guarantee::NotIn => {
                            DfGuaranteeType::NotIn
                        }
                    };

                    // Extract table reference parts as strings
                    let (table_ref_type, table_ref_table, table_ref_schema, table_ref_catalog) =
                        match &g.column.relation {
                            None => (
                                DfTableRefType::None,
                                String::new(),
                                String::new(),
                                String::new(),
                            ),
                            Some(TableReference::Bare { table }) => (
                                DfTableRefType::Bare,
                                table.to_string(),
                                String::new(),
                                String::new(),
                            ),
                            Some(TableReference::Partial { schema, table }) => (
                                DfTableRefType::Partial,
                                table.to_string(),
                                schema.to_string(),
                                String::new(),
                            ),
                            Some(TableReference::Full {
                                catalog,
                                schema,
                                table,
                            }) => (
                                DfTableRefType::Full,
                                table.to_string(),
                                schema.to_string(),
                                catalog.to_string(),
                            ),
                        };

                    let spans: Vec<DfSpan> = g
                        .column
                        .spans
                        .0
                        .iter()
                        .map(|s| DfSpan {
                            start_line: s.start.line as i64,
                            start_col: s.start.column as i64,
                            end_line: s.end.line as i64,
                            end_col: s.end.column as i64,
                        })
                        .collect();

                    // Sort literals for deterministic ordering, then serialize
                    let mut sorted_literals: Vec<datafusion::common::ScalarValue> =
                        g.literals.iter().cloned().collect();
                    sorted_literals.sort_by(|a, b| {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    let literal_protos: Vec<Vec<u8>> = sorted_literals
                        .iter()
                        .map(|sv| super::scalar_to_proto_bytes(sv).unwrap_or_default())
                        .collect();

                    super::LiteralGuarantee {
                        column_name,
                        guarantee_type,
                        table_ref_type,
                        table_ref_table,
                        table_ref_schema,
                        table_ref_catalog,
                        spans,
                        literal_protos,
                    }
                })
                .collect();

            Box::new(DfLiteralGuarantees { guarantees: data })
        }
    }

    // ============================================================================
    // DfLiteralGuarantees
    // ============================================================================

    /// Opaque wrapper holding the results of LiteralGuarantee::analyze().
    #[diplomat::opaque]
    pub struct DfLiteralGuarantees {
        pub(super) guarantees: Vec<super::LiteralGuarantee>,
    }

    impl DfLiteralGuarantees {
        /// Number of guarantees.
        pub fn count(&self) -> usize {
            self.guarantees.len()
        }

        /// Get the guarantee item at index idx as a struct.
        pub fn get<'a>(&'a self, idx: usize) -> Result<DfLiteralGuarantee<'a>, Box<DfError>> {
            let g = self.guarantees.get(idx).ok_or_else(|| {
                Box::<DfError>::from(format!("Guarantee index {} out of bounds", idx))
            })?;
            Ok(DfLiteralGuarantee {
                column_name: g.column_name.as_bytes().into(),
                guarantee_type: g.guarantee_type,
                table_ref_type: g.table_ref_type,
                table_ref_table: g.table_ref_table.as_bytes().into(),
                table_ref_schema: g.table_ref_schema.as_bytes().into(),
                table_ref_catalog: g.table_ref_catalog.as_bytes().into(),
                literal_count: g.literal_protos.len() as i64,
            })
        }

        /// Get the spans for guarantee at index idx.
        pub fn spans<'a>(&'a self, idx: usize) -> Result<&'a [DfSpan], Box<DfError>> {
            let g = self.guarantees.get(idx).ok_or_else(|| {
                Box::<DfError>::from(format!("Guarantee index {} out of bounds", idx))
            })?;
            Ok(&g.spans)
        }

        /// Get pre-serialized proto bytes for a specific literal.
        pub fn literal_proto_bytes(
            &self,
            g_idx: usize,
            l_idx: usize,
        ) -> Result<Box<DfExprBytes>, Box<DfError>> {
            let g = self.guarantees.get(g_idx).ok_or_else(|| {
                Box::<DfError>::from(format!("Guarantee index {} out of bounds", g_idx))
            })?;
            let bytes = g.literal_protos.get(l_idx).ok_or_else(|| {
                Box::<DfError>::from(format!("Literal index {} out of bounds", l_idx))
            })?;
            Ok(Box::new(DfExprBytes {
                bytes: bytes.clone(),
            }))
        }
    }

    // ============================================================================
    // DfSessionContext
    // ============================================================================

    #[diplomat::opaque]
    pub struct DfSessionContext {
        pub(crate) ctx: SessionContext,
        pub(crate) rt: Arc<Runtime>,
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
        /// keys and values are parallel string slices of config key-value pairs.
        pub fn new_with_config(
            keys: &[DiplomatStrSlice],
            values: &[DiplomatStrSlice],
        ) -> Result<Box<DfSessionContext>, Box<DfError>> {
            let config = build_session_config(keys, values)?;
            let rt = Runtime::new()?;
            let ctx = SessionContext::new_with_config(config);
            Ok(Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            }))
        }

        /// Create a SessionContext with configuration options and a custom RuntimeEnv.
        /// keys and values are parallel string slices of config key-value pairs.
        pub fn new_with_config_rt(
            keys: &[DiplomatStrSlice],
            values: &[DiplomatStrSlice],
            rt_env: &DfRuntimeEnv,
        ) -> Result<Box<DfSessionContext>, Box<DfError>> {
            let config = build_session_config(keys, values)?;
            let rt = Runtime::new()?;
            let ctx =
                SessionContext::new_with_config_rt(config, Arc::clone(&rt_env.rt_env));
            Ok(Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            }))
        }

        fn wrap_df(&self, df: datafusion::dataframe::DataFrame) -> Box<DfDataFrame> {
            Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            })
        }

        pub fn sql(&self, query: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let sql_str = diplomat_str(query)?;
            Ok(self.wrap_df(self.rt.block_on(self.ctx.sql(sql_str))?))
        }

        pub fn execute_logical_plan(
            &self,
            plan: &DfLogicalPlan,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self
                .wrap_df(self.rt.block_on(self.ctx.execute_logical_plan(plan.plan.clone()))?))
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
            )?;
            self.ctx.register_table(name_str, Arc::new(table))?;
            Ok(())
        }

        /// Register a table provider backed by a Java-implemented DfTableTrait.
        pub fn register_table_provider(
            &self,
            name: &DiplomatStr,
            table: impl DfTableTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let foreign = crate::table::ForeignDfTable::new(table);
            self.ctx.register_table(name_str, Arc::new(foreign))?;
            Ok(())
        }

        /// Register a scalar UDF backed by a Java-implemented DfScalarUdfTrait.
        pub fn register_udf(
            &self,
            udf: impl DfScalarUdfTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            let foreign = crate::udf::ForeignDfUdf::new(udf);
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
            urls: &[DiplomatStrSlice],
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

            if schema_addr == 0 {
                return Err("Null schema address".into());
            }
            let arrow_schema = unsafe {
                let ffi_schema = &*(schema_addr as *const FFI_ArrowSchema);
                Arc::new(ArrowSchema::try_from(ffi_schema)?)
            };

            let foreign_format = crate::file_format::ForeignDfFileFormat::new(
                format,
                Arc::clone(&arrow_schema),
                extension_str.clone(),
            );
            let format_arc: Arc<dyn FileFormat> = Arc::new(foreign_format);

            let options = ListingOptions::new(format_arc)
                .with_file_extension(extension_str)
                .with_collect_stat(collect_stat != 0)
                .with_target_partitions(target_partitions);

            let url_strs = diplomat_str_slice_to_vec(urls)?;
            let mut table_urls = Vec::with_capacity(url_strs.len());
            for url_str in &url_strs {
                let table_url = ListingTableUrl::parse(url_str)?;
                table_urls.push(table_url);
            }

            let config = ListingTableConfig::new_with_multi_paths(table_urls)
                .with_listing_options(options)
                .with_schema(arrow_schema);

            let table = ListingTable::try_new(config)?;
            self.ctx.register_table(name_str, Arc::new(table))?;

            Ok(())
        }

        /// Register a catalog backed by a Java-implemented DfCatalogTrait.
        pub fn register_catalog(
            &self,
            name: &DiplomatStr,
            catalog: impl DfCatalogTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let foreign = crate::catalog::ForeignDfCatalog::new(catalog);
            self.ctx.register_catalog(name_str, Arc::new(foreign));
            Ok(())
        }

        /// Register a variable provider backed by a Java-implemented DfVarProviderTrait.
        pub fn register_variable(
            &self,
            var_type: DfVarType,
            provider: impl DfVarProviderTrait + 'static,
        ) -> Result<(), Box<DfError>> {
            use datafusion::variable::VarType;
            let vt = match var_type {
                DfVarType::System => VarType::System,
                DfVarType::UserDefined => VarType::UserDefined,
            };
            let foreign = crate::var_provider::ForeignVarProvider::new(provider);
            self.ctx.register_variable(vt, Arc::new(foreign));
            Ok(())
        }

        /// Parse a SQL expression against a schema, returning serialized protobuf bytes.
        pub fn parse_sql_expr(
            &self,
            sql: &DiplomatStr,
            schema: &DfArrowSchema,
        ) -> Result<Box<DfExprBytes>, Box<DfError>> {
            let sql_str = diplomat_str(sql)?;
            let df_schema = DFSchema::try_from(schema.schema.as_ref().clone())?;
            let state = self.ctx.state();
            let expr = state.create_logical_expr(sql_str, &df_schema)?;
            let codec = DefaultLogicalExtensionCodec {};
            let serialized = serialize_exprs(&[expr], &codec)?;
            let proto_list = LogicalExprList { expr: serialized };
            let bytes = proto_list.encode_to_vec();
            Ok(Box::new(DfExprBytes { bytes }))
        }

        /// Check if a table exists by name.
        pub fn table_exist(&self, name: &DiplomatStr) -> Result<bool, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            Ok(self.ctx.table_exist(name_str)?)
        }

        /// Get a DataFrame for a registered table by name.
        pub fn table(&self, name: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            Ok(self.wrap_df(self.rt.block_on(self.ctx.table(name_str))?))
        }

        /// Read a Parquet file/directory into a DataFrame.
        pub fn read_parquet(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_parquet(path_str, Default::default()))?;
            Ok(self.wrap_df(df))
        }

        /// Read a CSV file/directory into a DataFrame.
        pub fn read_csv(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_csv(path_str, Default::default()))?;
            Ok(self.wrap_df(df))
        }

        /// Read a JSON file/directory into a DataFrame.
        pub fn read_json(
            &self,
            path: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self
                .rt
                .block_on(self.ctx.read_json(path_str, Default::default()))?;
            Ok(self.wrap_df(df))
        }

        /// Deregister a table by name. Returns true if a table was previously registered.
        pub fn deregister_table(&self, name: &DiplomatStr) -> Result<bool, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let result = self.ctx.deregister_table(name_str)?;
            Ok(result.is_some())
        }

        /// Register a CSV file/directory as a named table.
        /// options contains null-separated key\0value pairs.
        pub fn register_csv(
            &self,
            name: &DiplomatStr,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<(), Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let path_str = diplomat_str(path)?;
            let opts = super::parse_csv_options(options, schema_addr)?;
            self.rt.block_on(self.ctx.register_csv(name_str, path_str, opts))?;
            Ok(())
        }

        /// Register a Parquet file/directory as a named table.
        /// options contains null-separated key\0value pairs.
        pub fn register_parquet(
            &self,
            name: &DiplomatStr,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<(), Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let path_str = diplomat_str(path)?;
            let opts = super::parse_parquet_options(options, schema_addr)?;
            self.rt.block_on(self.ctx.register_parquet(name_str, path_str, opts))?;
            Ok(())
        }

        /// Register a JSON file/directory as a named table.
        /// options contains null-separated key\0value pairs.
        pub fn register_json(
            &self,
            name: &DiplomatStr,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<(), Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let path_str = diplomat_str(path)?;
            let opts = super::parse_json_options(options, schema_addr)?;
            self.rt.block_on(self.ctx.register_json(name_str, path_str, opts))?;
            Ok(())
        }

        /// Read a CSV file with options into a DataFrame.
        pub fn read_csv_with_options(
            &self,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let opts = super::parse_csv_options(options, schema_addr)?;
            let df = self.rt.block_on(self.ctx.read_csv(path_str, opts))?;
            Ok(self.wrap_df(df))
        }

        /// Read a Parquet file with options into a DataFrame.
        pub fn read_parquet_with_options(
            &self,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let opts = super::parse_parquet_options(options, schema_addr)?;
            let df = self.rt.block_on(self.ctx.read_parquet(path_str, opts))?;
            Ok(self.wrap_df(df))
        }

        /// Read a JSON file with options into a DataFrame.
        pub fn read_json_with_options(
            &self,
            path: &DiplomatStr,
            options: &[u8],
            schema_addr: usize,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let opts = super::parse_json_options(options, schema_addr)?;
            let df = self.rt.block_on(self.ctx.read_json(path_str, opts))?;
            Ok(self.wrap_df(df))
        }

        /// Return catalog names.
        pub fn catalog_names(&self) -> Box<DfStringArray> {
            let names = self.ctx.catalog_names();
            Box::new(DfStringArray { strings: names })
        }

        /// Return schema names for a catalog.
        pub fn catalog_schema_names(
            &self,
            catalog: &DiplomatStr,
        ) -> Result<Box<DfStringArray>, Box<DfError>> {
            let catalog_str = diplomat_str(catalog)?;
            let catalog_provider = self
                .ctx
                .catalog(catalog_str)
                .ok_or_else(|| DataFusionError::Plan(format!("Catalog '{}' not found", catalog_str)))?;
            let names = catalog_provider.schema_names();
            Ok(Box::new(DfStringArray { strings: names }))
        }

        /// Return table names for a catalog.schema.
        pub fn catalog_table_names(
            &self,
            catalog: &DiplomatStr,
            schema: &DiplomatStr,
        ) -> Result<Box<DfStringArray>, Box<DfError>> {
            let catalog_str = diplomat_str(catalog)?;
            let schema_str = diplomat_str(schema)?;
            let catalog_provider = self
                .ctx
                .catalog(catalog_str)
                .ok_or_else(|| DataFusionError::Plan(format!("Catalog '{}' not found", catalog_str)))?;
            let schema_provider = catalog_provider
                .schema(schema_str)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Schema '{}' not found in catalog '{}'",
                        schema_str, catalog_str
                    ))
                })?;
            let names = schema_provider.table_names();
            Ok(Box::new(DfStringArray { strings: names }))
        }

        /// Check if a table exists in a specific catalog.schema.
        pub fn catalog_table_exists(
            &self,
            catalog: &DiplomatStr,
            schema: &DiplomatStr,
            table: &DiplomatStr,
        ) -> Result<bool, Box<DfError>> {
            let catalog_str = diplomat_str(catalog)?;
            let schema_str = diplomat_str(schema)?;
            let table_str = diplomat_str(table)?;
            let catalog_provider = self
                .ctx
                .catalog(catalog_str)
                .ok_or_else(|| DataFusionError::Plan(format!("Catalog '{}' not found", catalog_str)))?;
            let schema_provider = catalog_provider
                .schema(schema_str)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Schema '{}' not found in catalog '{}'",
                        schema_str, catalog_str
                    ))
                })?;
            Ok(schema_provider.table_exist(table_str))
        }

        /// Get a snapshot of the session state.
        pub fn state(&self) -> Result<Box<DfSessionState>, Box<DfError>> {
            let state = self.ctx.state();
            let rt = Runtime::new()?;
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
        fn wrap(&self, df: datafusion::dataframe::DataFrame) -> Box<DfDataFrame> {
            Box::new(DfDataFrame {
                df,
                ctx: self.ctx.clone(),
                rt: Arc::clone(&self.rt),
            })
        }

        /// Collect all batches and format as a pretty-printed string.
        pub fn collect_to_string(&self, write: &mut DiplomatWrite) -> Result<(), Box<DfError>> {
            let df = self.df.clone();
            let batches = self.rt.block_on(df.collect())?;
            let formatted = arrow::util::pretty::pretty_format_batches(&batches)?;
            let _ = write!(write, "{}", formatted);
            Ok(())
        }

        // ── Execution methods ──

        /// Execute the DataFrame, collect all results, and return as a DfRecordBatchStream.
        pub fn collect_stream(&self) -> Result<Box<DfRecordBatchStream>, Box<DfError>> {
            let df = self.df.clone();
            let batches = self.rt.block_on(df.collect())?;
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
            let stream = self.rt.block_on(df.execute_stream())?;
            Ok(Box::new(DfLazyRecordBatchStream {
                inner: super::LazyStreamState {
                    stream: std::sync::Mutex::new(Some(stream)),
                    rt: Some(Arc::clone(&self.rt)),
                },
                schema,
            }))
        }

        /// Print DataFrame results to stdout.
        pub fn show(&self) -> Result<(), Box<DfError>> {
            let df = self.df.clone();
            self.rt.block_on(df.show())?;
            Ok(())
        }

        /// Get the row count.
        pub fn count(&self) -> Result<i64, Box<DfError>> {
            let df = self.df.clone();
            let count = self.rt.block_on(df.count())?;
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
                return Err("Null output address".into());
            }
            let arrow_schema = self.df.schema().as_arrow().as_ref().clone();
            let ffi_schema = FFI_ArrowSchema::try_from(&arrow_schema)?;
            unsafe {
                std::ptr::write(out_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(())
        }

        // ── Transformation methods ──

        /// Filter rows by a protobuf-encoded predicate expression.
        pub fn filter_bytes(
            &self,
            filter: &[u8],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, filter)?;
            if exprs.is_empty() {
                return Err("Filter requires exactly one predicate expression".into());
            }
            Ok(self.wrap(self.df.clone().filter(exprs[0].clone())?))
        }

        /// Project columns by protobuf-encoded expression list.
        pub fn select_bytes(
            &self,
            select: &[u8],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let exprs = deserialize_exprs(&self.ctx, select)?;
            Ok(self.wrap(self.df.clone().select(exprs)?))
        }

        /// Aggregate with protobuf-encoded group-by and aggregate expression lists.
        pub fn aggregate_bytes(
            &self,
            group: &[u8],
            aggr: &[u8],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let group_exprs = deserialize_exprs(&self.ctx, group)?;
            let aggr_exprs = deserialize_exprs(&self.ctx, aggr)?;
            Ok(self.wrap(self.df.clone().aggregate(group_exprs, aggr_exprs)?))
        }

        /// Sort by protobuf-encoded sort expressions.
        pub fn sort_bytes(
            &self,
            sort: &[u8],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let sort_exprs = deserialize_sort_exprs(&self.ctx, sort)?;
            Ok(self.wrap(self.df.clone().sort(sort_exprs)?))
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
            Ok(self.wrap(self.df.clone().limit(skip, fetch_option)?))
        }

        // ── Join and set operations ──

        /// Join on column names with optional protobuf filter bytes.
        pub fn join(
            &self,
            right: &DfDataFrame,
            join_type: DfJoinType,
            left_cols: &[DiplomatStrSlice],
            right_cols: &[DiplomatStrSlice],
            filter: &[u8],
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

            let left_strs = diplomat_str_slice_to_vec(left_cols)?;
            let right_strs = diplomat_str_slice_to_vec(right_cols)?;
            let left_col_strs: Vec<&str> = left_strs.iter().map(|s| s.as_str()).collect();
            let right_col_strs: Vec<&str> = right_strs.iter().map(|s| s.as_str()).collect();

            let filter = if filter.is_empty() {
                None
            } else {
                let exprs = deserialize_exprs(&self.ctx, filter)?;
                exprs.into_iter().next()
            };

            Ok(self.wrap(self.df.clone().join(
                right.df.clone(), jt, &left_col_strs, &right_col_strs, filter,
            )?))
        }

        /// Join on arbitrary protobuf-encoded expressions.
        pub fn join_on_bytes(
            &self,
            right: &DfDataFrame,
            join_type: DfJoinType,
            on: &[u8],
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

            let on_exprs = deserialize_exprs(&self.ctx, on)?;
            Ok(self.wrap(self.df.clone().join_on(right.df.clone(), jt, on_exprs)?))
        }

        /// Union of two DataFrames.
        pub fn union(&self, other: &DfDataFrame) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self.wrap(self.df.clone().union(other.df.clone())?))
        }

        /// Union distinct of two DataFrames.
        pub fn union_distinct(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self.wrap(self.df.clone().union_distinct(other.df.clone())?))
        }

        /// Intersect of two DataFrames.
        pub fn intersect(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self.wrap(self.df.clone().intersect(other.df.clone())?))
        }

        /// Except (set difference) of two DataFrames.
        pub fn except_all(
            &self,
            other: &DfDataFrame,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self.wrap(self.df.clone().except(other.df.clone())?))
        }

        /// Distinct rows.
        pub fn distinct(&self) -> Result<Box<DfDataFrame>, Box<DfError>> {
            Ok(self.wrap(self.df.clone().distinct()?))
        }

        // ── Column manipulation ──

        /// Add or replace a column. expr is protobuf-encoded.
        pub fn with_column(
            &self,
            name: &DiplomatStr,
            expr: &[u8],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let name_str = diplomat_str(name)?;
            let exprs = deserialize_exprs(&self.ctx, expr)?;
            if exprs.is_empty() {
                return Err("with_column requires exactly one expression".into());
            }
            Ok(self.wrap(self.df.clone().with_column(name_str, exprs[0].clone())?))
        }

        /// Rename a column.
        pub fn with_column_renamed(
            &self,
            old_name: &DiplomatStr,
            new_name: &DiplomatStr,
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let old_str = diplomat_str(old_name)?;
            let new_str = diplomat_str(new_name)?;
            Ok(self.wrap(self.df.clone().with_column_renamed(old_str, new_str)?))
        }

        /// Drop columns by name.
        pub fn drop_columns(
            &self,
            names: &[DiplomatStrSlice],
        ) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let col_strs = diplomat_str_slice_to_vec(names)?;
            let col_strs_ref: Vec<&str> = col_strs.iter().map(|s| s.as_str()).collect();
            Ok(self.wrap(self.df.clone().drop_columns(&col_strs_ref)?))
        }

        // ── Write operations ──

        /// Write results to Parquet.
        pub fn write_parquet(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_parquet(path_str, Default::default(), None))?;
            Ok(())
        }

        /// Write results to CSV.
        pub fn write_csv(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_csv(path_str, Default::default(), None))?;
            Ok(())
        }

        /// Write results to JSON.
        pub fn write_json(&self, path: &DiplomatStr) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_json(path_str, Default::default(), None))?;
            Ok(())
        }

        /// Write results to Parquet with options.
        pub fn write_parquet_with_options(
            &self,
            path: &DiplomatStr,
            write_opts: DfWriteOptions,
            partition_by: &[DiplomatStrSlice],
            format_opts: &[u8],
        ) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df_write_opts = super::build_write_options(&write_opts, partition_by)?;
            let parquet_opts = super::decode_parquet_write_options(format_opts)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_parquet(path_str, df_write_opts, parquet_opts))?;
            Ok(())
        }

        /// Write results to CSV with options.
        pub fn write_csv_with_options(
            &self,
            path: &DiplomatStr,
            write_opts: DfWriteOptions,
            partition_by: &[DiplomatStrSlice],
            format_opts: &[u8],
        ) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df_write_opts = super::build_write_options(&write_opts, partition_by)?;
            let csv_opts = super::decode_csv_write_options(format_opts)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_csv(path_str, df_write_opts, csv_opts))?;
            Ok(())
        }

        /// Write results to JSON with options.
        pub fn write_json_with_options(
            &self,
            path: &DiplomatStr,
            write_opts: DfWriteOptions,
            partition_by: &[DiplomatStrSlice],
            format_opts: &[u8],
        ) -> Result<(), Box<DfError>> {
            let path_str = diplomat_str(path)?;
            let df_write_opts = super::build_write_options(&write_opts, partition_by)?;
            let json_opts = super::decode_json_write_options(format_opts)?;
            let df = self.df.clone();
            self.rt.block_on(df.write_json(path_str, df_write_opts, json_opts))?;
            Ok(())
        }
    }
}

impl ffi::DfExprBytes {
    /// Reconstitute a `DfExprBytes` from a raw pointer previously created
    /// by `to_raw_ptr`, and extract its bytes.  Returns an empty Vec if
    /// the pointer is null (0).
    ///
    /// # Safety contained here
    /// The pointer must have been produced by `to_raw_ptr` and must not be
    /// used again after this call.
    pub(crate) fn take_from_raw(ptr: usize) -> Vec<u8> {
        if ptr == 0 {
            return Vec::new();
        }
        let boxed = unsafe { Box::from_raw(ptr as *mut ffi::DfExprBytes) };
        boxed.bytes
    }
}

impl ffi::DfStringArray {
    /// Reconstitute a `DfStringArray` from a raw pointer previously created
    /// by `to_raw_ptr`, and extract its strings.  Returns an empty Vec if
    /// the pointer is null (0).
    ///
    /// # Safety contained here
    /// The pointer must have been produced by `to_raw_ptr` and must not be
    /// used again after this call.
    pub(crate) fn take_from_raw(ptr: usize) -> Vec<String> {
        if ptr == 0 {
            return Vec::new();
        }
        let boxed = unsafe { Box::from_raw(ptr as *mut ffi::DfStringArray) };
        boxed.strings
    }
}

/// Pre-serialized data for a single LiteralGuarantee.
struct LiteralGuarantee {
    column_name: String,
    guarantee_type: ffi::DfGuaranteeType,
    table_ref_type: ffi::DfTableRefType,
    table_ref_table: String,
    table_ref_schema: String,
    table_ref_catalog: String,
    spans: Vec<ffi::DfSpan>,
    literal_protos: Vec<Vec<u8>>,
}

/// Serialize a ScalarValue to protobuf bytes.
fn scalar_to_proto_bytes(value: &datafusion::common::ScalarValue) -> Result<Vec<u8>, String> {
    let proto: datafusion_proto::protobuf::ScalarValue = value
        .try_into()
        .map_err(|e: datafusion_proto::protobuf::ToProtoError| {
            format!("Failed to convert ScalarValue to proto: {}", e)
        })?;
    Ok(prost::Message::encode_to_vec(&proto))
}


/// Import an Arrow schema from an FFI address, or return None if addr is 0.
fn import_schema_option(
    schema_addr: usize,
) -> Result<Option<std::sync::Arc<arrow::datatypes::Schema>>, Box<ffi::DfError>> {
    if schema_addr == 0 {
        return Ok(None);
    }
    let schema = unsafe {
        let ffi_schema = &*(schema_addr as *const arrow::ffi::FFI_ArrowSchema);
        arrow::datatypes::Schema::try_from(ffi_schema)?
    };
    Ok(Some(std::sync::Arc::new(schema)))
}

use prost::Message as _;

/// Build DataFrameWriteOptions from a DfWriteOptions and partition_by string slices.
fn build_write_options(
    opts: &ffi::DfWriteOptions,
    partition_by: &[diplomat_runtime::DiplomatStrSlice],
) -> Result<datafusion::dataframe::DataFrameWriteOptions, Box<ffi::DfError>> {
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::logical_expr::dml::InsertOp;

    let insert_op = match opts.insert_op {
        ffi::DfInsertOp::Append => InsertOp::Append,
        ffi::DfInsertOp::Overwrite => InsertOp::Overwrite,
        ffi::DfInsertOp::Replace => InsertOp::Replace,
    };

    let mut wo = DataFrameWriteOptions::new()
        .with_single_file_output(opts.single_file_output)
        .with_insert_operation(insert_op);

    if !partition_by.is_empty() {
        let cols: Vec<String> = partition_by
            .iter()
            .map(|s| {
                std::str::from_utf8(s)
                    .map(|s| s.to_string())
                    .map_err(|e| Box::new(ffi::DfError(e.to_string().into())))
            })
            .collect::<Result<_, _>>()?;
        wo = wo.with_partition_by(cols);
    }
    Ok(wo)
}

/// Decode protobuf bytes into TableParquetOptions for write.
fn decode_parquet_write_options(
    bytes: &[u8],
) -> Result<Option<datafusion::config::TableParquetOptions>, Box<ffi::DfError>> {
    use datafusion_proto::protobuf as proto;
    if bytes.is_empty() {
        return Ok(None);
    }
    let proto_opts = proto::TableParquetOptions::decode(bytes).map_err(|e| {
        Box::new(ffi::DfError(format!("Failed to decode parquet options: {}", e).into()))
    })?;
    let global: datafusion::config::ParquetOptions = match proto_opts.global {
        Some(ref g) => {
            use datafusion_proto::protobuf::parquet_options::*;
            // Start from defaults and only override fields that were explicitly set.
            // Proto3 scalars default to 0/""/false; oneof fields are Option.
            let mut opts = datafusion::config::ParquetOptions::default();
            if let Some(CompressionOpt::Compression(ref c)) = g.compression_opt {
                opts.compression = Some(c.clone());
            }
            if !g.writer_version.is_empty() {
                opts.writer_version = g.writer_version.parse().unwrap_or_default();
            }
            if let Some(EncodingOpt::Encoding(ref e)) = g.encoding_opt {
                opts.encoding = Some(e.clone());
            }
            if let Some(StatisticsEnabledOpt::StatisticsEnabled(ref s)) =
                g.statistics_enabled_opt
            {
                opts.statistics_enabled = Some(s.clone());
            }
            if !g.created_by.is_empty() {
                opts.created_by = g.created_by.clone();
            }
            if let Some(DictionaryEnabledOpt::DictionaryEnabled(d)) =
                g.dictionary_enabled_opt
            {
                opts.dictionary_enabled = Some(d);
            }
            if g.bloom_filter_on_write {
                opts.bloom_filter_on_write = true;
            }
            if g.skip_arrow_metadata {
                opts.skip_arrow_metadata = true;
            }
            if g.allow_single_file_parallelism {
                opts.allow_single_file_parallelism = true;
            }
            if g.data_pagesize_limit != 0 {
                opts.data_pagesize_limit = g.data_pagesize_limit as usize;
            }
            if g.write_batch_size != 0 {
                opts.write_batch_size = g.write_batch_size as usize;
            }
            if g.dictionary_page_size_limit != 0 {
                opts.dictionary_page_size_limit = g.dictionary_page_size_limit as usize;
            }
            if g.max_row_group_size != 0 {
                opts.max_row_group_size = g.max_row_group_size as usize;
            }
            if g.data_page_row_count_limit != 0 {
                opts.data_page_row_count_limit = g.data_page_row_count_limit as usize;
            }
            if g.maximum_parallel_row_group_writers != 0 {
                opts.maximum_parallel_row_group_writers =
                    g.maximum_parallel_row_group_writers as usize;
            }
            if g.maximum_buffered_record_batches_per_stream != 0 {
                opts.maximum_buffered_record_batches_per_stream =
                    g.maximum_buffered_record_batches_per_stream as usize;
            }
            if let Some(BloomFilterFppOpt::BloomFilterFpp(f)) = g.bloom_filter_fpp_opt {
                opts.bloom_filter_fpp = Some(f);
            }
            if let Some(BloomFilterNdvOpt::BloomFilterNdv(n)) = g.bloom_filter_ndv_opt {
                opts.bloom_filter_ndv = Some(n);
            }
            opts
        }
        None => datafusion::config::ParquetOptions::default(),
    };
    let mut column_specific_options = std::collections::HashMap::new();
    for col_opt in proto_opts.column_specific_options {
        if let Some(options) = col_opt.options {
            let col_opts: datafusion::config::ParquetColumnOptions = options.into();
            column_specific_options.insert(col_opt.column_name, col_opts);
        }
    }
    let opts = datafusion::config::TableParquetOptions {
        global,
        column_specific_options,
        key_value_metadata: proto_opts.key_value_metadata.into_iter().map(|(k, v)| (k, Some(v))).collect(),
        crypto: Default::default(),
    };
    Ok(Some(opts))
}

/// Decode protobuf bytes into CsvOptions for write.
fn decode_csv_write_options(
    bytes: &[u8],
) -> Result<Option<datafusion::config::CsvOptions>, Box<ffi::DfError>> {
    use datafusion_proto::protobuf as proto;
    if bytes.is_empty() {
        return Ok(None);
    }
    let proto_opts = proto::CsvOptions::decode(bytes).map_err(|e| {
        Box::new(ffi::DfError(format!("Failed to decode csv options: {}", e).into()))
    })?;
    let opts: datafusion::config::CsvOptions = (&proto_opts).into();
    Ok(Some(opts))
}

/// Decode protobuf bytes into JsonOptions for write.
fn decode_json_write_options(
    bytes: &[u8],
) -> Result<Option<datafusion::config::JsonOptions>, Box<ffi::DfError>> {
    use datafusion_proto::protobuf as proto;
    if bytes.is_empty() {
        return Ok(None);
    }
    let proto_opts = proto::JsonOptions::decode(bytes).map_err(|e| {
        Box::new(ffi::DfError(format!("Failed to decode json options: {}", e).into()))
    })?;
    let opts: datafusion::config::JsonOptions = (&proto_opts).into();
    Ok(Some(opts))
}

fn import_static_schema(
    schema_addr: usize,
) -> Result<Option<&'static arrow::datatypes::Schema>, Box<ffi::DfError>> {
    Ok(import_schema_option(schema_addr)?
        .map(|schema| &*Box::leak(Box::new((*schema).clone()))))
}

fn parse_csv_options<'a>(
    options: &[u8],
    schema_addr: usize,
) -> Result<datafusion::prelude::CsvReadOptions<'a>, Box<ffi::DfError>> {
    let mut opts = datafusion::prelude::CsvReadOptions::new();

    if !options.is_empty() {
        let proto = datafusion_proto::protobuf::CsvOptions::decode(options).map_err(|e| {
            Box::new(ffi::DfError(format!("Failed to decode csv options: {}", e).into()))
        })?;
        let csv_opts: datafusion::config::CsvOptions = (&proto).into();

        opts.has_header = csv_opts.has_header.unwrap_or(true);
        opts.delimiter = csv_opts.delimiter;
        opts.quote = csv_opts.quote;
        opts.terminator = csv_opts.terminator;
        opts.escape = csv_opts.escape;
        opts.comment = csv_opts.comment;
        opts.newlines_in_values = csv_opts.newlines_in_values.unwrap_or(false);
        if let Some(n) = csv_opts.schema_infer_max_rec {
            opts.schema_infer_max_records = n;
        }
        opts.file_compression_type = csv_opts.compression.into();
        if let Some(v) = csv_opts.null_regex {
            opts.null_regex = Some(v);
        }
        opts.truncated_rows = csv_opts.truncated_rows.unwrap_or(false);
    }

    opts.schema = import_static_schema(schema_addr)?;
    Ok(opts)
}

fn parse_parquet_options<'a>(
    options: &[u8],
    schema_addr: usize,
) -> Result<datafusion::prelude::ParquetReadOptions<'a>, Box<ffi::DfError>> {
    let mut opts = datafusion::prelude::ParquetReadOptions::default();

    if !options.is_empty() {
        let proto = datafusion_proto::protobuf::ParquetOptions::decode(options).map_err(|e| {
            Box::new(ffi::DfError(format!("Failed to decode parquet options: {}", e).into()))
        })?;
        // Read only the fields we need directly from proto, avoiding the full conversion
        // which can panic on empty writer_version
        opts.parquet_pruning = Some(proto.pruning);
        opts.skip_metadata = Some(proto.skip_metadata);
    }

    opts.schema = import_static_schema(schema_addr)?;
    Ok(opts)
}

fn parse_json_options<'a>(
    options: &[u8],
    schema_addr: usize,
) -> Result<datafusion::prelude::NdJsonReadOptions<'a>, Box<ffi::DfError>> {
    let mut opts = datafusion::prelude::NdJsonReadOptions::default();

    if !options.is_empty() {
        let proto = datafusion_proto::protobuf::JsonOptions::decode(options).map_err(|e| {
            Box::new(ffi::DfError(format!("Failed to decode json options: {}", e).into()))
        })?;
        let json_opts: datafusion::config::JsonOptions = (&proto).into();

        if let Some(n) = json_opts.schema_infer_max_rec {
            opts.schema_infer_max_records = n;
        }
        opts.file_compression_type = json_opts.compression.into();
    }

    opts.schema = import_static_schema(schema_addr)?;
    Ok(opts)
}

/// Holds the live async stream for DfLazyRecordBatchStream.
///
/// Defined outside #[diplomat::bridge] because the macro cannot handle dyn-trait fields
/// (SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>).
pub(crate) struct LazyStreamState {
    pub(crate) stream: std::sync::Mutex<
        Option<datafusion::physical_plan::SendableRecordBatchStream>,
    >,
    pub(crate) rt: Option<std::sync::Arc<tokio::runtime::Runtime>>,
}

// ============================================================================
// Bridge traits and upcall helpers (formerly in providers/mod.rs)
// ============================================================================

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

/// Import an Arrow schema from an FFI address.
/// Returns an empty schema if `addr` is 0 or conversion fails.
pub(crate) fn import_schema(addr: usize) -> std::sync::Arc<ArrowSchema> {
    if addr != 0 {
        unsafe {
            let ffi_schema = &*(addr as *const arrow::ffi::FFI_ArrowSchema);
            std::sync::Arc::new(
                ArrowSchema::try_from(ffi_schema).unwrap_or_else(|_| ArrowSchema::empty()),
            )
        }
    } else {
        std::sync::Arc::new(ArrowSchema::empty())
    }
}

// ── Bridge traits (marker traits for type erasure) ──

pub trait SchemaProviderBridge: datafusion::catalog::SchemaProvider + Send + Sync {}
pub trait TableProviderBridge: datafusion::catalog::TableProvider + Send + Sync {}
pub trait ExecutionPlanBridge: datafusion::physical_plan::ExecutionPlan + Send + Sync {}
pub trait RecordBatchReaderBridge: Send + Sync {
    fn schema(&self) -> std::sync::Arc<ArrowSchema>;
    fn next_batch(&self) -> Result<Option<RecordBatch>, datafusion::common::DataFusionError>;
}

pub trait FileSourceBridge: Send + Sync {
    fn create_file_opener(
        &self,
        schema: &ArrowSchema,
        projection: Option<&[usize]>,
        limit: Option<usize>,
        batch_size: Option<usize>,
    ) -> Result<Box<dyn FileOpenerBridge>, datafusion::common::DataFusionError>;
}

pub trait FileOpenerBridge: Send + Sync {
    fn open(
        &self,
        path: &str,
        file_size: u64,
        range: Option<(i64, i64)>,
    ) -> Result<Box<dyn RecordBatchReaderBridge>, datafusion::common::DataFusionError>;
}

