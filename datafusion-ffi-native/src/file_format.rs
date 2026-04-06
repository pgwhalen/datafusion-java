use crate::bridge::{FileOpenerBridge, FileSourceBridge};
use crate::upcall_utils::do_returning_upcall;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::FFI_ArrowSchema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, GetExt, Result as DFResult};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat as DataFusionFileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileSource as DataFusionFileSource, FileStream,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;
use object_store::ObjectStore;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfFileFormat ──

pub struct ForeignDfFileFormat<T: crate::bridge::ffi::DfFileFormatTrait> {
    inner: T,
    schema: Arc<ArrowSchema>,
    extension: String,
}

impl<T: crate::bridge::ffi::DfFileFormatTrait> ForeignDfFileFormat<T> {
    pub fn new(inner: T, schema: Arc<ArrowSchema>, extension: String) -> Self {
        Self {
            inner,
            schema,
            extension,
        }
    }
}

impl<T: crate::bridge::ffi::DfFileFormatTrait> fmt::Debug for ForeignDfFileFormat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfFileFormat")
            .field("extension", &self.extension)
            .finish()
    }
}

unsafe impl<T: crate::bridge::ffi::DfFileFormatTrait> Send for ForeignDfFileFormat<T> {}
unsafe impl<T: crate::bridge::ffi::DfFileFormatTrait> Sync for ForeignDfFileFormat<T> {}

#[async_trait]
impl<T: crate::bridge::ffi::DfFileFormatTrait + 'static> DataFusionFileFormat
    for ForeignDfFileFormat<T>
{
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_ext(&self) -> String {
        self.extension.clone()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        Ok(format!(
            "{}{}",
            self.get_ext(),
            file_compression_type.get_ext()
        ))
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn file_source(
        &self,
        table_schema: datafusion_datasource::TableSchema,
    ) -> Arc<dyn DataFusionFileSource> {
        // Export the schema so Java can read it
        let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref())
            .expect("Failed to export schema for file_source");
        let schema_addr = &ffi_schema as *const FFI_ArrowSchema as usize;

        let source_bridge: Result<Arc<dyn FileSourceBridge>, String> =
            do_returning_upcall::<crate::file_source::ffi::DfFileSource>(
                "Java file_source failed",
                Box::new(|err_addr, err_cap| {
                    self.inner.file_source(schema_addr, err_addr, err_cap)
                }),
            )
            .map(|boxed| Arc::from(boxed.0) as Arc<dyn FileSourceBridge>)
            .map_err(|e| e.to_string());

        let projection =
            datafusion_datasource::projection::SplitProjection::unprojected(&table_schema);

        Arc::new(ForeignDfFileSourceWrapper {
            bridge: source_bridge,
            table_schema,
            schema: Arc::clone(&self.schema),
            projection,
            projection_pushed: false,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        })
    }

    async fn infer_schema(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[object_store::ObjectMeta],
    ) -> DFResult<Arc<ArrowSchema>> {
        Ok(Arc::clone(&self.schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: Arc<ArrowSchema>,
        _object: &object_store::ObjectMeta,
    ) -> DFResult<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn datafusion::catalog::Session,
        mut conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if conf.batch_size.is_none() {
            conf.batch_size = Some(state.config().batch_size());
        }
        let file_source = Arc::clone(conf.file_source());
        let projected_schema = conf.projected_schema()?;
        let projected_statistics =
            datafusion::common::Statistics::new_unknown(&projected_schema);
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(conf.file_groups.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Arc::new(ForeignDfFileExec {
            base_config: conf,
            file_source,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
            projected_statistics,
        }))
    }
}

// ── ForeignDfFileSourceWrapper (implements DataFusion's FileSource trait) ──

#[derive(Clone)]
struct ForeignDfFileSourceWrapper {
    bridge: Result<Arc<dyn FileSourceBridge>, String>,
    table_schema: datafusion_datasource::TableSchema,
    schema: Arc<ArrowSchema>,
    projection: datafusion_datasource::projection::SplitProjection,
    projection_pushed: bool,
    metrics: Arc<ExecutionPlanMetricsSet>,
}

unsafe impl Send for ForeignDfFileSourceWrapper {}
unsafe impl Sync for ForeignDfFileSourceWrapper {}

impl fmt::Debug for ForeignDfFileSourceWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfFileSourceWrapper").finish()
    }
}

impl DataFusionFileSource for ForeignDfFileSourceWrapper {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> DFResult<Arc<dyn datafusion::datasource::physical_plan::FileOpener>> {
        let bridge = self.bridge.as_ref().map_err(|e| {
            DataFusionError::Execution(format!("FileSource creation failed: {}", e))
        })?;

        let projection = if self.projection_pushed {
            Some(self.projection.file_indices.as_slice())
        } else {
            None
        };
        let limit = base_config.limit;
        let batch_size = base_config.batch_size;

        let opener_bridge = bridge
            .create_file_opener(&self.schema, projection, limit, batch_size)
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to create FileOpener: {}", e))
            })?;

        Ok(Arc::new(ForeignDfFileOpenerWrapper {
            bridge: Arc::from(opener_bridge),
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn DataFusionFileSource> {
        Arc::new(ForeignDfFileSourceWrapper {
            bridge: self.bridge.as_ref().map(Arc::clone).map_err(|e| e.clone()),
            table_schema: self.table_schema.clone(),
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            projection_pushed: self.projection_pushed,
            metrics: Arc::new(ExecutionPlanMetricsSet::new()),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        // Can't easily return a borrowed &str from bridge, so use a default
        "java"
    }

    fn try_pushdown_projection(
        &self,
        projection: &datafusion::physical_plan::projection::ProjectionExprs,
    ) -> DFResult<Option<Arc<dyn DataFusionFileSource>>> {
        use datafusion_datasource::projection::SplitProjection;
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        source.projection_pushed = true;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&datafusion::physical_plan::projection::ProjectionExprs> {
        Some(&self.projection.source)
    }
}

// ── ForeignDfFileOpenerWrapper ──

struct ForeignDfFileOpenerWrapper {
    bridge: Arc<dyn FileOpenerBridge>,
}

impl datafusion::datasource::physical_plan::FileOpener for ForeignDfFileOpenerWrapper {
    fn open(
        &self,
        partitioned_file: PartitionedFile,
    ) -> DFResult<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = DFResult<
                            futures::stream::BoxStream<'static, DFResult<RecordBatch>>,
                        >,
                    > + Send,
            >,
        >,
    > {
        let bridge = Arc::clone(&self.bridge);

        Ok(Box::pin(async move {
            let path = format!("/{}", partitioned_file.path().as_ref());
            let file_size = partitioned_file.object_meta.size as u64;
            let range = partitioned_file
                .range
                .as_ref()
                .map(|r| (r.start as i64, r.end as i64));

            let reader = bridge.open(&path, file_size, range).map_err(|e| {
                DataFusionError::Execution(format!("Failed to open file: {}", e))
            })?;

            let reader_schema = reader.schema();
            let batch_stream = stream::unfold(reader, |reader| async move {
                match reader.next_batch() {
                    Ok(Some(batch)) => Some((Ok(batch), reader)),
                    Ok(None) => None,
                    Err(e) => Some((Err(e), reader)),
                }
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                reader_schema,
                batch_stream,
            )) as futures::stream::BoxStream<'static, DFResult<RecordBatch>>)
        }))
    }
}

// ── ForeignDfFileExec ──

struct ForeignDfFileExec {
    base_config: FileScanConfig,
    file_source: Arc<dyn DataFusionFileSource>,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
    projected_statistics: datafusion::common::Statistics,
}

unsafe impl Send for ForeignDfFileExec {}
unsafe impl Sync for ForeignDfFileExec {}

impl fmt::Debug for ForeignDfFileExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfFileExec").finish()
    }
}

impl DisplayAs for ForeignDfFileExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ForeignDfFileExec")
    }
}

impl ExecutionPlan for ForeignDfFileExec {
    fn name(&self) -> &str {
        "ForeignDfFileExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let opener = self.file_source.create_file_opener(
            object_store,
            &self.base_config,
            partition,
        )?;

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn statistics(&self) -> DFResult<datafusion::common::Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}
