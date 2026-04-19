#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
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
        /// task_ctx_addr is a raw pointer to a Box<DfTaskContext> that Java takes
        /// ownership of and must close. It is non-zero for the lifetime of the call.
        fn execute(
            &self,
            partition: i32,
            task_ctx_addr: usize,
            error_addr: usize,
            error_cap: usize,
        ) -> usize;
    }

    /// Opaque wrapper for an execution plan backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfExecutionPlan(pub(crate) Box<dyn crate::bridge::ExecutionPlanBridge>);

    impl DfExecutionPlan {
        /// Create from a DfExecutionPlanTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfExecutionPlanTrait + 'static) -> usize {
            let wrapper = crate::execution_plan::ForeignDfPlan::new(t);
            let boxed: Box<dyn crate::bridge::ExecutionPlanBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfExecutionPlan(boxed)));
            ptr as usize
        }
    }
}

use self::ffi::DfExecutionPlanTrait;
use crate::stream::ffi::DfRecordBatchReader;
use crate::bridge::{import_schema, ExecutionPlanBridge};
use crate::upcall_utils::{do_returning_upcall, ErrorBuffer};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfPlan ──

pub struct ForeignDfPlan<T: DfExecutionPlanTrait> {
    inner: T,
    /// Cached properties
    properties: PlanProperties,
}

impl<T: DfExecutionPlanTrait> ForeignDfPlan<T> {
    pub fn new(inner: T) -> Self {
        let schema = import_schema(inner.schema_address());

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
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Transfer a boxed DfTaskContext to Java. Java owns it and must close().
        let task_ctx = Box::new(crate::bridge::ffi::DfTaskContext { ctx: context });
        let task_ctx_addr = Box::into_raw(task_ctx) as usize;
        let reader = do_returning_upcall::<DfRecordBatchReader>(
            "Java execute callback failed",
            Box::new(|err: &ErrorBuffer| {
                self.inner
                    .execute(partition as i32, task_ctx_addr, err.addr(), err.cap())
            }),
        )?;
        let reader_bridge = reader.0;
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
