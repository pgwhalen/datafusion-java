//! Rust ExecutionPlan implementation that calls back into Java.

use crate::error::{callback_error, check_callback_result, set_error_return};
use crate::java_provider::{JavaExecutionPlanCallbacks, JavaRecordBatchReaderCallbacks};
use arrow::array::StructArray;
use arrow::datatypes::SchemaRef;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use std::any::Any;
use std::ffi::c_char;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A RecordBatch stream that calls back into Java.
pub struct JavaBackedRecordBatchStream {
    callbacks: *mut JavaRecordBatchReaderCallbacks,
    schema: SchemaRef,
}

impl JavaBackedRecordBatchStream {
    pub fn new(callbacks: *mut JavaRecordBatchReaderCallbacks, schema: SchemaRef) -> Self {
        Self { callbacks, schema }
    }
}

unsafe impl Send for JavaBackedRecordBatchStream {}

impl Stream for JavaBackedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        unsafe {
            let callbacks = &*self.callbacks;

            // Allocate FFI structs on the stack
            let mut array_out: FFI_ArrowArray = std::mem::zeroed();
            let mut schema_out: FFI_ArrowSchema = std::mem::zeroed();
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result = (callbacks.load_next_batch_fn)(
                callbacks.java_object,
                &mut array_out,
                &mut schema_out,
                &mut error_out,
            );

            match result {
                1 => {
                    // Batch available
                    match from_ffi(array_out, &schema_out) {
                        Ok(array_data) => {
                            let struct_array = StructArray::from(array_data);
                            let batch = RecordBatch::from(struct_array);
                            Poll::Ready(Some(Ok(batch)))
                        }
                        Err(e) => Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::ArrowError(Box::new(e), None),
                        ))),
                    }
                }
                0 => {
                    // End of stream
                    Poll::Ready(None)
                }
                _ => {
                    // Error
                    Poll::Ready(Some(Err(callback_error(
                        error_out,
                        "load next batch from Java RecordBatchReader",
                    ))))
                }
            }
        }
    }
}

impl RecordBatchStream for JavaBackedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for JavaBackedRecordBatchStream {
    fn drop(&mut self) {
        unsafe {
            let callbacks = &*self.callbacks;
            (callbacks.release_fn)(callbacks.java_object);
            // Free the callbacks struct itself
            drop(Box::from_raw(self.callbacks));
        }
    }
}

/// An ExecutionPlan that calls back into Java.
pub struct JavaBackedExecutionPlan {
    callbacks: *mut JavaExecutionPlanCallbacks,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl JavaBackedExecutionPlan {
    /// Create a new JavaBackedExecutionPlan from callback pointers.
    ///
    /// # Safety
    /// The callbacks pointer must be valid and point to a properly initialized struct.
    pub unsafe fn new(callbacks: *mut JavaExecutionPlanCallbacks) -> Result<Self> {
        let cb = &*callbacks;

        // Get schema from Java
        let mut schema_out: FFI_ArrowSchema = std::mem::zeroed();
        let mut error_out: *mut c_char = std::ptr::null_mut();

        let result = (cb.schema_fn)(cb.java_object, &mut schema_out, &mut error_out);
        check_callback_result(result, error_out, "get schema from Java ExecutionPlan")?;

        // Convert FFI schema to Arrow schema
        let schema = match arrow::datatypes::Schema::try_from(&schema_out) {
            Ok(s) => Arc::new(s),
            Err(e) => return Err(datafusion::error::DataFusionError::ArrowError(Box::new(e), None)),
        };

        // Get output partitioning count from Java
        let num_partitions = (cb.output_partitioning_fn)(cb.java_object);
        let partitioning = if num_partitions <= 1 {
            Partitioning::UnknownPartitioning(1)
        } else {
            Partitioning::UnknownPartitioning(num_partitions as usize)
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            callbacks,
            schema,
            properties,
        })
    }
}

unsafe impl Send for JavaBackedExecutionPlan {}
unsafe impl Sync for JavaBackedExecutionPlan {}

impl std::fmt::Debug for JavaBackedExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JavaBackedExecutionPlan")
    }
}

impl DisplayAs for JavaBackedExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "JavaBackedExecutionPlan")
    }
}

impl ExecutionPlan for JavaBackedExecutionPlan {
    fn name(&self) -> &str {
        "JavaBackedExecutionPlan"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // No children, so just return self
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unsafe {
            let cb = &*self.callbacks;

            let mut reader_out: *mut JavaRecordBatchReaderCallbacks = std::ptr::null_mut();
            let mut error_out: *mut c_char = std::ptr::null_mut();

            let result =
                (cb.execute_fn)(cb.java_object, partition, &mut reader_out, &mut error_out);

            check_callback_result(result, error_out, "execute Java ExecutionPlan")?;

            if reader_out.is_null() {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Java ExecutionPlan.execute returned null reader".to_string(),
                ));
            }

            let stream = JavaBackedRecordBatchStream::new(reader_out, Arc::clone(&self.schema));
            Ok(Box::pin(stream))
        }
    }
}

impl Drop for JavaBackedExecutionPlan {
    fn drop(&mut self) {
        unsafe {
            let callbacks = &*self.callbacks;
            (callbacks.release_fn)(callbacks.java_object);
            // Free the callbacks struct itself
            drop(Box::from_raw(self.callbacks));
        }
    }
}

/// Allocate a JavaExecutionPlanCallbacks struct.
///
/// # Safety
/// The returned pointer must be freed by Rust when the plan is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_execution_plan_callbacks() -> *mut JavaExecutionPlanCallbacks {
    Box::into_raw(Box::new(JavaExecutionPlanCallbacks {
        java_object: std::ptr::null_mut(),
        schema_fn: dummy_schema_fn,
        output_partitioning_fn: dummy_output_partitioning_fn,
        execute_fn: dummy_execute_fn,
        release_fn: dummy_release_fn,
    }))
}

/// Allocate a JavaRecordBatchReaderCallbacks struct.
///
/// # Safety
/// The returned pointer must be freed by Rust when the reader is dropped.
#[no_mangle]
pub extern "C" fn datafusion_alloc_record_batch_reader_callbacks(
) -> *mut JavaRecordBatchReaderCallbacks {
    Box::into_raw(Box::new(JavaRecordBatchReaderCallbacks {
        java_object: std::ptr::null_mut(),
        load_next_batch_fn: dummy_load_next_batch_fn,
        release_fn: dummy_release_fn,
    }))
}

// Dummy functions for initialization - Java will set the actual function pointers
unsafe extern "C" fn dummy_schema_fn(
    _java_object: *mut std::ffi::c_void,
    _schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "ExecutionPlan callbacks not initialized")
}

unsafe extern "C" fn dummy_output_partitioning_fn(_java_object: *mut std::ffi::c_void) -> i32 {
    1
}

unsafe extern "C" fn dummy_execute_fn(
    _java_object: *mut std::ffi::c_void,
    _partition: usize,
    _reader_out: *mut *mut JavaRecordBatchReaderCallbacks,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "ExecutionPlan callbacks not initialized")
}

unsafe extern "C" fn dummy_load_next_batch_fn(
    _java_object: *mut std::ffi::c_void,
    _array_out: *mut FFI_ArrowArray,
    _schema_out: *mut FFI_ArrowSchema,
    error_out: *mut *mut c_char,
) -> i32 {
    set_error_return(error_out, "RecordBatchReader callbacks not initialized")
}

unsafe extern "C" fn dummy_release_fn(_java_object: *mut std::ffi::c_void) {
    // Do nothing
}
