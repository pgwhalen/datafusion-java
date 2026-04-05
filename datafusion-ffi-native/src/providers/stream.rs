use crate::bridge::ffi::DfRecordBatchReaderTrait;
use super::{import_schema, ErrorBuffer, RecordBatchReaderBridge};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfStream ──

pub struct ForeignDfStream<T: DfRecordBatchReaderTrait> {
    inner: T,
    schema: Arc<ArrowSchema>,
}

impl<T: DfRecordBatchReaderTrait> ForeignDfStream<T> {
    pub fn new(inner: T) -> Self {
        let schema = import_schema(inner.schema_address());
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

        let err = ErrorBuffer::new();

        let result = self
            .inner
            .next(array_addr, schema_addr, err.addr(), err.cap());

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
                Err(DataFusionError::External(
                    format!("Java next() callback failed: {}", err.read()).into(),
                ))
            }
        }
    }
}
