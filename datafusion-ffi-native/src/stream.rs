#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
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

    /// Opaque wrapper for a record batch reader backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfRecordBatchReader(pub(crate) Box<dyn crate::bridge::RecordBatchReaderBridge>);

    impl DfRecordBatchReader {
        /// Create from a DfRecordBatchReaderTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfRecordBatchReaderTrait + 'static) -> usize {
            let wrapper = crate::stream::ForeignDfStream::new(t);
            let boxed: Box<dyn crate::bridge::RecordBatchReaderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfRecordBatchReader(boxed)));
            ptr as usize
        }
    }
}

use self::ffi::DfRecordBatchReaderTrait;
use crate::bridge::{import_schema, RecordBatchReaderBridge};
use crate::upcall_utils::ErrorBuffer;
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
