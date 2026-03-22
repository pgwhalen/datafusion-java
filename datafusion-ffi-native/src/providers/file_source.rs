use crate::bridge::ffi::{DfFileOpener, DfFileSourceTrait};
use super::{read_error, FileOpenerBridge, FileSourceBridge};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;
use std::fmt;

// ── ForeignDfFileSource (bridge impl) ──

pub struct ForeignDfFileSource<T: DfFileSourceTrait> {
    inner: T,
}

impl<T: DfFileSourceTrait> ForeignDfFileSource<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfFileSourceTrait> fmt::Debug for ForeignDfFileSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfFileSource").finish()
    }
}

unsafe impl<T: DfFileSourceTrait> Send for ForeignDfFileSource<T> {}
unsafe impl<T: DfFileSourceTrait> Sync for ForeignDfFileSource<T> {}

impl<T: DfFileSourceTrait + 'static> FileSourceBridge for ForeignDfFileSource<T> {
    fn create_file_opener(
        &self,
        schema: &ArrowSchema,
        projection: Option<&[usize]>,
        limit: Option<usize>,
        batch_size: Option<usize>,
    ) -> Result<Box<dyn FileOpenerBridge>, DataFusionError> {
        // Export schema
        let ffi_schema = FFI_ArrowSchema::try_from(schema).map_err(|e| {
            DataFusionError::External(format!("Failed to export schema: {}", e).into())
        })?;
        let schema_addr = &ffi_schema as *const FFI_ArrowSchema as usize;

        // Projection: convert usize indices to u32
        let proj_u32: Vec<u32> = match projection {
            Some(p) => p.iter().map(|i| *i as u32).collect(),
            None => Vec::new(),
        };

        // Limit: -1 for None
        let limit_val: i64 = match limit {
            Some(l) => l as i64,
            None => -1,
        };

        // Batch size: -1 for None
        let batch_size_val: i64 = match batch_size {
            Some(bs) => bs as i64,
            None => -1,
        };

        // Error buffer
        let error_cap: usize = 32768;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let ptr = self.inner.create_file_opener(
            schema_addr,
            proj_u32.as_ptr() as usize,
            proj_u32.len(),
            limit_val,
            batch_size_val,
            error_addr,
            error_cap,
        );

        if ptr == 0 {
            let msg = unsafe { read_error(error_addr, error_cap) };
            return Err(DataFusionError::External(
                format!("Java create_file_opener failed: {}", msg).into(),
            ));
        }

        let boxed = unsafe { Box::from_raw(ptr as *mut DfFileOpener) };
        Ok(boxed.0)
    }

}
