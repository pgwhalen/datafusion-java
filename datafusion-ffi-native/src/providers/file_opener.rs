use crate::bridge::ffi::{DfFileOpenerTrait, DfRecordBatchReader};
use super::{do_returning_upcall, FileOpenerBridge, RecordBatchReaderBridge};
use datafusion::common::DataFusionError;
use std::fmt;

// ── ForeignDfFileOpener (bridge impl) ──

pub struct ForeignDfFileOpener<T: DfFileOpenerTrait> {
    inner: T,
}

impl<T: DfFileOpenerTrait> ForeignDfFileOpener<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfFileOpenerTrait> fmt::Debug for ForeignDfFileOpener<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfFileOpener").finish()
    }
}

unsafe impl<T: DfFileOpenerTrait> Send for ForeignDfFileOpener<T> {}
unsafe impl<T: DfFileOpenerTrait> Sync for ForeignDfFileOpener<T> {}

impl<T: DfFileOpenerTrait + 'static> FileOpenerBridge for ForeignDfFileOpener<T> {
    fn open(
        &self,
        path: &str,
        file_size: u64,
        range: Option<(i64, i64)>,
    ) -> Result<Box<dyn RecordBatchReaderBridge>, DataFusionError> {
        let path_bytes = path.as_bytes();
        let (range_start, range_end) = range.unwrap_or((0, 0));

        let boxed = unsafe {
            do_returning_upcall::<DfRecordBatchReader>(
                "Java open failed",
                Box::new(|ea, ec| {
                    self.inner.open(
                        path_bytes.as_ptr() as usize,
                        path_bytes.len(),
                        file_size,
                        range_start,
                        range_end,
                        ea,
                        ec,
                    )
                }),
            )
        }?;
        Ok(boxed.0)
    }
}
