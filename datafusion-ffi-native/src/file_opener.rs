#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
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

    /// Opaque wrapper for a file opener backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfFileOpener(pub(crate) Box<dyn crate::bridge::FileOpenerBridge>);

    impl DfFileOpener {
        /// Create from a DfFileOpenerTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfFileOpenerTrait + 'static) -> usize {
            let wrapper = crate::file_opener::ForeignDfFileOpener::new(t);
            let boxed: Box<dyn crate::bridge::FileOpenerBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfFileOpener(boxed)));
            ptr as usize
        }
    }
}

use self::ffi::DfFileOpenerTrait;
use crate::stream::ffi::DfRecordBatchReader;
use crate::bridge::{FileOpenerBridge, RecordBatchReaderBridge};
use crate::upcall_utils::{do_returning_upcall, ErrorBuffer};
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

        Ok(do_returning_upcall::<DfRecordBatchReader>(
            "Java open failed",
            Box::new(|err: &ErrorBuffer| {
                self.inner.open(
                    path_bytes.as_ptr() as usize,
                    path_bytes.len(),
                    file_size,
                    range_start,
                    range_end,
                    err.addr(),
                    err.cap(),
                )
            }),
        )?
        .0)
    }
}
