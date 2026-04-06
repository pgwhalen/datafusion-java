#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    /// File source trait: creates file openers.
    pub trait DfFileSourceTrait {
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

    /// Opaque wrapper for a file source backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfFileSource(pub(crate) Box<dyn crate::bridge::FileSourceBridge>);

    impl DfFileSource {
        /// Create from a DfFileSourceTrait impl and return the raw pointer address.
        pub fn create_raw(t: impl DfFileSourceTrait + 'static) -> usize {
            let wrapper = crate::file_source::ForeignDfFileSource::new(t);
            let boxed: Box<dyn crate::bridge::FileSourceBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfFileSource(boxed)));
            ptr as usize
        }
    }
}

use self::ffi::DfFileSourceTrait;
use crate::file_opener::ffi::DfFileOpener;
use crate::bridge::{FileOpenerBridge, FileSourceBridge};
use crate::upcall_utils::do_returning_upcall;
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

        Ok(do_returning_upcall::<DfFileOpener>(
            "Java create_file_opener failed",
            Box::new(|ea, ec| {
                self.inner.create_file_opener(
                    schema_addr,
                    proj_u32.as_ptr() as usize,
                    proj_u32.len(),
                    limit_val,
                    batch_size_val,
                    ea,
                    ec,
                )
            }),
        )?
        .0)
    }

}
