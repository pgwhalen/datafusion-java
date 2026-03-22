//! DataFusion trait wrappers that dispatch through Diplomat trait vtables.
//!
//! Each Foreign* struct stores a Diplomat-generated trait struct (vtable + data pointer)
//! and implements the corresponding DataFusion trait by calling through the vtable.

mod catalog;
mod file_format;
mod file_opener;
mod file_source;
mod plan;
mod schema;
mod stream;
mod table;
mod udf;

pub use catalog::*;
pub use file_format::*;
pub use file_opener::*;
pub use file_source::*;
pub use plan::*;
pub use schema::*;
pub use stream::*;
pub use table::*;
pub use udf::*;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use std::sync::Arc;

// ── Bridge traits (marker traits for type erasure) ──

pub trait SchemaProviderBridge: datafusion::catalog::SchemaProvider + Send + Sync {}
pub trait TableProviderBridge: datafusion::catalog::TableProvider + Send + Sync {}
pub trait ExecutionPlanBridge: datafusion::physical_plan::ExecutionPlan + Send + Sync {}
pub trait RecordBatchReaderBridge: Send + Sync {
    fn schema(&self) -> Arc<ArrowSchema>;
    fn next_batch(&self) -> Result<Option<RecordBatch>, DataFusionError>;
}

pub trait FileSourceBridge: Send + Sync {
    fn create_file_opener(
        &self,
        schema: &ArrowSchema,
        projection: Option<&[usize]>,
        limit: Option<usize>,
        batch_size: Option<usize>,
    ) -> Result<Box<dyn FileOpenerBridge>, DataFusionError>;
}

pub trait FileOpenerBridge: Send + Sync {
    fn open(
        &self,
        path: &str,
        file_size: u64,
        range: Option<(i64, i64)>,
    ) -> Result<Box<dyn RecordBatchReaderBridge>, DataFusionError>;
}

/// Read an error message from the error buffer. Returns empty string if addr is 0.
pub(crate) unsafe fn read_error(addr: usize, cap: usize) -> String {
    if addr == 0 || cap == 0 {
        return String::new();
    }
    // Scan for actual content (non-zero bytes)
    let slice = std::slice::from_raw_parts(addr as *const u8, cap);
    // Find the end of the message (first zero byte or end of slice)
    let len = slice.iter().position(|&b| b == 0).unwrap_or(cap);
    if len == 0 {
        return String::new();
    }
    String::from_utf8_lossy(&slice[..len]).into_owned()
}
