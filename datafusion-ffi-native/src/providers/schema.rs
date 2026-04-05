use crate::bridge::ffi::{DfSchemaTrait, DfStringArray, DfTableProvider};
use super::{ErrorBuffer, SchemaProviderBridge, TableProviderBridge};
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfSchema ──

pub struct ForeignDfSchema<T: DfSchemaTrait> {
    inner: T,
}

impl<T: DfSchemaTrait> ForeignDfSchema<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfSchemaTrait> fmt::Debug for ForeignDfSchema<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfSchema").finish()
    }
}

unsafe impl<T: DfSchemaTrait> Send for ForeignDfSchema<T> {}
unsafe impl<T: DfSchemaTrait> Sync for ForeignDfSchema<T> {}

impl<T: DfSchemaTrait + 'static> SchemaProviderBridge for ForeignDfSchema<T> {}

#[async_trait]
impl<T: DfSchemaTrait + 'static> SchemaProvider for ForeignDfSchema<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        DfStringArray::take_from_raw(self.inner.table_names_raw())
    }

    fn table_exist(&self, name: &str) -> bool {
        let name_bytes = name.as_bytes();
        self.inner.table_exists(name_bytes.as_ptr() as usize, name_bytes.len())
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let name_bytes = name.as_bytes();

        let err = ErrorBuffer::new();

        let ptr = self.inner.table(name_bytes.as_ptr() as usize, name_bytes.len(), err.addr(), err.cap());
        if ptr == 0 {
            // Check if Java reported an error
            let msg = err.read();
            if !msg.is_empty() {
                return Err(DataFusionError::External(
                    format!("Java SchemaProvider.table() failed: {}", msg).into(),
                ));
            }
            return Ok(None);
        }
        // Reconstruct the DfTableProvider from the raw pointer
        let boxed = unsafe { Box::from_raw(ptr as *mut DfTableProvider) };
        let bridge: Box<dyn TableProviderBridge> = boxed.0;
        let arc: Arc<dyn TableProviderBridge> = Arc::from(bridge);
        Ok(Some(arc as Arc<dyn TableProvider>))
    }
}
