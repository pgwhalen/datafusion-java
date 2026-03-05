use crate::bridge::ffi::{DfSchemaTrait, DfTableProvider};
use super::{read_error, SchemaProviderBridge, TableProviderBridge};
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
        let cap: usize = 65536;
        let mut buf = vec![0u8; cap];
        let buf_addr = buf.as_mut_ptr() as usize;
        let written = self.inner.table_names_to(buf_addr, cap);
        if written <= 0 {
            return Vec::new();
        }
        let written = written as usize;
        let s = String::from_utf8_lossy(&buf[..written]);
        s.split('\0')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let name_bytes = name.as_bytes();
        self.inner.table_exists(name_bytes.as_ptr() as usize, name_bytes.len())
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let name_bytes = name.as_bytes();

        // Error buffer
        let error_cap: usize = 32768;
        let mut error_buf = vec![0u8; error_cap];
        let error_addr = error_buf.as_mut_ptr() as usize;

        let ptr = self.inner.table(name_bytes.as_ptr() as usize, name_bytes.len(), error_addr, error_cap);
        if ptr == 0 {
            // Check if Java reported an error
            let msg = unsafe { read_error(error_addr, error_cap) };
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
