use crate::bridge::ffi::{DfCatalogTrait, DfSchemaProvider, DfStringArray};
use super::SchemaProviderBridge;

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

// ── ForeignDfCatalog ──

pub struct ForeignDfCatalog<T: DfCatalogTrait> {
    inner: T,
}

impl<T: DfCatalogTrait> ForeignDfCatalog<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: DfCatalogTrait> fmt::Debug for ForeignDfCatalog<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignDfCatalog").finish()
    }
}

unsafe impl<T: DfCatalogTrait> Send for ForeignDfCatalog<T> {}
unsafe impl<T: DfCatalogTrait> Sync for ForeignDfCatalog<T> {}

impl<T: DfCatalogTrait + 'static> CatalogProvider for ForeignDfCatalog<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        DfStringArray::take_from_raw(self.inner.schema_names_raw())
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let name_bytes = name.as_bytes();
        let schema_provider = super::do_option_upcall::<DfSchemaProvider>(|| {
            self.inner.schema(name_bytes.as_ptr() as usize, name_bytes.len())
        })?;
        let bridge: Box<dyn SchemaProviderBridge> = schema_provider.0;
        let arc: Arc<dyn SchemaProviderBridge> = Arc::from(bridge);
        Some(arc as Arc<dyn SchemaProvider>)
    }
}
