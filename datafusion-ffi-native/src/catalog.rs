use crate::bridge::ffi::{DfCatalogTrait, DfStringArray};
use crate::schema::ffi::DfSchemaProvider;
use crate::bridge::SchemaProviderBridge;

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
        let sp = crate::upcall_utils::do_option_upcall::<DfSchemaProvider>(|| {
            self.inner.schema(name_bytes.as_ptr() as usize, name_bytes.len())
        })?;
        let arc: Arc<dyn SchemaProviderBridge> = Arc::from(sp.0);
        Some(arc as Arc<dyn SchemaProvider>)
    }
}
