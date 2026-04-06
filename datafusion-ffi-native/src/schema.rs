#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    /// Schema provider trait: returns table names and table providers.
    pub trait DfSchemaTrait {
        /// Returns a raw pointer to a DfStringArray containing table names.
        /// The caller takes ownership and must free via Box::from_raw.
        fn table_names_raw(&self) -> usize;
        /// Returns true if table exists.
        fn table_exists(&self, name_addr: usize, name_len: usize) -> bool;
        /// Returns DfTableProvider raw ptr, or 0 for not found/error (check error buffer).
        fn table(&self, name_addr: usize, name_len: usize, error_addr: usize, error_cap: usize) -> usize;
    }

    /// Opaque wrapper for a schema provider backed by a Diplomat trait.
    #[diplomat::opaque]
    pub struct DfSchemaProvider(pub(crate) Box<dyn crate::bridge::SchemaProviderBridge>);

    impl DfSchemaProvider {
        /// Create from a DfSchemaTrait impl and return the raw pointer address.
        /// Java uses this in re-entrant downcalls during upcalls.
        pub fn create_raw(t: impl DfSchemaTrait + 'static) -> usize {
            let wrapper = crate::schema::ForeignDfSchema::new(t);
            let boxed: Box<dyn crate::bridge::SchemaProviderBridge> = Box::new(wrapper);
            let ptr = Box::into_raw(Box::new(DfSchemaProvider(boxed)));
            ptr as usize
        }
    }
}

use self::ffi::DfSchemaTrait;
use crate::bridge::ffi::DfStringArray;
use crate::bridge::ffi::DfTableProvider;
use crate::bridge::{SchemaProviderBridge, TableProviderBridge};
use crate::upcall_utils::{do_option_returning_upcall, ErrorBuffer};
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
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

        let table = do_option_returning_upcall::<DfTableProvider>(
            "Java SchemaProvider.table() failed",
            Box::new(|err: &ErrorBuffer| self.inner.table(name_bytes.as_ptr() as usize, name_bytes.len(), err.addr(), err.cap())),
        )?;

        Ok(table.map(|t| {
            let arc: Arc<dyn TableProviderBridge> = Arc::from(t.0);
            arc as Arc<dyn TableProvider>
        }))
    }
}
