//! SQLite `TableProvider` / `CatalogProvider` factory backed by
//! `datafusion_table_providers::sqlite`. The `sqlite-federation` feature on the upstream crate
//! means any table opened through here is already federation-aware: registered with a
//! `SessionContext::new_with_federation`, DataFusion pushes filters/projections/joins down to
//! SQLite transparently.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::DiplomatStr;

    /// Open mode. `Memory` uses the standard in-process in-memory database; `File` opens (or
    /// creates) the file at `path`.
    #[diplomat::enum_convert(crate::sqlite_factory::RustSqliteMode)]
    pub enum DfSqliteMode {
        Memory,
        File,
    }

    /// Connection pool to a SQLite database. Multiple tables opened from the same pool share a
    /// single set of cached connections. Corresponds to
    /// `datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool`.
    #[diplomat::opaque]
    pub struct DfSqliteConnectionPool(
        pub(crate) std::sync::Arc<
            datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
        >,
    );

    impl DfSqliteConnectionPool {
        /// Open a new pool. `path` is ignored for `Memory` mode; for `File` mode it's the
        /// filesystem path. `busy_timeout_ms` is applied as SQLite's busy timeout.
        pub fn open(
            path: &DiplomatStr,
            mode: DfSqliteMode,
            busy_timeout_ms: u64,
        ) -> Result<Box<Self>, Box<crate::bridge::ffi::DfError>> {
            crate::sqlite_factory::open_pool_impl(path, mode, busy_timeout_ms)
        }

        /// Open a single table as a `TableProvider` suitable for `registerTable`.
        pub fn open_table(
            &self,
            table_name: &DiplomatStr,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustTableProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::sqlite_factory::open_table_impl(&self.0, table_name)
        }

        /// Build a `CatalogProvider` that exposes every schema the SQLite database reports.
        /// Suitable for `registerCatalog`.
        pub fn open_catalog(
            &self,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustCatalogProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::sqlite_factory::open_catalog_impl(&self.0)
        }
    }
}

use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::sql::TableReference;
use datafusion_table_providers::common::DatabaseCatalogProvider;
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::{
    SqliteConnectionPool, SqliteConnectionPoolFactory,
};
use datafusion_table_providers::sql::db_connection_pool::Mode as PoolMode;
use datafusion_table_providers::sqlite::SqliteTableFactory;
use diplomat_runtime::DiplomatStr;

use crate::bridge::ffi::DfError;
use crate::diplomat_util::diplomat_str;
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::{DfRustCatalogProvider, DfRustTableProvider};

/// Mirror of `Mode` used solely for `#[diplomat::enum_convert]` — the upstream type is opaque to
/// the proc macro.
pub(crate) enum RustSqliteMode {
    Memory,
    File,
}

impl From<RustSqliteMode> for PoolMode {
    fn from(value: RustSqliteMode) -> Self {
        match value {
            RustSqliteMode::Memory => PoolMode::Memory,
            RustSqliteMode::File => PoolMode::File,
        }
    }
}

impl From<PoolMode> for RustSqliteMode {
    fn from(value: PoolMode) -> Self {
        match value {
            PoolMode::Memory => RustSqliteMode::Memory,
            PoolMode::File => RustSqliteMode::File,
        }
    }
}

pub(crate) fn open_pool_impl(
    path: &DiplomatStr,
    mode: ffi::DfSqliteMode,
    busy_timeout_ms: u64,
) -> Result<Box<ffi::DfSqliteConnectionPool>, Box<DfError>> {
    let path_str = diplomat_str(path)?.to_owned();
    let mode: PoolMode = RustSqliteMode::from(mode).into();
    let busy = Duration::from_millis(busy_timeout_ms);
    let factory = SqliteConnectionPoolFactory::new(&path_str, mode, busy);
    let pool = factory_runtime()
        .block_on(factory.build())
        .map_err(|e| format!("SQLite pool build failed: {}", e))?;
    Ok(Box::new(ffi::DfSqliteConnectionPool(Arc::new(pool))))
}

pub(crate) fn open_table_impl(
    pool: &Arc<SqliteConnectionPool>,
    table_name: &DiplomatStr,
) -> Result<Box<DfRustTableProvider>, Box<DfError>> {
    let name = diplomat_str(table_name)?.to_owned();
    let factory = SqliteTableFactory::new(Arc::clone(pool));
    let table = factory_runtime()
        .block_on(factory.table_provider(TableReference::from(name)))
        .map_err(|e| format!("SQLite open_table failed: {}", e))?;
    let provider: Arc<dyn TableProvider> = table;
    Ok(DfRustTableProvider::new(provider))
}

pub(crate) fn open_catalog_impl(
    pool: &Arc<SqliteConnectionPool>,
) -> Result<Box<DfRustCatalogProvider>, Box<DfError>> {
    let dyn_pool: Arc<datafusion_table_providers::sqlite::DynSqliteConnectionPool> =
        Arc::clone(pool) as _;
    let catalog = factory_runtime()
        .block_on(DatabaseCatalogProvider::try_new(dyn_pool))
        .map_err(|e| format!("SQLite catalog build failed: {}", e))?;
    Ok(DfRustCatalogProvider::new(
        Arc::new(catalog) as Arc<dyn CatalogProvider>,
    ))
}
