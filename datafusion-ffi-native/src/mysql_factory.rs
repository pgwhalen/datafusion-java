//! MySQL `TableProvider` / `CatalogProvider` factory backed by
//! `datafusion_table_providers::mysql`. With the `mysql-federation` feature enabled on the
//! upstream crate, any table opened through here is federation-aware: register against a
//! `SessionContext::new_with_federation` and DataFusion pushes filters, projections, and joins
//! down to MySQL transparently.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

    /// Connection pool to a MySQL database. Multiple tables opened from the same pool share the
    /// underlying `mysql_async` pool. Corresponds to
    /// `datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool`.
    #[diplomat::opaque]
    pub struct DfMysqlConnectionPool(
        pub(crate) std::sync::Arc<
            datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool,
        >,
    );

    impl DfMysqlConnectionPool {
        /// Open a new pool. Parameters are the upstream `MySQLConnectionPool::new` params (e.g.
        /// `host`, `user`, `db`, `pass`, `tcp_port`, `sslmode`, or `connection_string`). Keys and
        /// values must be parallel slices of equal length.
        pub fn open(
            option_keys: &[DiplomatStrSlice],
            option_values: &[DiplomatStrSlice],
        ) -> Result<Box<Self>, Box<crate::bridge::ffi::DfError>> {
            crate::mysql_factory::open_pool_impl(option_keys, option_values)
        }

        /// Open a single table as a `TableProvider` suitable for `registerTable`.
        /// `table_reference` is parsed by DataFusion (e.g. `schema.table`, `catalog.schema.table`,
        /// or just `table`).
        pub fn open_table(
            &self,
            table_reference: &DiplomatStr,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustTableProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::mysql_factory::open_table_impl(&self.0, table_reference)
        }

        /// Build a `CatalogProvider` exposing every schema the MySQL database reports.
        /// Suitable for `registerCatalog`.
        pub fn open_catalog(
            &self,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustCatalogProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::mysql_factory::open_catalog_impl(&self.0)
        }
    }
}

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::sql::TableReference;
use datafusion_table_providers::common::DatabaseCatalogProvider;
use datafusion_table_providers::mysql::{DynMySQLConnectionPool, MySQLTableFactory};
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

use crate::bridge::ffi::DfError;
use crate::diplomat_util::{diplomat_str, diplomat_str_pairs_to_map};
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::{DfRustCatalogProvider, DfRustTableProvider};

pub(crate) fn open_pool_impl(
    keys: &[DiplomatStrSlice],
    values: &[DiplomatStrSlice],
) -> Result<Box<ffi::DfMysqlConnectionPool>, Box<DfError>> {
    let params = to_secret_map(diplomat_str_pairs_to_map(keys, values)?);
    let pool = factory_runtime()
        .block_on(MySQLConnectionPool::new(params))
        .map_err(|e| format!("MySQL pool build failed: {}", e))?;
    Ok(Box::new(ffi::DfMysqlConnectionPool(Arc::new(pool))))
}

pub(crate) fn open_table_impl(
    pool: &Arc<MySQLConnectionPool>,
    table_reference: &DiplomatStr,
) -> Result<Box<DfRustTableProvider>, Box<DfError>> {
    let name = diplomat_str(table_reference)?.to_owned();
    let factory = MySQLTableFactory::new(Arc::clone(pool));
    let table = factory_runtime()
        .block_on(factory.table_provider(TableReference::from(name)))
        .map_err(|e| format!("MySQL open_table failed: {}", e))?;
    Ok(DfRustTableProvider::new(table as Arc<dyn TableProvider>))
}

pub(crate) fn open_catalog_impl(
    pool: &Arc<MySQLConnectionPool>,
) -> Result<Box<DfRustCatalogProvider>, Box<DfError>> {
    let dyn_pool: Arc<DynMySQLConnectionPool> = Arc::clone(pool) as _;
    let catalog = factory_runtime()
        .block_on(DatabaseCatalogProvider::try_new(dyn_pool))
        .map_err(|e| format!("MySQL catalog build failed: {}", e))?;
    Ok(DfRustCatalogProvider::new(
        Arc::new(catalog) as Arc<dyn CatalogProvider>,
    ))
}
