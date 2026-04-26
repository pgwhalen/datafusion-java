//! PostgreSQL `TableProvider` / `CatalogProvider` factory backed by
//! `datafusion_table_providers::postgres`. With the `postgres-federation` feature enabled on the
//! upstream crate, any table opened through here is federation-aware: register against a
//! `SessionContext::new_with_federation` and DataFusion pushes filters, projections, and joins
//! down to Postgres transparently.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

    /// Connection pool to a PostgreSQL database. Multiple tables opened from the same pool share
    /// the underlying `bb8` pool. Corresponds to
    /// `datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool`.
    #[diplomat::opaque]
    pub struct DfPostgresConnectionPool(
        pub(crate) std::sync::Arc<
            datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool,
        >,
    );

    impl DfPostgresConnectionPool {
        /// Open a new pool. Parameters are the upstream `PostgresConnectionPool::new` params
        /// (e.g. `host`, `user`, `db`, `pass`, `port`, `sslmode`, or `connection_string`). Keys
        /// and values must be parallel slices of equal length.
        pub fn open(
            option_keys: &[DiplomatStrSlice],
            option_values: &[DiplomatStrSlice],
        ) -> Result<Box<Self>, Box<crate::bridge::ffi::DfError>> {
            crate::postgres_factory::open_pool_impl(option_keys, option_values)
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
            crate::postgres_factory::open_table_impl(&self.0, table_reference)
        }

        /// Build a `CatalogProvider` exposing every schema the Postgres database reports.
        /// Suitable for `registerCatalog`.
        pub fn open_catalog(
            &self,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustCatalogProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::postgres_factory::open_catalog_impl(&self.0)
        }
    }
}

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, TableProvider};
use datafusion::sql::TableReference;
use datafusion_table_providers::common::DatabaseCatalogProvider;
use datafusion_table_providers::postgres::{DynPostgresConnectionPool, PostgresTableFactory};
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use datafusion_table_providers::util::secrets::to_secret_map;
use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

use crate::bridge::ffi::DfError;
use crate::diplomat_util::{diplomat_str, diplomat_str_pairs_to_map};
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::{DfRustCatalogProvider, DfRustTableProvider};

pub(crate) fn open_pool_impl(
    keys: &[DiplomatStrSlice],
    values: &[DiplomatStrSlice],
) -> Result<Box<ffi::DfPostgresConnectionPool>, Box<DfError>> {
    let params = to_secret_map(diplomat_str_pairs_to_map(keys, values)?);
    let pool = factory_runtime()
        .block_on(PostgresConnectionPool::new(params))
        .map_err(|e| format!("Postgres pool build failed: {}", e))?;
    Ok(Box::new(ffi::DfPostgresConnectionPool(Arc::new(pool))))
}

pub(crate) fn open_table_impl(
    pool: &Arc<PostgresConnectionPool>,
    table_reference: &DiplomatStr,
) -> Result<Box<DfRustTableProvider>, Box<DfError>> {
    let name = diplomat_str(table_reference)?.to_owned();
    let factory = PostgresTableFactory::new(Arc::clone(pool));
    let table = factory_runtime()
        .block_on(factory.table_provider(TableReference::from(name)))
        .map_err(|e| format!("Postgres open_table failed: {}", e))?;
    Ok(DfRustTableProvider::new(table as Arc<dyn TableProvider>))
}

pub(crate) fn open_catalog_impl(
    pool: &Arc<PostgresConnectionPool>,
) -> Result<Box<DfRustCatalogProvider>, Box<DfError>> {
    let dyn_pool: Arc<DynPostgresConnectionPool> = Arc::clone(pool) as _;
    let catalog = factory_runtime()
        .block_on(DatabaseCatalogProvider::try_new(dyn_pool))
        .map_err(|e| format!("Postgres catalog build failed: {}", e))?;
    Ok(DfRustCatalogProvider::new(
        Arc::new(catalog) as Arc<dyn CatalogProvider>,
    ))
}
