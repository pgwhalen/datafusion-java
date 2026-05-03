//! DuckDB `TableProvider` factory backed by `datafusion_table_providers::duckdb`. Gated behind
//! the `duckdb` Cargo feature because DuckDB bundles a C library, making it more than a pure-Rust
//! drop-in.
//!
//! Only the `open_table` path is exposed — the catalog path would require naming
//! `Arc<DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>`
//! directly, and `duckdb`/`r2d2` would have to be added here as direct deps. That's left for a
//! follow-up if a real user asks for it.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::DiplomatStr;

    /// Open mode. `Memory` uses an in-process in-memory database; `File` opens (or creates) the
    /// database at `path`.
    #[diplomat::enum_convert(crate::duckdb_factory::RustDuckDbMode)]
    pub enum DfDuckdbMode {
        Memory,
        File,
    }

    /// Connection pool to a DuckDB database. Corresponds to
    /// `datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool`.
    #[diplomat::opaque]
    pub struct DfDuckdbConnectionPool(
        pub(crate) std::sync::Arc<
            datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
        >,
    );

    impl DfDuckdbConnectionPool {
        /// Open a new pool. `path` is ignored for `Memory` mode; for `File` mode it's the
        /// filesystem path. DuckDB is opened read-write.
        pub fn open(
            path: &DiplomatStr,
            mode: DfDuckdbMode,
        ) -> Result<Box<Self>, Box<crate::bridge::ffi::DfError>> {
            crate::duckdb_factory::open_pool_impl(path, mode)
        }

        /// Open a single table as a `TableProvider` suitable for `registerTable`. With the
        /// `duckdb-federation` upstream feature enabled, the returned provider is federation-aware.
        pub fn open_table(
            &self,
            table_name: &DiplomatStr,
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustTableProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::duckdb_factory::open_table_impl(&self.0, table_name)
        }
    }
}

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::{
    DuckDbConnectionPool, DuckDbConnectionPoolBuilder,
};
use diplomat_runtime::DiplomatStr;

use crate::bridge::ffi::DfError;
use crate::diplomat_util::diplomat_str;
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::DfRustTableProvider;

/// Mirror of `Mode` used solely for `#[diplomat::enum_convert]` — the upstream type is opaque to
/// the proc macro.
pub(crate) enum RustDuckDbMode {
    Memory,
    File,
}

pub(crate) fn open_pool_impl(
    path: &DiplomatStr,
    mode: ffi::DfDuckdbMode,
) -> Result<Box<ffi::DfDuckdbConnectionPool>, Box<DfError>> {
    // Use the builder rather than `new_memory` / `new_file` — the builder avoids having to name
    // the upstream `duckdb::AccessMode` enum, which is not re-exported by datafusion-table-
    // providers.
    let builder = match mode {
        ffi::DfDuckdbMode::Memory => DuckDbConnectionPoolBuilder::memory(),
        ffi::DfDuckdbMode::File => {
            let path_str = diplomat_str(path)?;
            DuckDbConnectionPoolBuilder::file(path_str)
        }
    };
    let pool = builder
        .build()
        .map_err(|e| format!("DuckDB pool build failed: {}", e))?;
    Ok(Box::new(ffi::DfDuckdbConnectionPool(Arc::new(pool))))
}

pub(crate) fn open_table_impl(
    pool: &Arc<DuckDbConnectionPool>,
    table_name: &DiplomatStr,
) -> Result<Box<DfRustTableProvider>, Box<DfError>> {
    let name = diplomat_str(table_name)?.to_owned();
    let factory = DuckDBTableFactory::new(Arc::clone(pool));
    let table = factory_runtime()
        .block_on(factory.table_provider(TableReference::from(name)))
        .map_err(|e| format!("DuckDB open_table failed: {}", e))?;
    Ok(DfRustTableProvider::new(table as Arc<dyn TableProvider>))
}
