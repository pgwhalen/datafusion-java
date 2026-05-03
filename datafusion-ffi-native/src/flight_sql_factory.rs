//! Flight SQL table factory: wraps `datafusion_table_providers::flight::FlightTableFactory`
//! (with the bundled `FlightSqlDriver`) and exposes a single `open_table` method that
//! returns a `DfRustTableProvider` for the Java side to register.
//!
//! This produces a **plain** `TableProvider` — no federation. Filters are not pushed
//! down; DataFusion filters the returned rows locally. Use `DfFlightSqlFederatedCatalog`
//! (see `flight_sql_federation.rs`) for real cross-remote filter / join pushdown.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

    /// Flight SQL table factory. Wraps `datafusion_table_providers::flight::FlightTableFactory`
    /// configured with the bundled `FlightSqlDriver`.
    #[diplomat::opaque]
    pub struct DfFlightSqlTableFactory(
        pub(crate) datafusion_table_providers::flight::FlightTableFactory,
    );

    impl DfFlightSqlTableFactory {
        #[diplomat::attr(auto, constructor)]
        pub fn new() -> Box<Self> {
            let driver = std::sync::Arc::new(
                datafusion_table_providers::flight::sql::FlightSqlDriver::new(),
            );
            Box::new(DfFlightSqlTableFactory(
                datafusion_table_providers::flight::FlightTableFactory::new(driver),
            ))
        }

        /// Connect to a Flight SQL endpoint and return a `TableProvider` handle.
        ///
        /// `entry_point` is a URL like `http://host:32010` or `grpc+tls://host:32010`.
        /// `option_keys` / `option_values` are parallel slices of configuration options
        /// (e.g. `flight.sql.query`, `flight.sql.username`, `flight.sql.header.<name>`).
        pub fn open_table(
            &self,
            entry_point: &DiplomatStr,
            option_keys: &[DiplomatStrSlice],
            option_values: &[DiplomatStrSlice],
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustTableProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::flight_sql_factory::open_table_impl(
                &self.0,
                entry_point,
                option_keys,
                option_values,
            )
        }
    }
}

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_table_providers::flight::FlightTableFactory;
use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

use crate::bridge::ffi::DfError;
use crate::diplomat_util::{diplomat_str, diplomat_str_pairs_to_map};
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::DfRustTableProvider;

pub(crate) fn open_table_impl(
    factory: &FlightTableFactory,
    entry_point: &DiplomatStr,
    option_keys: &[DiplomatStrSlice],
    option_values: &[DiplomatStrSlice],
) -> Result<Box<DfRustTableProvider>, Box<DfError>> {
    let entry_str = diplomat_str(entry_point)?.to_owned();
    let options = diplomat_str_pairs_to_map(option_keys, option_values)?;
    let rt = factory_runtime();
    let table = rt
        .block_on(factory.open_table(entry_str, options))
        .map_err(|e| format!("Flight SQL open_table failed: {}", e))?;
    let provider: Arc<dyn TableProvider> = Arc::new(table);
    Ok(DfRustTableProvider::new(provider))
}
