//! Opaques for Rust-constructed `Arc<dyn TableProvider>` / `SchemaProvider` / `CatalogProvider`.
//!
//! These are produced by per-backend factory modules (Flight SQL, Postgres, MySQL, ...) and
//! handed to `DfSessionContext::register_rust_*` methods. They exist purely to carry the Arc
//! across the FFI boundary without exposing generics in the Diplomat surface.

use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    /// Opaque wrapper around `Arc<dyn TableProvider>` constructed on the Rust side.
    #[diplomat::opaque]
    pub struct DfRustTableProvider(pub(crate) super::Arc<dyn super::TableProvider>);

    /// Opaque wrapper around `Arc<dyn SchemaProvider>` constructed on the Rust side.
    #[diplomat::opaque]
    pub struct DfRustSchemaProvider(pub(crate) super::Arc<dyn super::SchemaProvider>);

    /// Opaque wrapper around `Arc<dyn CatalogProvider>` constructed on the Rust side.
    #[diplomat::opaque]
    pub struct DfRustCatalogProvider(pub(crate) super::Arc<dyn super::CatalogProvider>);
}

// Factory helpers used by the per-backend provider modules (flight_sql_factory,
// postgres_factory, etc.) to hand an Arc across the Diplomat boundary. The
// `#[allow(dead_code)]` is present because these are unused until the provider
// modules land — remove it once any factory calls them.
#[allow(dead_code)]
impl ffi::DfRustTableProvider {
    pub(crate) fn new(provider: Arc<dyn TableProvider>) -> Box<Self> {
        Box::new(Self(provider))
    }
}

#[allow(dead_code)]
impl ffi::DfRustSchemaProvider {
    pub(crate) fn new(provider: Arc<dyn SchemaProvider>) -> Box<Self> {
        Box::new(Self(provider))
    }
}

#[allow(dead_code)]
impl ffi::DfRustCatalogProvider {
    pub(crate) fn new(provider: Arc<dyn CatalogProvider>) -> Box<Self> {
        Box::new(Self(provider))
    }
}
