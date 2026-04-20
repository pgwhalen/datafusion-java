//! Shared multi-thread Tokio runtime used by Rust-side provider factories
//! (Flight SQL, Postgres, MySQL, ...) to `block_on` async construction work.
//!
//! Each `DfSessionContext` owns its own runtime (for query execution). This
//! runtime is separate — it exists only for provider construction, so the
//! per-session runtime stays dedicated to query work.

use std::sync::{Arc, OnceLock};

use tokio::runtime::Runtime;

static FACTORY_RT: OnceLock<Arc<Runtime>> = OnceLock::new();

pub(crate) fn factory_runtime() -> Arc<Runtime> {
    Arc::clone(
        FACTORY_RT.get_or_init(|| {
            Arc::new(Runtime::new().expect("Failed to create provider factory Tokio runtime"))
        }),
    )
}
