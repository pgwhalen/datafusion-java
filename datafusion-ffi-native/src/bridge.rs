#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use datafusion::execution::context::SessionContext;
    use diplomat_runtime::DiplomatWrite;
    use std::fmt::Write;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    #[diplomat::opaque]
    #[diplomat::attr(auto, error)]
    pub struct DfError(pub(super) Box<str>);

    impl DfError {
        pub fn to_display(&self, write: &mut DiplomatWrite) {
            let _ = write!(write, "{}", self.0);
        }
    }

    #[diplomat::opaque]
    pub struct DfSessionContext {
        pub(super) ctx: SessionContext,
        pub(super) rt: Arc<Runtime>,
    }

    impl DfSessionContext {
        pub fn new() -> Box<DfSessionContext> {
            let rt = Runtime::new().expect("Failed to create Tokio runtime");
            let ctx = SessionContext::new();
            Box::new(DfSessionContext {
                ctx,
                rt: Arc::new(rt),
            })
        }

        pub fn sql(&self, query: &DiplomatStr) -> Result<Box<DfDataFrame>, Box<DfError>> {
            let sql_str = std::str::from_utf8(query)
                .map_err(|e| Box::new(DfError(format!("Invalid UTF-8: {}", e).into())))?;
            let df = self
                .rt
                .block_on(self.ctx.sql(sql_str))
                .map_err(|e| Box::new(DfError(format!("{}", e).into())))?;
            Ok(Box::new(DfDataFrame {
                df,
                rt: Arc::clone(&self.rt),
            }))
        }

        pub fn session_id(&self, write: &mut DiplomatWrite) {
            let _ = write!(write, "{}", self.ctx.session_id());
        }

        pub fn session_start_time_millis(&self) -> i64 {
            self.ctx.session_start_time().timestamp_millis()
        }
    }

    #[diplomat::opaque]
    pub struct DfDataFrame {
        pub(super) df: datafusion::dataframe::DataFrame,
        pub(super) rt: Arc<Runtime>,
    }
}
