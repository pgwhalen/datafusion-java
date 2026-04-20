//! Flight SQL federated catalog: a DataFusion-federation-aware `SQLExecutor` backed directly by
//! `arrow_flight::sql::client::FlightSqlServiceClient`.
//!
//! The resulting `CatalogProvider` exposes a single in-memory schema (name is caller-provided,
//! default `public`) with one federated `TableProvider` per caller-supplied table name. Because
//! each table flows through `SQLFederationProvider`, DataFusion pushes filters, projections,
//! joins, and aggregates down to the Flight SQL endpoint transparently. For a simple
//! "one baked-in query per table" provider with no pushdown, see `flight_sql_factory.rs`.

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};

    /// Factory for building a Flight SQL federated catalog. One factory call per remote
    /// endpoint: it performs a Handshake (if credentials were provided) and fetches each
    /// requested table's schema, then surfaces the whole remote as a `DfRustCatalogProvider`.
    #[diplomat::opaque]
    pub struct DfFlightSqlFederatedCatalogFactory;

    impl DfFlightSqlFederatedCatalogFactory {
        #[diplomat::attr(auto, constructor)]
        pub fn new() -> Box<Self> {
            Box::new(DfFlightSqlFederatedCatalogFactory)
        }

        /// Build a federated catalog.
        ///
        /// `entry_point` is a URL like `http://host:32010` or `grpc+tls://host:32010`.
        /// `compute_context` must be a stable unique identifier for this remote — federation
        /// will only collapse sub-plans that share a compute context.
        /// `schema_name` is the schema name under which `table_names` are exposed (typically
        /// `public`).
        /// `option_keys`/`option_values` accept the same `flight.sql.username`,
        /// `flight.sql.password`, and `flight.sql.header.<name>` keys as the simple
        /// `DfFlightSqlTableFactory`.
        pub fn build(
            &self,
            entry_point: &DiplomatStr,
            compute_context: &DiplomatStr,
            schema_name: &DiplomatStr,
            option_keys: &[DiplomatStrSlice],
            option_values: &[DiplomatStrSlice],
            table_names: &[DiplomatStrSlice],
        ) -> Result<
            Box<crate::rust_table_provider::ffi::DfRustCatalogProvider>,
            Box<crate::bridge::ffi::DfError>,
        > {
            crate::flight_sql_federation::build_impl(
                entry_point,
                compute_context,
                schema_name,
                option_keys,
                option_values,
                table_names,
            )
        }
    }
}

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider};
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLSchemaProvider};
use diplomat_runtime::{DiplomatStr, DiplomatStrSlice};
use futures::stream::{StreamExt, TryStreamExt};
use tonic::transport::{Channel, ClientTlsConfig};

use crate::bridge::ffi::DfError;
use crate::diplomat_util::{diplomat_str, diplomat_str_pairs_to_map, diplomat_str_slice_to_vec};
use crate::factory_runtime::factory_runtime;
use crate::rust_table_provider::ffi::DfRustCatalogProvider;

const USERNAME_KEY: &str = "flight.sql.username";
const PASSWORD_KEY: &str = "flight.sql.password";
const HEADER_PREFIX: &str = "flight.sql.header.";

struct FlightSqlExecutor {
    channel: Channel,
    headers: HashMap<String, String>,
    token: Option<String>,
    compute_context: String,
    dialect: Arc<dyn Dialect>,
}

impl fmt::Debug for FlightSqlExecutor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FlightSqlExecutor")
            .field("compute_context", &self.compute_context)
            .finish()
    }
}

impl FlightSqlExecutor {
    fn new_client(&self) -> FlightSqlServiceClient<Channel> {
        let mut client = FlightSqlServiceClient::new(self.channel.clone());
        for (k, v) in &self.headers {
            client.set_header(k, v);
        }
        if let Some(t) = &self.token {
            client.set_token(t.clone());
        }
        client
    }
}

fn to_df_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

#[async_trait]
impl SQLExecutor for FlightSqlExecutor {
    fn name(&self) -> &str {
        "flight_sql"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.compute_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::clone(&self.dialect)
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> DfResult<SendableRecordBatchStream> {
        let query = query.to_string();
        let mut client = self.new_client();
        let stream_schema = Arc::clone(&schema);

        let future_stream = async move {
            let info = client.execute(query, None).await.map_err(to_df_err)?;
            let mut sub_streams = Vec::with_capacity(info.endpoint.len());
            for ep in info.endpoint.into_iter() {
                let ticket = ep.ticket.ok_or_else(|| {
                    DataFusionError::Execution(
                        "Flight SQL endpoint missing ticket".to_string(),
                    )
                })?;
                let rb_stream = client.do_get(ticket).await.map_err(to_df_err)?;
                sub_streams.push(rb_stream.map_err(to_df_err));
            }
            let merged = futures::stream::iter(sub_streams).flatten();
            Ok::<SendableRecordBatchStream, DataFusionError>(Box::pin(
                RecordBatchStreamAdapter::new(Arc::clone(&stream_schema), merged),
            ))
        };

        let stream = futures::stream::once(future_stream).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DfResult<Vec<String>> {
        // Not used by our happy path (SQLSchemaProvider::new_with_tables). Callers that want
        // server-driven discovery would need to extend this to issue a Flight SQL GetTables.
        Ok(vec![])
    }

    async fn get_table_schema(&self, table_name: &str) -> DfResult<SchemaRef> {
        let mut client = self.new_client();
        let sql = format!("SELECT * FROM {} WHERE 1 = 0", table_name);
        let info = client.execute(sql, None).await.map_err(to_df_err)?;
        let schema = info.try_decode_schema().map_err(to_df_err)?;
        Ok(Arc::new(schema))
    }
}

async fn connect_channel(entry_point: &str) -> DfResult<Channel> {
    let tls_config = ClientTlsConfig::new().with_enabled_roots();
    Channel::from_shared(entry_point.to_string())
        .map_err(to_df_err)?
        .tls_config(tls_config)
        .map_err(to_df_err)?
        .connect()
        .await
        .map_err(to_df_err)
}

async fn build_executor(
    entry_point: String,
    compute_context: String,
    options: HashMap<String, String>,
) -> DfResult<FlightSqlExecutor> {
    let channel = connect_channel(&entry_point).await?;

    let mut headers = HashMap::new();
    for (k, v) in &options {
        if let Some(name) = k.strip_prefix(HEADER_PREFIX) {
            headers.insert(name.to_string(), v.clone());
        }
    }

    let mut token = None;
    if let Some(username) = options.get(USERNAME_KEY) {
        let password = options
            .get(PASSWORD_KEY)
            .cloned()
            .unwrap_or_default();
        let mut handshake_client = FlightSqlServiceClient::new(channel.clone());
        for (k, v) in &headers {
            handshake_client.set_header(k, v);
        }
        // Match FlightSqlDriver::metadata: handshake errors are ignored (unauthenticated servers
        // are a valid configuration). If the server did issue a token, we capture it.
        if handshake_client.handshake(username, &password).await.is_ok() {
            token = handshake_client.token().cloned();
        }
    }

    Ok(FlightSqlExecutor {
        channel,
        headers,
        token,
        compute_context,
        dialect: Arc::new(DefaultDialect {}),
    })
}

pub(crate) fn build_impl(
    entry_point: &DiplomatStr,
    compute_context: &DiplomatStr,
    schema_name: &DiplomatStr,
    option_keys: &[DiplomatStrSlice<'_>],
    option_values: &[DiplomatStrSlice<'_>],
    table_names: &[DiplomatStrSlice<'_>],
) -> Result<Box<DfRustCatalogProvider>, Box<DfError>> {
    let entry_str = diplomat_str(entry_point)?.to_owned();
    let compute_str = diplomat_str(compute_context)?.to_owned();
    let schema_str = diplomat_str(schema_name)?.to_owned();
    if schema_str.is_empty() {
        return Err("schema_name must not be empty".into());
    }
    let options = diplomat_str_pairs_to_map(option_keys, option_values)?;
    let tables = diplomat_str_slice_to_vec(table_names)?;

    let rt = factory_runtime();
    let executor = rt.block_on(build_executor(entry_str, compute_str, options))?;
    let federation_provider = Arc::new(SQLFederationProvider::new(Arc::new(executor)));
    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(rt.block_on(
        SQLSchemaProvider::new_with_tables(federation_provider, tables),
    )?);

    let catalog = MemoryCatalogProvider::new();
    catalog.register_schema(&schema_str, schema_provider)?;

    Ok(DfRustCatalogProvider::new(
        Arc::new(catalog) as Arc<dyn CatalogProvider>,
    ))
}
