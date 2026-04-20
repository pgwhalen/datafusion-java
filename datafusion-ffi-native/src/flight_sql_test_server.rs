//! In-process Flight SQL server used by the Java integration tests.
//!
//! Only compiled when the `test-server` feature is enabled. The server is backed by its own
//! DataFusion `SessionContext`; `register_memtable` feeds it Arrow batches (imported from the same
//! Arrow FFI helpers used everywhere else in this crate), and inbound `CommandStatementQuery`
//! requests are planned and executed against that context.
//!
//! This is test infrastructure only. It implements just enough of the Flight SQL protocol to
//! exercise the federated-catalog pushdown path: `do_handshake` (unauthenticated), schema
//! introspection via `SELECT * FROM <table> WHERE 1 = 0`, and statement execution via
//! `get_flight_info_statement` / `do_get_statement`.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    flight_service_server::FlightService, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Ticket,
};
use dashmap::DashMap;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use prost::Message as _;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

#[diplomat::bridge]
#[diplomat::abi_rename = "datafusion_{0}"]
pub mod ffi {
    use diplomat_runtime::DiplomatStr;

    /// In-process Flight SQL server backed by a DataFusion `SessionContext`. Tests start one
    /// per remote, seed it with `register_memtable`, and point a
    /// `DfFlightSqlFederatedCatalogFactory` at `port()`.
    #[diplomat::opaque]
    pub struct DfFlightSqlTestServer(pub(crate) super::ServerHandle);

    impl DfFlightSqlTestServer {
        /// Start a new Flight SQL test server bound to `127.0.0.1` on an OS-assigned port.
        #[diplomat::attr(auto, constructor)]
        pub fn start() -> Result<Box<Self>, Box<crate::bridge::ffi::DfError>> {
            Ok(Box::new(DfFlightSqlTestServer(super::start_impl()?)))
        }

        /// Returns the TCP port the server is listening on.
        pub fn port(&self) -> u16 {
            self.0.port
        }

        /// Register a `MemTable` under `name`, seeded with a single Arrow batch.
        pub fn register_memtable(
            &self,
            name: &DiplomatStr,
            batch: &crate::bridge::ffi::DfArrowBatch,
        ) -> Result<(), Box<crate::bridge::ffi::DfError>> {
            super::register_memtable_impl(&self.0, name, batch)
        }

        /// Returns the SQL statements the server has received, in order.
        pub fn received_sql(&self) -> Box<crate::bridge::ffi::DfStringArray> {
            super::received_sql_impl(&self.0)
        }
    }
}

pub(crate) struct ServerHandle {
    pub(crate) port: u16,
    pub(crate) state: Arc<TestServerState>,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    pub(crate) server_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        if let Some(jh) = self.server_handle.lock().unwrap().take() {
            let _ = self.rt.block_on(jh);
        }
    }
}

pub(crate) struct TestServerState {
    pub(crate) ctx: SessionContext,
    pub(crate) queries: DashMap<String, CachedResult>,
    pub(crate) received_sql: Mutex<Vec<String>>,
}

pub(crate) struct CachedResult {
    pub(crate) schema: SchemaRef,
    pub(crate) batches: Vec<RecordBatch>,
}

pub(crate) fn start_impl() -> Result<ServerHandle, Box<crate::bridge::ffi::DfError>> {
    let rt = Arc::new(
        Runtime::new().map_err(|e| format!("Failed to create test-server runtime: {e}"))?,
    );
    let state = Arc::new(TestServerState {
        ctx: SessionContext::new(),
        queries: DashMap::new(),
        received_sql: Mutex::new(Vec::new()),
    });

    let (port_tx, port_rx) = oneshot::channel::<Result<u16, String>>();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let state_for_task = Arc::clone(&state);

    let server_handle = rt.spawn(async move {
        let addr = match "127.0.0.1:0".parse() {
            Ok(a) => a,
            Err(e) => {
                let _ = port_tx.send(Err(format!("parse addr failed: {e}")));
                return;
            }
        };
        let incoming = match TcpIncoming::bind(addr) {
            Ok(i) => i,
            Err(e) => {
                let _ = port_tx.send(Err(format!("bind failed: {e}")));
                return;
            }
        };
        let local_addr = match incoming.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = port_tx.send(Err(format!("local_addr failed: {e}")));
                return;
            }
        };
        let _ = port_tx.send(Ok(local_addr.port()));

        let service = TestFlightSqlService {
            state: state_for_task,
        };
        let _ = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    let port = rt
        .block_on(port_rx)
        .map_err(|e| format!("test server never reported a port: {e}"))?
        .map_err(|e| format!("test server failed to start: {e}"))?;

    Ok(ServerHandle {
        port,
        state,
        rt,
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        server_handle: Mutex::new(Some(server_handle)),
    })
}

pub(crate) fn register_memtable_impl(
    handle: &ServerHandle,
    name: &diplomat_runtime::DiplomatStr,
    batch: &crate::bridge::ffi::DfArrowBatch,
) -> Result<(), Box<crate::bridge::ffi::DfError>> {
    // Access the pub(super) fields on DfArrowBatch from bridge.rs via a helper accessor pattern.
    let name_str = std::str::from_utf8(name)?;
    let (schema, record_batch) = arrow_batch_fields(batch);
    let table = MemTable::try_new(schema, vec![vec![record_batch]])?;
    handle
        .state
        .ctx
        .register_table(name_str, Arc::new(table))?;
    Ok(())
}

pub(crate) fn received_sql_impl(
    handle: &ServerHandle,
) -> Box<crate::bridge::ffi::DfStringArray> {
    let strings = handle.state.received_sql.lock().unwrap().clone();
    Box::new(crate::bridge::ffi::DfStringArray { strings })
}

fn arrow_batch_fields(
    batch: &crate::bridge::ffi::DfArrowBatch,
) -> (SchemaRef, RecordBatch) {
    // Safety-wise we are reading `pub(super)` fields from the same crate.
    (Arc::clone(&batch.schema), batch.batch.clone())
}

// ----- FlightSqlService implementation -----

#[derive(Clone)]
struct TestFlightSqlService {
    state: Arc<TestServerState>,
}

impl TestFlightSqlService {
    async fn prepare_and_cache(
        &self,
        sql: &str,
    ) -> Result<(String, SchemaRef, usize), Status> {
        self.state
            .received_sql
            .lock()
            .unwrap()
            .push(sql.to_string());
        let df = self
            .state
            .ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("sql failed: {e}")))?;
        let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("collect failed: {e}")))?;
        let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let handle = uuid::Uuid::new_v4().to_string();
        self.state.queries.insert(
            handle.clone(),
            CachedResult {
                schema: Arc::clone(&schema),
                batches,
            },
        );
        Ok((handle, schema, row_count))
    }
}

#[tonic::async_trait]
impl FlightSqlService for TestFlightSqlService {
    type FlightService = TestFlightSqlService;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // Unauthenticated: hand back a single empty payload.
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Default::default(),
        };
        let stream = futures::stream::iter(vec![Ok(response)]);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let (handle, schema, row_count) = self.prepare_and_cache(&query.query).await?;

        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: handle.into_bytes().into(),
            }
            .as_any()
            .encode_to_vec(),
        );
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(format!("encode schema failed: {e}")))?
            .with_descriptor(flight_descriptor)
            .with_endpoint(endpoint)
            .with_total_records(row_count as i64)
            .with_ordered(false);

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle_str = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("bad handle: {e}")))?;
        let cached = self
            .state
            .queries
            .remove(&handle_str)
            .map(|(_, v)| v)
            .ok_or_else(|| Status::not_found("unknown statement handle"))?;
        let batches = cached.batches;
        let schema = cached.schema;

        let batch_stream = futures::stream::iter(
            batches
                .into_iter()
                .map(Ok::<RecordBatch, arrow_flight::error::FlightError>),
        );
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream)
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

