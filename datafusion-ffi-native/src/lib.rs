mod bridge;
mod catalog;
mod diplomat_util;
mod execution_plan;
mod factory_runtime;
mod file_format;
mod file_opener;
mod file_source;
#[cfg(feature = "duckdb")]
mod duckdb_factory;
#[cfg(feature = "flight")]
mod flight_sql_factory;
#[cfg(feature = "flight")]
mod flight_sql_federation;
#[cfg(feature = "test-server")]
mod flight_sql_test_server;
#[cfg(feature = "mysql")]
mod mysql_factory;
#[cfg(feature = "postgres")]
mod postgres_factory;
#[cfg(feature = "sqlite")]
mod sqlite_factory;
mod logical_plan;
mod rust_table_provider;
mod schema;
mod stream;
mod table;
mod udf;
mod upcall_utils;
mod var_provider;
