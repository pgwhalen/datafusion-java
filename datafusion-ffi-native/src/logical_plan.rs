//! Internal helpers supporting the LogicalPlan FFI surface in [`crate::bridge`].
//!
//! The Diplomat-exposed opaques (`DfLogicalPlan`, `DfLogicalPlanBuilder`) and their `impl`
//! blocks must live inside the `#[diplomat::bridge]` module, so those stay in `bridge.rs`.
//! Everything in this file is Rust-only: a protobuf extension codec used by
//! `DfLogicalPlan::to_proto_bytes`, and small formatting helpers shared by the
//! TableScan/Statement/DDL fallback accessors.
//!
//! Keeping this code out of `bridge.rs` shrinks that already-unwieldy file and avoids mixing
//! internal plumbing with the FFI surface.

use std::fmt::Write;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{not_impl_err, Result as DFResult, TableReference};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use diplomat_runtime::DiplomatWrite;

use crate::bridge::ffi::DfTableRefType;

/// Extension codec used by [`crate::bridge::ffi::DfLogicalPlan::to_proto_bytes`].
///
/// Emits empty bytes for custom table providers (e.g. `MemTable`) and extensions so that
/// `LogicalPlanNode::try_from_logical_plan` succeeds for plans built against in-memory/custom
/// providers. The Java side re-reads provider-specific fields (table name, projection, filters,
/// fetch) via variant-specific bridge accessors and never round-trips these bytes back to Rust,
/// so the encoded form can safely be empty.
#[derive(Debug, Clone)]
pub(crate) struct JavaSerializationCodec;

impl LogicalExtensionCodec for JavaSerializationCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> DFResult<Extension> {
        not_impl_err!("JavaSerializationCodec does not decode")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> DFResult<()> {
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> DFResult<Arc<dyn TableProvider>> {
        not_impl_err!("JavaSerializationCodec does not decode")
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> DFResult<()> {
        Ok(())
    }
}

// ============================================================================
// TableReference helpers — small utilities shared by DDL/Statement fallback
// accessors in bridge.rs.
// ============================================================================

pub(crate) fn table_ref_type(tr: &TableReference) -> DfTableRefType {
    match tr {
        TableReference::Bare { .. } => DfTableRefType::Bare,
        TableReference::Partial { .. } => DfTableRefType::Partial,
        TableReference::Full { .. } => DfTableRefType::Full,
    }
}

pub(crate) fn write_table_name(tr: &TableReference, write: &mut DiplomatWrite) {
    let _ = write!(write, "{}", tr.table());
}

pub(crate) fn write_table_schema(tr: &TableReference, write: &mut DiplomatWrite) {
    match tr {
        TableReference::Partial { schema, .. } | TableReference::Full { schema, .. } => {
            let _ = write!(write, "{}", schema);
        }
        _ => {}
    }
}

pub(crate) fn write_table_catalog(tr: &TableReference, write: &mut DiplomatWrite) {
    if let TableReference::Full { catalog, .. } = tr {
        let _ = write!(write, "{}", catalog);
    }
}
