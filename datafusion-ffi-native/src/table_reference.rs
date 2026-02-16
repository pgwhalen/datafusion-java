//! FFI helpers for serializing `TableReference` as protobuf bytes.

use datafusion::common::TableReference;
use datafusion_proto::protobuf as proto;
use prost::Message;

/// Serialize an `Option<TableReference>` to protobuf bytes.
///
/// Returns an empty `Vec` for `None`, otherwise returns the encoded
/// `proto::TableReference` message.
pub fn table_reference_to_proto_bytes(relation: &Option<TableReference>) -> Vec<u8> {
    match relation {
        None => Vec::new(),
        Some(rel) => {
            let proto = match rel {
                TableReference::Bare { table } => proto::TableReference {
                    table_reference_enum: Some(
                        proto::table_reference::TableReferenceEnum::Bare(
                            proto::BareTableReference {
                                table: table.to_string(),
                            },
                        ),
                    ),
                },
                TableReference::Partial { schema, table } => proto::TableReference {
                    table_reference_enum: Some(
                        proto::table_reference::TableReferenceEnum::Partial(
                            proto::PartialTableReference {
                                schema: schema.to_string(),
                                table: table.to_string(),
                            },
                        ),
                    ),
                },
                TableReference::Full {
                    catalog,
                    schema,
                    table,
                } => proto::TableReference {
                    table_reference_enum: Some(
                        proto::table_reference::TableReferenceEnum::Full(
                            proto::FullTableReference {
                                catalog: catalog.to_string(),
                                schema: schema.to_string(),
                                table: table.to_string(),
                            },
                        ),
                    ),
                },
            };
            proto.encode_to_vec()
        }
    }
}
