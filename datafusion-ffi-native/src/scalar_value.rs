//! ScalarValue FFI — proto-based serialization for scalar values.

use datafusion::common::ScalarValue;
use prost::Message;

/// Serialize a ScalarValue to protobuf bytes.
pub(crate) fn scalar_to_proto_bytes(value: &ScalarValue) -> Result<Vec<u8>, String> {
    let proto: datafusion_proto::protobuf::ScalarValue = value
        .try_into()
        .map_err(|e: datafusion_proto::protobuf::ToProtoError| {
            format!("Failed to convert ScalarValue to proto: {}", e)
        })?;
    Ok(proto.encode_to_vec())
}
