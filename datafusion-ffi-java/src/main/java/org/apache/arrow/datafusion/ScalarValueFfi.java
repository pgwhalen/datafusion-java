package org.apache.arrow.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * FFI deserialization logic for {@link ScalarValue}.
 *
 * <p>Scalar values are serialized as protobuf bytes on the Rust side and deserialized here using
 * {@link ScalarValueProtoConverter#fromProto}.
 */
final class ScalarValueFfi {

  private ScalarValueFfi() {}

  /**
   * Deserializes a ScalarValue from protobuf bytes stored in native memory.
   *
   * @param ptr pointer to the proto bytes in native memory
   * @param len length of the proto bytes
   * @return the deserialized ScalarValue
   */
  static ScalarValue fromProtoBytes(MemorySegment ptr, long len) {
    if (ptr.equals(MemorySegment.NULL) || len == 0) {
      return new ScalarValue.Null();
    }
    byte[] bytes = new byte[(int) len];
    MemorySegment.copy(ptr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, bytes, 0, (int) len);
    try {
      org.apache.arrow.datafusion.proto.ScalarValue proto =
          org.apache.arrow.datafusion.proto.ScalarValue.parseFrom(bytes);
      return ScalarValueProtoConverter.fromProto(proto);
    } catch (InvalidProtocolBufferException e) {
      throw new DataFusionException("Failed to decode ScalarValue protobuf", e);
    }
  }
}
