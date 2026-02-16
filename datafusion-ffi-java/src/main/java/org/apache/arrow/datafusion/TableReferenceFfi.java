package org.apache.arrow.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * FFI utilities for reading and converting {@link TableReference} values.
 *
 * <p>This is separated from {@link TableReference} so it can be reused wherever table references
 * appear in FFI structs (e.g., literal guarantees, column references) and in proto conversion
 * (e.g., expression serialization).
 */
final class TableReferenceFfi {

  private TableReferenceFfi() {}

  /**
   * Reads a {@link TableReference} from proto bytes in native memory.
   *
   * @param bytesPtr pointer to the proto bytes, or {@code MemorySegment.NULL} for no relation
   * @param len length of the proto bytes
   * @return the table reference, or null if the pointer is null or length is 0
   * @throws DataFusionException if proto decoding fails
   */
  static TableReference readTableReference(MemorySegment bytesPtr, long len) {
    if (bytesPtr.equals(MemorySegment.NULL) || len == 0) {
      return null;
    }
    byte[] bytes = new byte[(int) len];
    MemorySegment.copy(bytesPtr.reinterpret(len), ValueLayout.JAVA_BYTE, 0, bytes, 0, (int) len);
    try {
      return convertTableReference(
          org.apache.arrow.datafusion.proto.TableReference.parseFrom(bytes));
    } catch (InvalidProtocolBufferException e) {
      throw new DataFusionException("Failed to decode TableReference protobuf", e);
    }
  }

  /**
   * Converts a proto {@link org.apache.arrow.datafusion.proto.TableReference} to a Java {@link
   * TableReference}.
   *
   * @param proto the proto table reference
   * @return the Java table reference, or null if proto is null or has no enum set
   */
  static TableReference convertTableReference(
      org.apache.arrow.datafusion.proto.TableReference proto) {
    if (proto == null) return null;
    return switch (proto.getTableReferenceEnumCase()) {
      case BARE -> new TableReference.Bare(proto.getBare().getTable());
      case PARTIAL ->
          new TableReference.Partial(proto.getPartial().getSchema(), proto.getPartial().getTable());
      case FULL ->
          new TableReference.Full(
              proto.getFull().getCatalog(),
              proto.getFull().getSchema(),
              proto.getFull().getTable());
      default -> null;
    };
  }

  /**
   * Converts a Java {@link TableReference} to a proto {@link
   * org.apache.arrow.datafusion.proto.TableReference}.
   *
   * @param ref_ the Java table reference
   * @return the proto table reference
   */
  static org.apache.arrow.datafusion.proto.TableReference toProtoTableReference(
      TableReference ref_) {
    var builder = org.apache.arrow.datafusion.proto.TableReference.newBuilder();
    switch (ref_) {
      case TableReference.Bare b ->
          builder.setBare(
              org.apache.arrow.datafusion.proto.BareTableReference.newBuilder()
                  .setTable(b.table()));
      case TableReference.Partial p ->
          builder.setPartial(
              org.apache.arrow.datafusion.proto.PartialTableReference.newBuilder()
                  .setSchema(p.schema())
                  .setTable(p.table()));
      case TableReference.Full f ->
          builder.setFull(
              org.apache.arrow.datafusion.proto.FullTableReference.newBuilder()
                  .setCatalog(f.catalog())
                  .setSchema(f.schema())
                  .setTable(f.table()));
    }
    return builder.build();
  }
}
