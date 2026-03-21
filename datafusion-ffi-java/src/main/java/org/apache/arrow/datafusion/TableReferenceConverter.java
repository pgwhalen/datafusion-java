package org.apache.arrow.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.datafusion.common.TableReference;
import org.apache.arrow.datafusion.physical_expr.LiteralGuaranteeBridge;

/**
 * Utilities for converting between Java {@link TableReference} and protobuf representations.
 *
 * <p>This is a pure Java converter with no FFI dependencies. It is used by {@link
 * ExprProtoConverter} for expression serialization and by {@link LiteralGuaranteeBridge} for
 * guarantee deserialization.
 */
public final class TableReferenceConverter {

  private TableReferenceConverter() {}

  /**
   * Deserializes a {@link TableReference} from protobuf bytes.
   *
   * @param bytes the proto bytes
   * @return the table reference, or null if the bytes are empty
   * @throws InvalidProtocolBufferException if proto decoding fails
   */
  static TableReference fromProtoBytes(byte[] bytes) throws InvalidProtocolBufferException {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return convertTableReference(org.apache.arrow.datafusion.proto.TableReference.parseFrom(bytes));
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
