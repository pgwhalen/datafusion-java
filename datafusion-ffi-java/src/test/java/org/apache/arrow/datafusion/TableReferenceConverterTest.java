package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.arrow.datafusion.common.TableReference;
import org.junit.jupiter.api.Test;

/** Tests for {@link TableReferenceConverter} round-trip serialization. */
public class TableReferenceConverterTest {

  @Test
  void testBareRoundTrip() {
    TableReference original = new TableReference.Bare("my_table");
    var proto = TableReferenceConverter.toProtoTableReference(original);
    TableReference result = TableReferenceConverter.convertTableReference(proto);
    assertEquals(original, result);
  }

  @Test
  void testPartialRoundTrip() {
    TableReference original = new TableReference.Partial("my_schema", "my_table");
    var proto = TableReferenceConverter.toProtoTableReference(original);
    TableReference result = TableReferenceConverter.convertTableReference(proto);
    assertEquals(original, result);
  }

  @Test
  void testFullRoundTrip() {
    TableReference original = new TableReference.Full("my_catalog", "my_schema", "my_table");
    var proto = TableReferenceConverter.toProtoTableReference(original);
    TableReference result = TableReferenceConverter.convertTableReference(proto);
    assertEquals(original, result);
  }

  @Test
  void testNullProtoReturnsNull() {
    assertNull(TableReferenceConverter.convertTableReference(null));
  }

  @Test
  void testNullBytesReturnsNull() throws InvalidProtocolBufferException {
    assertNull(TableReferenceConverter.fromProtoBytes(null));
  }

  @Test
  void testEmptyBytesReturnsNull() throws InvalidProtocolBufferException {
    assertNull(TableReferenceConverter.fromProtoBytes(new byte[] {}));
  }

  @Test
  void testFromProtoBytesRoundTrip() throws InvalidProtocolBufferException {
    TableReference original = new TableReference.Full("cat", "sch", "tbl");
    byte[] bytes = TableReferenceConverter.toProtoTableReference(original).toByteArray();
    TableReference result = TableReferenceConverter.fromProtoBytes(bytes);
    assertEquals(original, result);
  }

  @Test
  void testPartialAccessors() {
    TableReference.Partial partial = new TableReference.Partial("my_schema", "my_table");
    assertEquals("my_schema", partial.schema());
    assertEquals("my_table", partial.table());
  }

  @Test
  void testFullAccessors() {
    TableReference.Full full = new TableReference.Full("my_catalog", "my_schema", "my_table");
    assertEquals("my_catalog", full.catalog());
    assertEquals("my_schema", full.schema());
    assertEquals("my_table", full.table());
  }
}
