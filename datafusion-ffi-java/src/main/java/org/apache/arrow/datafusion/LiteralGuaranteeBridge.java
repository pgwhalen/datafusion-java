package org.apache.arrow.datafusion;

import com.google.protobuf.InvalidProtocolBufferException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Bridge between the public {@link LiteralGuarantee} API and Diplomat-generated {@link
 * DfLiteralGuarantees}.
 */
final class LiteralGuaranteeBridge {

  private LiteralGuaranteeBridge() {}

  /**
   * Analyze a physical expression for literal guarantees using Diplomat bindings.
   *
   * @param bridge the physical expression bridge
   * @return the list of literal guarantees
   */
  static List<LiteralGuarantee> analyze(PhysicalExprBridge bridge) {
    try (DfLiteralGuarantees guarantees = bridge.dfExpr().analyzeGuarantees()) {
      long count = guarantees.count();
      if (count == 0) {
        return List.of();
      }

      List<LiteralGuarantee> result = new ArrayList<>((int) count);

      for (long gIdx = 0; gIdx < count; gIdx++) {
        // Column name
        String columnName = guarantees.columnName(gIdx);

        // Table reference from proto bytes
        TableReference relation = readTableRef(guarantees, gIdx);

        // Spans
        long spansCount = guarantees.spanCount(gIdx);
        List<Span> spans = new ArrayList<>((int) spansCount);
        for (long sIdx = 0; sIdx < spansCount; sIdx++) {
          spans.add(
              new Span(
                  new Location(
                      guarantees.spanStartLine(gIdx, sIdx), guarantees.spanStartCol(gIdx, sIdx)),
                  new Location(
                      guarantees.spanEndLine(gIdx, sIdx), guarantees.spanEndCol(gIdx, sIdx))));
        }

        // Guarantee type
        int guaranteeTypeVal = guarantees.guaranteeType(gIdx);
        Guarantee guaranteeType =
            switch (guaranteeTypeVal) {
              case 0 -> Guarantee.IN;
              case 1 -> Guarantee.NOT_IN;
              default ->
                  throw new DataFusionException("Unknown guarantee type: " + guaranteeTypeVal);
            };

        // Literals
        long literalCount = guarantees.literalCount(gIdx);
        Set<ScalarValue> literals = new LinkedHashSet<>((int) literalCount);
        for (long lIdx = 0; lIdx < literalCount; lIdx++) {
          byte[] protoBytes = readExprBytes(guarantees.literalProtoBytes(gIdx, lIdx));
          if (protoBytes.length == 0) {
            literals.add(new ScalarValue.Null());
          } else {
            try {
              org.apache.arrow.datafusion.proto.ScalarValue proto =
                  org.apache.arrow.datafusion.proto.ScalarValue.parseFrom(protoBytes);
              literals.add(ScalarValueProtoConverter.fromProto(proto));
            } catch (InvalidProtocolBufferException e) {
              throw new DataFusionException("Failed to decode ScalarValue protobuf", e);
            }
          }
        }

        Column column = new Column(columnName, relation, new Spans(spans));
        result.add(new LiteralGuarantee(column, guaranteeType, literals));
      }

      return result;
    } catch (DfError e) {
      throw new NativeDataFusionException(e);
    }
  }

  private static TableReference readTableRef(DfLiteralGuarantees guarantees, long gIdx) {
    byte[] protoBytes = readExprBytes(guarantees.tableRefProtoBytes(gIdx));
    if (protoBytes.length == 0) {
      return null;
    }
    try {
      return TableReferenceConverter.fromProtoBytes(protoBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DataFusionException("Failed to decode TableReference protobuf", e);
    }
  }

  /** Extract bytes from a DfExprBytes opaque using the Diplomat pattern. */
  private static byte[] readExprBytes(DfExprBytes exprBytes) {
    try (exprBytes) {
      long len = exprBytes.len();
      if (len == 0) {
        return new byte[0];
      }
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment buf = arena.allocate(len);
        exprBytes.copyTo(buf.address(), len);
        return buf.toArray(ValueLayout.JAVA_BYTE);
      }
    }
  }
}
