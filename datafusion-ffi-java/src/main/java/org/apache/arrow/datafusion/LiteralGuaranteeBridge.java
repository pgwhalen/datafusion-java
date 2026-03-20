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

      for (long i = 0; i < count; i++) {
        DfLiteralGuarantee item = guarantees.get(i);

        // Table reference from enum + string parts
        TableReference relation =
            switch (item.tableRefType) {
              case NONE -> null;
              case BARE -> new TableReference.Bare(item.tableRefTable);
              case PARTIAL -> new TableReference.Partial(item.tableRefSchema, item.tableRefTable);
              case FULL ->
                  new TableReference.Full(
                      item.tableRefCatalog, item.tableRefSchema, item.tableRefTable);
            };

        // Spans from DfSpan array
        DfSpan[] dfSpans = guarantees.spans(i);
        List<Span> spans = new ArrayList<>(dfSpans.length);
        for (DfSpan s : dfSpans) {
          spans.add(
              new Span(new Location(s.startLine, s.startCol), new Location(s.endLine, s.endCol)));
        }

        // Guarantee type from enum
        Guarantee guaranteeType =
            switch (item.guaranteeType) {
              case IN -> Guarantee.IN;
              case NOT_IN -> Guarantee.NOT_IN;
            };

        // Literals via per-literal proto bytes
        Set<ScalarValue> literals = new LinkedHashSet<>((int) item.literalCount);
        for (long k = 0; k < item.literalCount; k++) {
          byte[] protoBytes = readExprBytes(guarantees.literalProtoBytes(i, k));
          if (protoBytes.length == 0) {
            literals.add(new ScalarValue.Null());
          } else {
            try {
              org.apache.arrow.datafusion.proto.ScalarValue proto =
                  org.apache.arrow.datafusion.proto.ScalarValue.parseFrom(protoBytes);
              literals.add(ScalarValueProtoConverter.fromProto(proto));
            } catch (InvalidProtocolBufferException e) {
              throw new DataFusionError("Failed to decode ScalarValue protobuf", e);
            }
          }
        }

        Column column = new Column(item.columnName, relation, new Spans(spans));
        result.add(new LiteralGuarantee(column, guaranteeType, literals));
      }

      return result;
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
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
