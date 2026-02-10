package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * FFI implementation for analyzing physical expressions to extract {@link LiteralGuarantee}s.
 *
 * <p>This class contains the native interop code that was previously in {@link LiteralGuarantee}.
 */
final class LiteralGuaranteeFfi {

  private LiteralGuaranteeFfi() {}

  /**
   * Analyzes a physical expression to extract literal guarantees.
   *
   * <p>This mirrors DataFusion's {@code LiteralGuarantee::analyze()} function.
   *
   * @param expr the physical expression to analyze
   * @return the list of literal guarantees extracted from the expression
   * @throws DataFusionException if analysis fails
   */
  static List<LiteralGuarantee> analyze(PhysicalExpr expr) {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment countOut = arena.allocate(ValueLayout.JAVA_LONG);
      countOut.set(ValueLayout.JAVA_LONG, 0, 0);

      MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);
      MemorySegment handle;
      try {
        handle =
            (MemorySegment)
                DataFusionBindings.LITERAL_GUARANTEE_ANALYZE.invokeExact(
                    expr.ffi().nativeHandle(), countOut, errorOut);
      } catch (Throwable e) {
        throw new DataFusionException("Failed to analyze literal guarantees", e);
      }
      NativeUtil.checkPointer(handle, errorOut, "Analyze literal guarantees");

      try {
        return readGuarantees(arena, handle, countOut.get(ValueLayout.JAVA_LONG, 0));
      } finally {
        try {
          DataFusionBindings.GUARANTEES_DESTROY.invokeExact(handle);
        } catch (Throwable ignored) {
          // Best effort cleanup
        }
      }
    }
  }

  private static List<LiteralGuarantee> readGuarantees(
      Arena arena, MemorySegment handle, long count) {
    if (count == 0) {
      return List.of();
    }

    List<LiteralGuarantee> result = new ArrayList<>((int) count);

    for (int gIdx = 0; gIdx < (int) count; gIdx++) {
      MemorySegment nameOut = arena.allocate(ValueLayout.ADDRESS);
      MemorySegment nameLenOut = arena.allocate(ValueLayout.JAVA_LONG);
      MemorySegment relTypeOut = arena.allocate(ValueLayout.JAVA_INT);
      MemorySegment relStrsOut = arena.allocate(ValueLayout.ADDRESS, 3);
      MemorySegment guaranteeTypeOut = arena.allocate(ValueLayout.JAVA_INT);
      MemorySegment literalCountOut = arena.allocate(ValueLayout.JAVA_LONG);
      MemorySegment spansCountOut = arena.allocate(ValueLayout.JAVA_LONG);

      final int guaranteeIdx = gIdx;
      NativeUtil.call(
          arena,
          "Get guarantee info",
          errorOut ->
              (int)
                  DataFusionBindings.GUARANTEE_GET_INFO.invokeExact(
                      handle,
                      (long) guaranteeIdx,
                      nameOut,
                      nameLenOut,
                      relTypeOut,
                      relStrsOut,
                      guaranteeTypeOut,
                      literalCountOut,
                      spansCountOut,
                      errorOut));

      // Read column name (borrowed from Rust, NOT null-terminated)
      MemorySegment namePtr = nameOut.get(ValueLayout.ADDRESS, 0);
      long nameLen = nameLenOut.get(ValueLayout.JAVA_LONG, 0);
      byte[] nameBytes = new byte[(int) nameLen];
      MemorySegment.copy(
          namePtr.reinterpret(nameLen), ValueLayout.JAVA_BYTE, 0, nameBytes, 0, (int) nameLen);
      String columnName = new String(nameBytes, StandardCharsets.UTF_8);

      // Read table reference
      int relType = relTypeOut.get(ValueLayout.JAVA_INT, 0);
      TableReference relation = TableReferenceFfi.readTableReference(relType, relStrsOut);

      // Read spans
      long spansCount = spansCountOut.get(ValueLayout.JAVA_LONG, 0);
      List<Span> spans = new ArrayList<>((int) spansCount);
      for (int sIdx = 0; sIdx < (int) spansCount; sIdx++) {
        MemorySegment startLineOut = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment startColOut = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment endLineOut = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment endColOut = arena.allocate(ValueLayout.JAVA_LONG);

        final int spanIdx = sIdx;
        NativeUtil.call(
            arena,
            "Get guarantee span",
            errorOut ->
                (int)
                    DataFusionBindings.GUARANTEE_GET_SPAN.invokeExact(
                        handle,
                        (long) guaranteeIdx,
                        (long) spanIdx,
                        startLineOut,
                        startColOut,
                        endLineOut,
                        endColOut,
                        errorOut));

        spans.add(
            new Span(
                new Location(
                    startLineOut.get(ValueLayout.JAVA_LONG, 0),
                    startColOut.get(ValueLayout.JAVA_LONG, 0)),
                new Location(
                    endLineOut.get(ValueLayout.JAVA_LONG, 0),
                    endColOut.get(ValueLayout.JAVA_LONG, 0))));
      }

      // Read guarantee type
      int guaranteeTypeVal = guaranteeTypeOut.get(ValueLayout.JAVA_INT, 0);
      Guarantee guaranteeType =
          switch (guaranteeTypeVal) {
            case 0 -> Guarantee.IN;
            case 1 -> Guarantee.NOT_IN;
            default -> throw new DataFusionException("Unknown guarantee type: " + guaranteeTypeVal);
          };

      // Read literals
      long literalCount = literalCountOut.get(ValueLayout.JAVA_LONG, 0);
      Set<ScalarValue> literals = new LinkedHashSet<>((int) literalCount);
      for (int lIdx = 0; lIdx < (int) literalCount; lIdx++) {
        MemorySegment scalarOut = arena.allocate(ScalarValueFfi.FFI_STRUCT_SIZE);

        final int litIdx = lIdx;
        NativeUtil.call(
            arena,
            "Get guarantee literal",
            errorOut ->
                (int)
                    DataFusionBindings.GUARANTEE_GET_LITERAL.invokeExact(
                        handle, (long) guaranteeIdx, (long) litIdx, scalarOut, errorOut));

        literals.add(ScalarValueFfi.fromFfi(scalarOut));
      }

      Column column = new Column(columnName, relation, new Spans(spans));
      result.add(new LiteralGuarantee(column, guaranteeType, literals));
    }

    return result;
  }
}
