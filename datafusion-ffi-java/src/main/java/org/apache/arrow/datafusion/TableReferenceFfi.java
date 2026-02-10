package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * FFI utilities for reading {@link TableReference} values from native memory.
 *
 * <p>This is separated from {@link TableReference} so it can be reused wherever table references
 * appear in FFI structs (e.g., literal guarantees, column references).
 */
final class TableReferenceFfi {

  private TableReferenceFfi() {}

  /**
   * Reads a {@link TableReference} from FFI output parameters.
   *
   * @param relType the relation type: 0=none, 1=bare, 2=partial, 3=full
   * @param relStrsOut a memory segment containing up to 3 null-terminated string pointers
   * @return the table reference, or null if relType is 0
   * @throws DataFusionException if relType is unknown
   */
  static TableReference readTableReference(int relType, MemorySegment relStrsOut) {
    return switch (relType) {
      case 0 -> null;
      case 1 -> {
        MemorySegment tablePtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 0);
        yield new TableReference.Bare(NativeUtil.readNullTerminatedString(tablePtr));
      }
      case 2 -> {
        MemorySegment schemaPtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 0);
        MemorySegment tablePtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 1);
        yield new TableReference.Partial(
            NativeUtil.readNullTerminatedString(schemaPtr),
            NativeUtil.readNullTerminatedString(tablePtr));
      }
      case 3 -> {
        MemorySegment catalogPtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 0);
        MemorySegment schemaPtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 1);
        MemorySegment tablePtr = relStrsOut.getAtIndex(ValueLayout.ADDRESS, 2);
        yield new TableReference.Full(
            NativeUtil.readNullTerminatedString(catalogPtr),
            NativeUtil.readNullTerminatedString(schemaPtr),
            NativeUtil.readNullTerminatedString(tablePtr));
      }
      default -> throw new DataFusionException("Unknown relation type: " + relType);
    };
  }
}
