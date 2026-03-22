package org.apache.arrow.datafusion;

import org.apache.arrow.datafusion.datasource.FileOpener;
import org.apache.arrow.datafusion.datasource.FileRange;
import org.apache.arrow.datafusion.datasource.ObjectMeta;
import org.apache.arrow.datafusion.datasource.PartitionedFile;
import org.apache.arrow.datafusion.physical_plan.RecordBatchReader;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link FileOpener} to the Diplomat-generated {@link DfFileOpenerTrait}
 * interface for FFI callbacks.
 */
final class DfFileOpenerAdapter implements DfFileOpenerTrait {
  private final FileOpener opener;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  DfFileOpenerAdapter(FileOpener opener, BufferAllocator allocator, boolean fullStackTrace) {
    this.opener = opener;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long open(
      long pathAddr,
      long pathLen,
      long fileSize,
      long rangeStart,
      long rangeEnd,
      long errorAddr,
      long errorCap) {
    try {
      String pathStr = NativeUtil.readString(pathAddr, pathLen);
      ObjectMeta meta = new ObjectMeta(pathStr, fileSize);
      // (0, 0) means no range (zero-length range is nonsensical)
      FileRange range =
          (rangeStart == 0 && rangeEnd == 0) ? null : new FileRange(rangeStart, rangeEnd);
      PartitionedFile file = new PartitionedFile(meta, range);

      RecordBatchReader reader = opener.open(file);

      DfRecordBatchReaderAdapter adapter =
          new DfRecordBatchReaderAdapter(reader, allocator, fullStackTrace);
      long ptr = DfRecordBatchReader.createRaw(adapter);
      adapter.closeFfiSchema();
      return ptr;
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
