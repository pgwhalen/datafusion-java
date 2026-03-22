package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.datafusion.datasource.FileOpener;
import org.apache.arrow.datafusion.datasource.FileScanConfig;
import org.apache.arrow.datafusion.datasource.FileSource;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Adapts a user-implemented {@link FileSource} to the Diplomat-generated {@link DfFileSourceTrait}
 * interface for FFI callbacks.
 */
final class DfFileSourceAdapter implements DfFileSourceTrait {
  private static final long ARROW_SCHEMA_SIZE = 72;

  private final FileSource source;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  DfFileSourceAdapter(FileSource source, BufferAllocator allocator, boolean fullStackTrace) {
    this.source = source;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long createFileOpener(
      long schemaAddr,
      long projectionAddr,
      long projectionLen,
      long limit,
      long batchSize,
      long errorAddr,
      long errorCap) {
    try {
      // Import schema from FFI address
      Schema schema;
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        MemorySegment dest =
            MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment src = MemorySegment.ofAddress(schemaAddr).reinterpret(ARROW_SCHEMA_SIZE);
        dest.copyFrom(src);
        // Clear release callback (offset 56) in Rust source so Rust's Drop is a no-op.
        src.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL);
        schema = Data.importSchema(allocator, ffiSchema, null);
      }

      // Convert projection from raw u32 buffer
      int[] projectionArr = NativeUtil.readU32s(projectionAddr, projectionLen);
      List<Integer> projectionList;
      if (projectionArr.length > 0) {
        projectionList = new ArrayList<>(projectionArr.length);
        for (int idx : projectionArr) {
          projectionList.add(idx);
        }
        projectionList = Collections.unmodifiableList(projectionList);
      } else {
        projectionList = Collections.emptyList();
      }

      Long limitVal = limit >= 0 ? limit : null;
      Long batchSizeVal = batchSize >= 0 ? batchSize : null;
      FileScanConfig config = new FileScanConfig(projectionList, limitVal, batchSizeVal, false, 0);

      FileOpener opener = source.createFileOpener(schema, allocator, config);

      DfFileOpenerAdapter adapter = new DfFileOpenerAdapter(opener, allocator, fullStackTrace);
      return DfFileOpener.createRaw(adapter);
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
