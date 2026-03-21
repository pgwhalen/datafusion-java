package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.datafusion.datasource.FileFormat;
import org.apache.arrow.datafusion.datasource.FileSource;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Adapts a user-implemented {@link FileFormat} to the Diplomat-generated {@link DfFileFormatTrait}
 * interface for FFI callbacks.
 */
public final class DfFileFormatAdapter implements DfFileFormatTrait {
  private final FileFormat format;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;

  public DfFileFormatAdapter(FileFormat format, BufferAllocator allocator, boolean fullStackTrace) {
    this.format = format;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;
  }

  @Override
  public long extensionTo(long bufAddr, long bufCap) {
    try {
      byte[] bytes = format.getExtension().getBytes(StandardCharsets.UTF_8);
      int len = (int) Math.min(bytes.length, bufCap);
      MemorySegment.ofAddress(bufAddr)
          .reinterpret(bufCap)
          .copyFrom(MemorySegment.ofArray(bytes).asSlice(0, len));
      return len;
    } catch (Exception e) {
      return -1;
    }
  }

  @Override
  public long fileSource(long schemaAddr, long errorAddr, long errorCap) {
    try {
      FileSource source = format.fileSource();

      DfFileSourceAdapter adapter = new DfFileSourceAdapter(source, allocator, fullStackTrace);
      return DfFileSource.createRaw(adapter);
    } catch (Exception e) {
      Errors.writeException(errorAddr, errorCap, e, fullStackTrace);
      return 0;
    }
  }
}
