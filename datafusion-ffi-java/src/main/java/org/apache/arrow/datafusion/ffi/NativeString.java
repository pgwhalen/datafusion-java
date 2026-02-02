package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.datafusion.DataFusionException;

/**
 * Wraps a MemorySegment containing a null-terminated C string.
 *
 * <p>This record provides a type-safe wrapper for FFI string parameters, making the intent clear
 * and centralizing the conversion logic. The string length is queried from native code to support
 * strings of any length.
 *
 * @param segment the memory segment containing the C string
 */
record NativeString(MemorySegment segment) {

  /**
   * Extracts the string value from the segment.
   *
   * <p>This method queries the actual string length from native code, so it supports strings of any
   * length without a hardcoded limit.
   *
   * @return the Java string value, or empty string if the segment is null or empty
   */
  public String value() {
    if (segment.equals(MemorySegment.NULL)) {
      return "";
    }

    try {
      long len = (long) NativeUtil.STRING_LEN.invokeExact(segment);
      if (len == 0) {
        return "";
      }
      // Reinterpret with exact size needed (length + 1 for null terminator)
      return segment.reinterpret(len + 1).getString(0);
    } catch (Throwable e) {
      throw new DataFusionException("Failed to read native string", e);
    }
  }
}
