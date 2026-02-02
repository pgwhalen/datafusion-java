package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.MemorySegment;

/**
 * Wraps a MemorySegment containing a null-terminated C string.
 *
 * <p>This record provides a type-safe wrapper for FFI string parameters, making the intent clear
 * and centralizing the conversion logic.
 *
 * @param segment the memory segment containing the C string
 */
record NativeString(MemorySegment segment) {
  private static final long DEFAULT_MAX_LENGTH = 1024;

  /**
   * Extracts the string value from the segment using the default max length.
   *
   * @return the Java string value
   */
  public String value() {
    return segment.reinterpret(DEFAULT_MAX_LENGTH).getUtf8String(0);
  }

  /**
   * Extracts the string value with a custom max length.
   *
   * @param maxLength the maximum length to read
   * @return the Java string value
   */
  public String value(long maxLength) {
    return segment.reinterpret(maxLength).getUtf8String(0);
  }
}
