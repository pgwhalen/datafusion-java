package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Utility class for adapter helpers and Diplomat slice readers.
 *
 * <p>Used by Diplomat-generated trait adapters (Df*Adapter classes) and by Diplomat-generated code
 * for reading slice parameters.
 */
final class NativeUtil {

  private NativeUtil() {}

  static long writeStrings(long bufAddr, long bufCap, List<String> names) {
    byte[] bytes = String.join("\0", names).getBytes(StandardCharsets.UTF_8);
    int len = (int) Math.min(bytes.length, bufCap);
    MemorySegment.ofAddress(bufAddr).reinterpret(bufCap).copyFrom(MemorySegment.ofArray(bytes));
    return len;
  }

  static String readString(long addr, long len) {
    byte[] bytes = MemorySegment.ofAddress(addr).reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /** Read len u32 values from the given raw address into an int[]. */
  static int[] readU32s(long addr, long len) {
    if (len == 0) return new int[0];
    return MemorySegment.ofAddress(addr).reinterpret(len * 4L).toArray(ValueLayout.JAVA_INT);
  }

  /** Read len bytes from the given raw address into a byte[]. */
  static byte[] readBytes(long addr, long len) {
    if (len == 0) return new byte[0];
    return MemorySegment.ofAddress(addr).reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
  }

  /**
   * Convert a Diplomat slice parameter (Object = MemorySegment pointing to {data: ADDRESS, len:
   * long}) to a Java String.
   */
  static String readDiplomatStr(Object obj) {
    MemorySegment view = ((MemorySegment) obj).reinterpret(16);
    MemorySegment dataPtr = view.get(ValueLayout.ADDRESS, 0);
    long len = view.get(ValueLayout.JAVA_LONG, 8);
    if (len == 0) return "";
    byte[] bytes = dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Convert a Diplomat slice parameter (Object = MemorySegment pointing to {data: ADDRESS, len:
   * long}) to a byte[].
   */
  static byte[] readDiplomatBytes(Object obj) {
    MemorySegment view = ((MemorySegment) obj).reinterpret(16);
    MemorySegment dataPtr = view.get(ValueLayout.ADDRESS, 0);
    long len = view.get(ValueLayout.JAVA_LONG, 8);
    if (len == 0) return new byte[0];
    return dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
  }

  /**
   * Convert a Diplomat slice parameter (Object = MemorySegment pointing to {data: ADDRESS, len:
   * long}) to an int[] (treating each element as a 32-bit unsigned integer).
   */
  static int[] readDiplomatInts(Object obj) {
    MemorySegment view = ((MemorySegment) obj).reinterpret(16);
    MemorySegment dataPtr = view.get(ValueLayout.ADDRESS, 0);
    long len = view.get(ValueLayout.JAVA_LONG, 8);
    if (len == 0) return new int[0];
    return dataPtr.reinterpret(len * 4).toArray(ValueLayout.JAVA_INT);
  }
}
