package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.datafusion.generated.DfStringArray;

/**
 * Utility class for adapter helpers used by Diplomat-generated trait adapters (Df*Adapter classes).
 */
final class NativeUtil {

  private NativeUtil() {}

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
   * Build a DfStringArray from a list of strings and return its raw pointer. The caller (Rust)
   * takes ownership and must free via Box::from_raw.
   */
  static long toRawStringArray(List<String> strings) {
    DfStringArray arr = DfStringArray.newEmpty();
    for (String s : strings) {
      arr.push(s);
    }
    return arr.toRawPtr();
  }
}
