package org.apache.arrow.datafusion;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.datafusion.generated.DfStringArray;

/**
 * Utility class for adapter helpers used by Diplomat-generated trait adapters (Df*Adapter classes).
 */
final class NativeUtil {

  // Native method handles for reading from a raw DfStringArray pointer.
  // Used in trait callbacks where Rust passes a DfStringArray to Java.
  private static final MethodHandle STRING_ARRAY_LEN;
  private static final MethodHandle STRING_ARRAY_GET;
  private static final MethodHandle STRING_ARRAY_DESTROY;

  private static final MethodHandle DIPLOMAT_BUFFER_WRITE_CREATE;
  private static final MethodHandle DIPLOMAT_BUFFER_WRITE_GET_BYTES;
  private static final MethodHandle DIPLOMAT_BUFFER_WRITE_LEN;
  private static final MethodHandle DIPLOMAT_BUFFER_WRITE_DESTROY;

  static {
    var lib = NativeLoader.get();
    var linker = Linker.nativeLinker();
    STRING_ARRAY_LEN =
        linker.downcallHandle(
            lib.find("datafusion_DfStringArray_len").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    STRING_ARRAY_GET =
        linker.downcallHandle(
            lib.find("datafusion_DfStringArray_get").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    STRING_ARRAY_DESTROY =
        linker.downcallHandle(
            lib.find("datafusion_DfStringArray_destroy").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_CREATE =
        linker.downcallHandle(
            lib.find("diplomat_buffer_write_create").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    DIPLOMAT_BUFFER_WRITE_GET_BYTES =
        linker.downcallHandle(
            lib.find("diplomat_buffer_write_get_bytes").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_LEN =
        linker.downcallHandle(
            lib.find("diplomat_buffer_write_len").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_DESTROY =
        linker.downcallHandle(
            lib.find("diplomat_buffer_write_destroy").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
  }

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

  /**
   * Read strings from a raw DfStringArray pointer and destroy it (takes ownership). Uses native
   * method handles directly since the generated DfStringArray constructor is package-private.
   */
  static List<String> fromRawStringArray(long ptr) {
    if (ptr == 0) return List.of();
    MemorySegment handle = MemorySegment.ofAddress(ptr);
    try {
      long len = (long) STRING_ARRAY_LEN.invokeExact(handle);
      List<String> result = new ArrayList<>((int) len);
      for (long i = 0; i < len; i++) {
        result.add(readStringArrayElement(handle, i));
      }
      return result;
    } catch (Throwable ex) {
      throw new RuntimeException("Failed to read DfStringArray", ex);
    } finally {
      try {
        STRING_ARRAY_DESTROY.invokeExact(handle);
      } catch (Throwable ignored) {
      }
    }
  }

  private static String readStringArrayElement(MemorySegment handle, long idx) throws Throwable {
    MemorySegment write = (MemorySegment) DIPLOMAT_BUFFER_WRITE_CREATE.invokeExact(0L);
    try {
      STRING_ARRAY_GET.invokeExact(handle, idx, write);
      MemorySegment bytes = (MemorySegment) DIPLOMAT_BUFFER_WRITE_GET_BYTES.invokeExact(write);
      long len = (long) DIPLOMAT_BUFFER_WRITE_LEN.invokeExact(write);
      if (len == 0) return "";
      byte[] data = bytes.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
      return new String(data, StandardCharsets.UTF_8);
    } finally {
      DIPLOMAT_BUFFER_WRITE_DESTROY.invokeExact(write);
    }
  }
}
