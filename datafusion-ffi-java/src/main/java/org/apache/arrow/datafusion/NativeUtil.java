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
 * Adapter-side FFM helpers used by {@code Df*Adapter} classes that implement Diplomat trait
 * upcalls. Scoped to the <em>raw {@code long} address</em> side of the FFI boundary: each method
 * works with addresses or raw pointers handed in by Rust during a trait callback, plus the
 * downcall-handle plumbing needed to talk back to Rust.
 *
 * <p>This is distinct from:
 *
 * <ul>
 *   <li>{@code BridgeUtil} ({@code common} package, public) — bridge-side helpers that work with
 *       Diplomat-generated <em>opaque types</em> ({@link DfStringArray}, {@code DfExprBytes},
 *       {@code DfError}) returned by downcalls.
 *   <li>{@link ArrowFfiUtil} — Arrow C Data Interface struct transfer (constants, copy +
 *       clear-release for {@code FFI_ArrowSchema} / {@code FFI_ArrowArray}).
 * </ul>
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
    STRING_ARRAY_LEN =
        createDowncallHandle(
            "datafusion_DfStringArray_len",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    STRING_ARRAY_GET =
        createDowncallHandle(
            "datafusion_DfStringArray_get",
            FunctionDescriptor.ofVoid(
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    STRING_ARRAY_DESTROY =
        createDowncallHandle(
            "datafusion_DfStringArray_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_CREATE =
        createDowncallHandle(
            "diplomat_buffer_write_create",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    DIPLOMAT_BUFFER_WRITE_GET_BYTES =
        createDowncallHandle(
            "diplomat_buffer_write_get_bytes",
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_LEN =
        createDowncallHandle(
            "diplomat_buffer_write_len",
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
    DIPLOMAT_BUFFER_WRITE_DESTROY =
        createDowncallHandle(
            "diplomat_buffer_write_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
  }

  /**
   * Resolve a native symbol and bind a downcall handle to it. Throws {@link
   * java.util.NoSuchElementException} (via {@code orElseThrow}) if the symbol is missing.
   */
  static MethodHandle createDowncallHandle(String name, FunctionDescriptor descriptor) {
    return Linker.nativeLinker()
        .downcallHandle(NativeLoader.get().find(name).orElseThrow(), descriptor);
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
