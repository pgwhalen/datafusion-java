package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * Utility class for handling errors from native DataFusion functions.
 *
 * <p>This class provides methods for allocating error output pointers, extracting error messages,
 * and checking results from native function calls. It handles the memory management required for
 * error strings allocated by the native code.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * try (Arena arena = Arena.ofConfined()) {
 *   MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);
 *   int result = (int) SomeBinding.NATIVE_FUNCTION.invokeExact(arg1, arg2, errorOut);
 *   NativeUtil.checkResult(result, errorOut, "operation name");
 * }
 * }</pre>
 */
public final class NativeUtil {
  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup LOOKUP = NativeLoader.get();

  static final MethodHandle STRING_LEN =
      downcall(
          "datafusion_string_len",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

  static final MethodHandle FREE_STRING =
      downcall("datafusion_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private NativeUtil() {}

  /**
   * Allocates and initializes an error output pointer.
   *
   * <p>The returned segment should be passed to native functions that accept an error_out
   * parameter. After the native call returns, use {@link #checkResult}, {@link #checkPointer}, or
   * {@link #checkStreamResult} to check for errors.
   *
   * @param arena The arena to allocate from
   * @return A memory segment initialized to NULL, suitable for error output
   */
  public static MemorySegment allocateErrorOut(Arena arena) {
    MemorySegment errorOut = arena.allocate(ValueLayout.ADDRESS);
    errorOut.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
    return errorOut;
  }

  /**
   * Extracts and frees an error message from a native error pointer.
   *
   * <p>This method reads the error string from the native memory, frees the native memory, and
   * returns the Java string. It handles error messages of any length by first querying the string
   * length from the native code.
   *
   * @param errorOut The error output pointer from a native function call
   * @return The error message, or null if no error was set
   */
  public static String extractAndFreeError(MemorySegment errorOut) {
    MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
    if (errorPtr.equals(MemorySegment.NULL)) {
      return null;
    }

    try {
      // Get the actual length of the error string
      long len = (long) STRING_LEN.invokeExact(errorPtr);
      if (len == 0) {
        return "";
      }

      // Reinterpret with the exact size needed (length + 1 for null terminator)
      String message = errorPtr.reinterpret(len + 1).getUtf8String(0);

      return message;
    } catch (Throwable e) {
      // If we fail to read the message, return a fallback
      return "(failed to read error message: " + e.getMessage() + ")";
    } finally {
      // Always free the error string
      try {
        FREE_STRING.invokeExact(errorPtr);
      } catch (Throwable ignored) {
        // Nothing we can do if free fails
      }
    }
  }

  /**
   * Checks a result code and throws an exception if it indicates an error.
   *
   * <p>Use this for functions that return 0 on success and non-zero on error.
   *
   * @param result The result code from the native function
   * @param errorOut The error output pointer from the native function call
   * @param operation A description of the operation for error messages
   * @throws NativeErrorException if result is non-zero
   */
  public static void checkResult(int result, MemorySegment errorOut, String operation) {
    if (result != 0) {
      String message = extractAndFreeError(errorOut);
      throw new NativeErrorException(operation, message);
    }
  }

  /**
   * Checks a pointer result and throws an exception if it is NULL.
   *
   * <p>Use this for functions that return a pointer on success and NULL on error.
   *
   * @param pointer The pointer returned from the native function
   * @param errorOut The error output pointer from the native function call
   * @param operation A description of the operation for error messages
   * @throws NativeErrorException if pointer is NULL
   */
  public static void checkPointer(MemorySegment pointer, MemorySegment errorOut, String operation) {
    if (pointer.equals(MemorySegment.NULL)) {
      String message = extractAndFreeError(errorOut);
      throw new NativeErrorException(operation, message);
    }
  }

  /**
   * Checks a stream result code and throws an exception if it indicates an error.
   *
   * <p>Use this for streaming functions that return: - positive value: data available - 0: end of
   * stream - negative value: error
   *
   * @param result The result code from the native function
   * @param errorOut The error output pointer from the native function call
   * @param operation A description of the operation for error messages
   * @throws NativeErrorException if result is negative
   */
  public static void checkStreamResult(int result, MemorySegment errorOut, String operation) {
    if (result < 0) {
      String message = extractAndFreeError(errorOut);
      throw new NativeErrorException(operation, message);
    }
  }

  private static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
    MemorySegment symbol =
        LOOKUP.find(name).orElseThrow(() -> new RuntimeException("Symbol not found: " + name));
    return LINKER.downcallHandle(symbol, descriptor);
  }
}
