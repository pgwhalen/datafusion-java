package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import org.apache.arrow.datafusion.DataFusionException;
import org.apache.arrow.datafusion.NativeErrorException;

/**
 * Utility class for handling errors from native DataFusion functions.
 *
 * <p>This class provides high-level methods for calling native functions with automatic error
 * handling. The preferred API uses functional interfaces to encapsulate the native call:
 *
 * <pre>{@code
 * // For void operations (int result code, 0 = success):
 * NativeUtil.call(arena, "Register table", errorOut ->
 *     (int) DataFusionBindings.REGISTER.invokeExact(ctx, name, errorOut));
 *
 * // For pointer-returning operations:
 * MemorySegment df = NativeUtil.callForPointer(arena, "Execute SQL", errorOut ->
 *     (MemorySegment) DataFusionBindings.SQL.invokeExact(ctx, query, errorOut));
 *
 * // For stream operations (returns result: positive=data, 0=end, negative=error):
 * int result = NativeUtil.callForStreamResult(arena, "Stream next", errorOut ->
 *     (int) DataFusionBindings.STREAM_NEXT.invokeExact(stream, errorOut));
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

  // ========================================================================
  // Functional interfaces for native calls
  // ========================================================================

  /**
   * A native function call that returns an int result code.
   *
   * <p>Used with {@link #call} and {@link #callForStreamResult}.
   */
  @FunctionalInterface
  public interface NativeIntCall {
    /**
     * Invokes the native function.
     *
     * @param errorOut the error output segment to pass to the native function
     * @return the int result code
     * @throws Throwable if the invocation fails
     */
    int invoke(MemorySegment errorOut) throws Throwable;
  }

  /**
   * A native function call that returns a pointer.
   *
   * <p>Used with {@link #callForPointer}.
   */
  @FunctionalInterface
  public interface NativePointerCall {
    /**
     * Invokes the native function.
     *
     * @param errorOut the error output segment to pass to the native function
     * @return the pointer result
     * @throws Throwable if the invocation fails
     */
    MemorySegment invoke(MemorySegment errorOut) throws Throwable;
  }

  // ========================================================================
  // High-level call methods with automatic error handling
  // ========================================================================

  /**
   * Calls a native function that returns an int result code (0 = success).
   *
   * <p>This method allocates an error output segment, invokes the native function, and checks the
   * result. If the result is non-zero, it extracts the error message and throws an exception.
   *
   * @param arena the arena to allocate the error output from
   * @param operation a description of the operation for error messages
   * @param call the native function to invoke
   * @throws NativeErrorException if the native function returns a non-zero result
   * @throws DataFusionException if the invocation fails unexpectedly
   */
  public static void call(Arena arena, String operation, NativeIntCall call) {
    MemorySegment errorOut = allocateErrorOut(arena);
    try {
      int result = call.invoke(errorOut);
      checkResult(result, errorOut, operation);
    } catch (NativeErrorException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to " + operation, e);
    }
  }

  /**
   * Calls a native function that returns a pointer (non-null = success).
   *
   * <p>This method allocates an error output segment, invokes the native function, and checks the
   * result. If the pointer is null, it extracts the error message and throws an exception.
   *
   * @param arena the arena to allocate the error output from
   * @param operation a description of the operation for error messages
   * @param call the native function to invoke
   * @return the non-null pointer returned by the native function
   * @throws NativeErrorException if the native function returns a null pointer
   * @throws DataFusionException if the invocation fails unexpectedly
   */
  public static MemorySegment callForPointer(
      Arena arena, String operation, NativePointerCall call) {
    MemorySegment errorOut = allocateErrorOut(arena);
    try {
      MemorySegment pointer = call.invoke(errorOut);
      checkPointer(pointer, errorOut, operation);
      return pointer;
    } catch (NativeErrorException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to " + operation, e);
    }
  }

  /**
   * Calls a native streaming function and returns the result after error checking.
   *
   * <p>This method allocates an error output segment, invokes the native function, and checks for
   * errors. Stream functions return: positive = data available, 0 = end of stream, negative =
   * error. If negative, it extracts the error message and throws an exception.
   *
   * @param arena the arena to allocate the error output from
   * @param operation a description of the operation for error messages
   * @param call the native function to invoke
   * @return the result code (guaranteed to be >= 0)
   * @throws NativeErrorException if the native function returns a negative result
   * @throws DataFusionException if the invocation fails unexpectedly
   */
  public static int callForStreamResult(Arena arena, String operation, NativeIntCall call) {
    MemorySegment errorOut = allocateErrorOut(arena);
    try {
      int result = call.invoke(errorOut);
      checkStreamResult(result, errorOut, operation);
      return result;
    } catch (NativeErrorException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to " + operation, e);
    }
  }

  // ========================================================================
  // Lower-level utilities (for cases where the high-level API doesn't fit)
  // ========================================================================

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
      return errorPtr.reinterpret(len + 1).getString(0);
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
   * <p>Use this for streaming functions that return: positive value = data available, 0 = end of
   * stream, negative value = error.
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
