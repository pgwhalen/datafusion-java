package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for handling errors from native DataFusion functions.
 *
 * <p>This class provides high-level methods for calling native functions with automatic error
 * handling. The preferred API uses functional interfaces to encapsulate the native call:
 *
 * <pre>{@code
 * // For void operations (int result code, 0 = success):
 * NativeUtil.call(arena, "Register table", errorOut ->
 *     (int) REGISTER.invokeExact(ctx, name, errorOut));
 *
 * // For pointer-returning operations:
 * MemorySegment df = NativeUtil.callForPointer(arena, "Execute SQL", errorOut ->
 *     (MemorySegment) SQL.invokeExact(ctx, query, errorOut));
 *
 * // For stream operations (returns result: positive=data, 0=end, negative=error):
 * int result = NativeUtil.callForStreamResult(arena, "Stream next", errorOut ->
 *     (int) STREAM_NEXT.invokeExact(stream, errorOut));
 * }</pre>
 */
final class NativeUtil {
  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup LOOKUP = NativeLoader.get();

  static final MethodHandle STRING_LEN =
      downcall(
          "datafusion_string_len",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

  static final MethodHandle FREE_STRING =
      downcall("datafusion_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  static final MethodHandle FREE_BYTES =
      downcall(
          "datafusion_free_bytes",
          FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

  private static final MethodHandle RSTRING_SIZE =
      downcall("datafusion_rstring_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle CREATE_RSTRING =
      downcall(
          "datafusion_create_rstring",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS.withName("ptr"), // UTF-8 bytes
              ValueLayout.JAVA_LONG.withName("len"),
              ValueLayout.ADDRESS.withName("out") // RString buffer
              ));

  // Cached RString size (queried from Rust once)
  private static volatile long rstringSizeBytes = -1;

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
  interface NativeIntCall {
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
  interface NativePointerCall {
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
  static void call(Arena arena, String operation, NativeIntCall call) {
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
  static MemorySegment callForPointer(Arena arena, String operation, NativePointerCall call) {
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
  static int callForStreamResult(Arena arena, String operation, NativeIntCall call) {
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
  // FFI struct size validation and RString helpers
  // ========================================================================

  /**
   * Get the size in bytes of an abi_stable {@code RString}. The value is cached after the first
   * call.
   */
  static long getRStringSize() {
    long size = rstringSizeBytes;
    if (size < 0) {
      try {
        size = (long) RSTRING_SIZE.invokeExact();
        rstringSizeBytes = size;
      } catch (Throwable e) {
        throw new DataFusionException("Failed to query RString size", e);
      }
    }
    return size;
  }

  /**
   * Write a UTF-8 string as an abi_stable {@code RString} into a buffer at the given offset. Uses
   * the Rust {@code datafusion_create_rstring} helper.
   *
   * @param message the string to write
   * @param buffer the destination buffer
   * @param offset byte offset within the buffer where the RString should be written
   * @param arena arena used for temporary allocation of the UTF-8 bytes
   */
  static void writeRString(String message, MemorySegment buffer, long offset, Arena arena) {
    byte[] msgBytes = message.getBytes(StandardCharsets.UTF_8);
    MemorySegment msgSegment = arena.allocateFrom(ValueLayout.JAVA_BYTE, msgBytes);
    try {
      CREATE_RSTRING.invokeExact(
          msgSegment, (long) msgBytes.length, buffer.asSlice(offset, getRStringSize()));
    } catch (Throwable ignored) {
      // Best effort — if we can't write the RString, the zero-initialized buffer
      // will produce a default/empty error on the Rust side.
    }
  }

  /**
   * Validate that a Java-side size constant matches the Rust-reported size for an FFI type.
   *
   * @param javaSize the size constant defined in Java
   * @param sizeHandle a MethodHandle that calls a Rust size helper (returns long)
   * @param typeName the type name for error messages
   * @throws DataFusionException if the sizes don't match
   */
  static void validateSize(long javaSize, MethodHandle sizeHandle, String typeName) {
    try {
      long rustSize = (long) sizeHandle.invokeExact();
      if (rustSize != javaSize) {
        throw new DataFusionException(
            typeName + " size mismatch: Java=" + javaSize + " Rust=" + rustSize);
      }
    } catch (DataFusionException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to validate " + typeName + " size", e);
    }
  }

  // ========================================================================
  // Shared FFI symbol lookups (used by multiple Handle classes)
  // ========================================================================

  /** Version function pointer — stored in the {@code version} field of FFI provider structs. */
  static final MemorySegment VERSION_FN =
      NativeLoader.get().find("datafusion_ffi_version").orElseThrow();

  /**
   * Library marker function pointer — stored in the {@code library_marker_id} field of FFI provider
   * structs to ensure the "foreign library" conversion path is taken.
   */
  static final MemorySegment JAVA_MARKER_ID_FN =
      NativeLoader.get().find("datafusion_java_marker_id").orElseThrow();

  // ========================================================================
  // Shared FFI_LogicalExtensionCodec helpers (used by all provider Handle classes)
  // ========================================================================

  private static final MethodHandle LOGICAL_CODEC_SIZE_MH =
      downcall("datafusion_ffi_logical_codec_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static final MethodHandle CREATE_NOOP_LOGICAL_CODEC =
      downcall(
          "datafusion_create_noop_logical_codec", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static volatile long logicalCodecSizeBytes = -1;

  /** Get the size of FFI_LogicalExtensionCodec (cached). */
  static long getLogicalCodecSize() {
    long size = logicalCodecSizeBytes;
    if (size < 0) {
      try {
        size = (long) LOGICAL_CODEC_SIZE_MH.invokeExact();
        logicalCodecSizeBytes = size;
      } catch (Throwable e) {
        throw new DataFusionException("Failed to query FFI_LogicalExtensionCodec size", e);
      }
    }
    return size;
  }

  // ========================================================================
  // Shared RVec<RString> / ROption<RString> helpers (used by catalog/schema Handle classes)
  // ========================================================================

  private static final MethodHandle RVEC_RSTRING_SIZE_MH =
      downcall("datafusion_rvec_rstring_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static final MethodHandle CREATE_RVEC_RSTRING =
      downcall(
          "datafusion_create_rvec_rstring",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS.withName("ptrs"),
              ValueLayout.ADDRESS.withName("lens"),
              ValueLayout.JAVA_LONG.withName("count"),
              ValueLayout.ADDRESS.withName("out")));

  private static final MethodHandle ROPTION_RSTRING_SIZE_MH =
      downcall("datafusion_roption_rstring_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  static final MethodHandle CREATE_ROPTION_RSTRING_NONE =
      downcall(
          "datafusion_create_roption_rstring_none", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static volatile long rvecRstringSizeBytes = -1;
  private static volatile long roptionRstringSizeBytes = -1;

  /** Get the size of RVec&lt;RString&gt; (cached). */
  static long getRvecRstringSize() {
    long size = rvecRstringSizeBytes;
    if (size < 0) {
      try {
        size = (long) RVEC_RSTRING_SIZE_MH.invokeExact();
        rvecRstringSizeBytes = size;
      } catch (Throwable e) {
        throw new DataFusionException("Failed to query RVec<RString> size", e);
      }
    }
    return size;
  }

  /** Get the size of ROption&lt;RString&gt; (cached). */
  static long getRoptionRstringSize() {
    long size = roptionRstringSizeBytes;
    if (size < 0) {
      try {
        size = (long) ROPTION_RSTRING_SIZE_MH.invokeExact();
        roptionRstringSizeBytes = size;
      } catch (Throwable e) {
        throw new DataFusionException("Failed to query ROption<RString> size", e);
      }
    }
    return size;
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
  static MemorySegment allocateErrorOut(Arena arena) {
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
  static String extractAndFreeError(MemorySegment errorOut) {
    MemorySegment errorPtr = errorOut.get(ValueLayout.ADDRESS, 0);
    if (errorPtr.equals(MemorySegment.NULL)) {
      return null;
    }

    try {
      return readNullTerminatedString(errorPtr);
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
  static void checkResult(int result, MemorySegment errorOut, String operation) {
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
  static void checkPointer(MemorySegment pointer, MemorySegment errorOut, String operation) {
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
  static void checkStreamResult(int result, MemorySegment errorOut, String operation) {
    if (result < 0) {
      String message = extractAndFreeError(errorOut);
      throw new NativeErrorException(operation, message);
    }
  }

  /**
   * Reads a null-terminated C string from a native memory pointer.
   *
   * <p>Uses {@code datafusion_string_len} to determine the exact length of the string, avoiding
   * hardcoded buffer sizes. Returns an empty string if the pointer is NULL.
   *
   * @param ptr the pointer to the null-terminated C string
   * @return the Java string, or empty string if ptr is NULL
   */
  static String readNullTerminatedString(MemorySegment ptr) {
    if (ptr.equals(MemorySegment.NULL)) {
      return "";
    }

    try {
      long len = (long) STRING_LEN.invokeExact(ptr);
      if (len == 0) {
        return "";
      }
      return ptr.reinterpret(len + 1).getString(0);
    } catch (Throwable e) {
      return "(failed to read string: " + e.getMessage() + ")";
    }
  }

  /** Get the native linker for creating upcall stubs. */
  static Linker getLinker() {
    return LINKER;
  }

  static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
    MemorySegment symbol =
        LOOKUP.find(name).orElseThrow(() -> new RuntimeException("Symbol not found: " + name));
    return LINKER.downcallHandle(symbol, descriptor);
  }
}
