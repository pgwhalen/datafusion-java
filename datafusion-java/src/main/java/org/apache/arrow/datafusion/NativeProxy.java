package org.apache.arrow.datafusion;

/**
 * A native proxy is a proxy that points to a Rust managed object so that when it requires releasing
 * resources the point will be used.
 *
 * @deprecated The new FFI library does not use native proxy pointers.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
interface NativeProxy {

  /**
   * Get a pointer to the native object
   *
   * @return Pointer value as a long
   */
  long getPointer();
}
