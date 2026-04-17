package org.apache.arrow.datafusion;

/**
 * A runtime represents the underlying async runtime in datafusion engine
 *
 * @deprecated The new FFI library manages the runtime internally.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
public interface Runtime extends AutoCloseable, NativeProxy {}
