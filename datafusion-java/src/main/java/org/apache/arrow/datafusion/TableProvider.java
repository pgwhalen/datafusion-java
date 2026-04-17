package org.apache.arrow.datafusion;

/**
 * vague interface that maps to {@code Arc<dyn TableProvider>}.
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.catalog.TableProvider} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
public interface TableProvider extends NativeProxy {}
