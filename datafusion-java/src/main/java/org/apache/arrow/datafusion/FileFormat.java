package org.apache.arrow.datafusion;

/**
 * Interface for file formats that can provide table data
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.FileFormat} instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
public interface FileFormat extends AutoCloseable, NativeProxy {}
