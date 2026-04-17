package org.apache.arrow.datafusion;

/**
 * The Apache Parquet file format configuration
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.ParquetReadOptions} with the new
 *     SessionContext API instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class ParquetFormat implements FileFormat {
  /** Create new ParquetFormat with default options */
  public ParquetFormat() {}

  @Override
  public long getPointer() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
