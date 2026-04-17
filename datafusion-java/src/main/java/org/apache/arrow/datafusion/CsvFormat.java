package org.apache.arrow.datafusion;

/**
 * The CSV file format configuration
 *
 * @deprecated Use {@link org.apache.arrow.datafusion.datasource.CsvReadOptions} with the new
 *     SessionContext API instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class CsvFormat implements FileFormat {
  /** Create new CSV format with default options */
  public CsvFormat() {}

  @Override
  public long getPointer() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
