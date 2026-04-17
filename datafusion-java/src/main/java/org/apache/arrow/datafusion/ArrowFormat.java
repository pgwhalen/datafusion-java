package org.apache.arrow.datafusion;

/**
 * The Apache Arrow IPC file format configuration. This format is also known as Feather V2
 *
 * @deprecated Use SQL CREATE EXTERNAL TABLE ... STORED AS ARROW with the new SessionContext API
 *     instead.
 */
@Deprecated(since = "0.17.4", forRemoval = true)
@SuppressWarnings("deprecation")
public class ArrowFormat implements FileFormat {
  /** Create a new ArrowFormat with default options */
  public ArrowFormat() {}

  @Override
  public long getPointer() {
    return 0;
  }

  @Override
  public void close() {
    // no-op
  }
}
