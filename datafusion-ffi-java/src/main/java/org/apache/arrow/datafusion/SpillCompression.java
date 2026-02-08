package org.apache.arrow.datafusion;

/** Compression codec used for spilling intermediate results to disk. */
public enum SpillCompression {
  ZSTD,
  LZ4_FRAME,
  UNCOMPRESSED;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
