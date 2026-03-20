package org.apache.arrow.datafusion.config;

/**
 * Compression codec used for spilling intermediate results to disk.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/config/enum.SpillCompression.html">Rust
 *     DataFusion: SpillCompression</a>
 */
public enum SpillCompression {
  ZSTD,
  LZ4_FRAME,
  UNCOMPRESSED;

  /** Returns the lowercase string value expected by DataFusion's config system. */
  String toConfigValue() {
    return name().toLowerCase();
  }
}
