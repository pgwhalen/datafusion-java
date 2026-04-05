package org.apache.arrow.datafusion.datasource;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading Parquet files.
 *
 * <p>Mirrors Rust's {@code ParquetReadOptions}. Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * <p>{@snippet : ParquetReadOptions options = ParquetReadOptions.builder() .parquetPruning(true)
 * .skipMetadata(false) .build(); ctx.registerParquet("my_table", "/path/to/data.parquet", options);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html">Rust
 *     DataFusion: ParquetReadOptions</a>
 */
public final class ParquetReadOptions {
  private final Boolean parquetPruning;
  private final Boolean skipMetadata;
  private final Schema schema;

  private ParquetReadOptions(Builder builder) {
    this.parquetPruning = builder.parquetPruning;
    this.skipMetadata = builder.skipMetadata;
    this.schema = builder.schema;
  }

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the schema, or null if not set.
   *
   * <p>Example:
   *
   * <p>{@snippet : ParquetReadOptions options = ParquetReadOptions.builder() .schema(mySchema)
   * .parquetPruning(true) .build(); Schema schema = options.schema(); }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/datasource/file_format/options/struct.ParquetReadOptions.html#method.schema">Rust
   *     DataFusion: ParquetReadOptions::schema</a>
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Encodes the options (excluding schema) as protobuf bytes (ParquetOptions proto).
   *
   * <p>Example:
   *
   * <p>{@snippet : ParquetReadOptions options = ParquetReadOptions.builder() .parquetPruning(true)
   * .build(); byte[] encoded = options.encodeOptions(); }
   */
  public byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.ParquetOptions.Builder b =
        org.apache.arrow.datafusion.proto.ParquetOptions.newBuilder();
    if (parquetPruning != null) {
      b.setPruning(parquetPruning);
    }
    if (skipMetadata != null) {
      b.setSkipMetadata(skipMetadata);
    }
    return b.build().toByteArray();
  }

  /** Builder for {@link ParquetReadOptions}. */
  public static final class Builder {
    private Boolean parquetPruning = null;
    private Boolean skipMetadata = null;
    private Schema schema = null;

    private Builder() {}

    public Builder parquetPruning(Boolean parquetPruning) {
      this.parquetPruning = parquetPruning;
      return this;
    }

    public Builder skipMetadata(Boolean skipMetadata) {
      this.skipMetadata = skipMetadata;
      return this;
    }

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public ParquetReadOptions build() {
      return new ParquetReadOptions(this);
    }
  }
}
