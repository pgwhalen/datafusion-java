package org.apache.arrow.datafusion.datasource;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading newline-delimited JSON files.
 *
 * <p>Mirrors Rust's {@code NdJsonReadOptions}. Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * {@snippet :
 * NdJsonReadOptions options = NdJsonReadOptions.builder()
 *     .schemaInferMaxRecords(200)
 *     .schema(mySchema)
 *     .build();
 * ctx.registerJson("my_table", "/path/to/data.json", options);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html">Rust
 *     DataFusion: NdJsonReadOptions</a>
 */
public final class NdJsonReadOptions {
  private final int schemaInferMaxRecords;
  private final Schema schema;

  private NdJsonReadOptions(Builder builder) {
    this.schemaInferMaxRecords = builder.schemaInferMaxRecords;
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
   * {@snippet :
   * NdJsonReadOptions options = NdJsonReadOptions.builder()
   *     .schema(mySchema)
   *     .build();
   * Schema schema = options.schema();
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/file_format/options/struct.NdJsonReadOptions.html#method.schema">Rust
   *     DataFusion: NdJsonReadOptions::schema</a>
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Encodes the options (excluding schema) as protobuf bytes (JsonOptions proto).
   *
   * <p>Example:
   *
   * {@snippet :
   * NdJsonReadOptions options = NdJsonReadOptions.builder()
   *     .schemaInferMaxRecords(200)
   *     .build();
   * byte[] encoded = options.encodeOptions();
   * }
   */
  public byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.JsonOptions.Builder b =
        org.apache.arrow.datafusion.proto.JsonOptions.newBuilder();
    b.setSchemaInferMaxRec(schemaInferMaxRecords);
    // UNCOMPRESSED = 4
    b.setCompressionValue(4);
    return b.build().toByteArray();
  }

  /** Builder for {@link NdJsonReadOptions}. */
  public static final class Builder {
    private int schemaInferMaxRecords = 100;
    private Schema schema = null;

    private Builder() {}

    public Builder schemaInferMaxRecords(int schemaInferMaxRecords) {
      this.schemaInferMaxRecords = schemaInferMaxRecords;
      return this;
    }

    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public NdJsonReadOptions build() {
      return new NdJsonReadOptions(this);
    }
  }
}
