package org.apache.arrow.datafusion.datasource;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading Arrow IPC files.
 *
 * <p>Mirrors Rust's {@code ArrowReadOptions}. Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * {@snippet :
 * ArrowReadOptions options = ArrowReadOptions.builder()
 *     .schema(mySchema)
 *     .build();
 * ctx.registerArrow("my_table", "/path/to/data.arrow", options, allocator);
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html">Rust
 *     DataFusion: ArrowReadOptions</a>
 */
public final class ArrowReadOptions {
  private final Schema schema;

  private ArrowReadOptions(Builder builder) {
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
   * ArrowReadOptions options = ArrowReadOptions.builder()
   *     .schema(mySchema)
   *     .build();
   * Schema schema = options.schema();
   * }
   *
   * @see <a
   *     href="https://docs.rs/datafusion/53.1.0/datafusion/datasource/file_format/options/struct.ArrowReadOptions.html#method.schema">Rust
   *     DataFusion: ArrowReadOptions::schema</a>
   */
  public Schema schema() {
    return schema;
  }

  /**
   * Encodes the options (excluding schema) as protobuf bytes (ArrowOptions proto).
   *
   * <p>Example:
   *
   * {@snippet :
   * ArrowReadOptions options = ArrowReadOptions.builder().build();
   * byte[] encoded = options.encodeOptions();
   * }
   */
  public byte[] encodeOptions() {
    return org.apache.arrow.datafusion.proto.ArrowOptions.newBuilder().build().toByteArray();
  }

  /** Builder for {@link ArrowReadOptions}. */
  public static final class Builder {
    private Schema schema = null;

    private Builder() {}

    /**
     * Sets the schema for the Arrow IPC file.
     *
     * @param schema the Arrow schema, or null to infer
     * @return this builder
     */
    public Builder schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Builds the {@link ArrowReadOptions}.
     *
     * @return the built options
     */
    public ArrowReadOptions build() {
      return new ArrowReadOptions(this);
    }
  }
}
