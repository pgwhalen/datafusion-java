package org.apache.arrow.datafusion;

/**
 * Options for writing JSON files.
 *
 * <p>Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * {@snippet :
 * JsonOptions options = JsonOptions.builder()
 *     .compressionLevel(3)
 *     .build();
 * byte[] encoded = options.encodeOptions();
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion-common/53.1.0/datafusion_common/config/struct.JsonOptions.html">Rust
 *     DataFusion: JsonOptions</a>
 */
public final class JsonOptions {
  private final Integer compressionLevel;

  private JsonOptions(Builder builder) {
    this.compressionLevel = builder.compressionLevel;
  }

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Encodes the options as protobuf bytes (JsonOptions proto).
   *
   * <p>Example:
   *
   * {@snippet :
   * JsonOptions options = JsonOptions.builder()
   *     .compressionLevel(3)
   *     .build();
   * byte[] encoded = options.encodeOptions();
   * }
   */
  public byte[] encodeOptions() {
    org.apache.arrow.datafusion.proto.JsonOptions.Builder b =
        org.apache.arrow.datafusion.proto.JsonOptions.newBuilder();
    // Compression is UNCOMPRESSED by default (enum value 4)
    b.setCompressionValue(4);
    if (compressionLevel != null) {
      b.setCompressionLevel(compressionLevel);
    }
    return b.build().toByteArray();
  }

  /** Builder for {@link JsonOptions}. */
  public static final class Builder {
    private Integer compressionLevel = null;

    private Builder() {}

    public Builder compressionLevel(Integer compressionLevel) {
      this.compressionLevel = compressionLevel;
      return this;
    }

    public JsonOptions build() {
      return new JsonOptions(this);
    }
  }
}
