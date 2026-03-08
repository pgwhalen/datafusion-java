package org.apache.arrow.datafusion;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading newline-delimited JSON files.
 *
 * <p>Mirrors Rust's {@code NdJsonReadOptions}. Use {@link #builder()} to create instances.
 */
public final class NdJsonReadOptions {
  private final int schemaInferMaxRecords;
  private final String fileExtension;
  private final String fileCompressionType;
  private final boolean infinite;
  private final Schema schema;

  private NdJsonReadOptions(Builder builder) {
    this.schemaInferMaxRecords = builder.schemaInferMaxRecords;
    this.fileExtension = builder.fileExtension;
    this.fileCompressionType = builder.fileCompressionType;
    this.infinite = builder.infinite;
    this.schema = builder.schema;
  }

  /** Creates a new builder with default values. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the schema, or null if not set. */
  public Schema schema() {
    return schema;
  }

  /** Encodes the options (excluding schema) as null-separated key-value bytes. */
  byte[] encodeOptions() {
    Map<String, String> kv = new LinkedHashMap<>();
    kv.put("schema_infer_max_records", String.valueOf(schemaInferMaxRecords));
    kv.put("file_extension", fileExtension);
    kv.put("file_compression_type", fileCompressionType);
    kv.put("infinite", String.valueOf(infinite));
    return CsvReadOptions.encodeKeyValues(kv);
  }

  /** Builder for {@link NdJsonReadOptions}. */
  public static final class Builder {
    private int schemaInferMaxRecords = 100;
    private String fileExtension = ".json";
    private String fileCompressionType = "UNCOMPRESSED";
    private boolean infinite = false;
    private Schema schema = null;

    private Builder() {}

    public Builder schemaInferMaxRecords(int schemaInferMaxRecords) {
      this.schemaInferMaxRecords = schemaInferMaxRecords;
      return this;
    }

    public Builder fileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

    public Builder fileCompressionType(String fileCompressionType) {
      this.fileCompressionType = fileCompressionType;
      return this;
    }

    public Builder infinite(boolean infinite) {
      this.infinite = infinite;
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
