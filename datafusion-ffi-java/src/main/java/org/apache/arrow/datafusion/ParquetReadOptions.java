package org.apache.arrow.datafusion;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Options for reading Parquet files.
 *
 * <p>Mirrors Rust's {@code ParquetReadOptions}. Use {@link #builder()} to create instances.
 */
public final class ParquetReadOptions {
  private final String fileExtension;
  private final Boolean parquetPruning;
  private final Boolean skipMetadata;
  private final Schema schema;

  private ParquetReadOptions(Builder builder) {
    this.fileExtension = builder.fileExtension;
    this.parquetPruning = builder.parquetPruning;
    this.skipMetadata = builder.skipMetadata;
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
    kv.put("file_extension", fileExtension);
    if (parquetPruning != null) {
      kv.put("parquet_pruning", String.valueOf(parquetPruning));
    }
    if (skipMetadata != null) {
      kv.put("skip_metadata", String.valueOf(skipMetadata));
    }
    return CsvReadOptions.encodeKeyValues(kv);
  }

  /** Builder for {@link ParquetReadOptions}. */
  public static final class Builder {
    private String fileExtension = ".parquet";
    private Boolean parquetPruning = null;
    private Boolean skipMetadata = null;
    private Schema schema = null;

    private Builder() {}

    public Builder fileExtension(String fileExtension) {
      this.fileExtension = fileExtension;
      return this;
    }

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
