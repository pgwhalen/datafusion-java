package org.apache.arrow.datafusion.dataframe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.datafusion.logical_expr.InsertOp;

/**
 * Options controlling how data is written from a DataFrame.
 *
 * <p>Use {@link #builder()} to create instances.
 *
 * <p>Example:
 *
 * {@snippet :
 * DataFrameWriteOptions options = DataFrameWriteOptions.builder()
 *     .singleFileOutput(true)
 *     .insertOp(InsertOp.OVERWRITE)
 *     .partitionBy(List.of("year", "month"))
 *     .build();
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/dataframe/struct.DataFrameWriteOptions.html">Rust
 *     DataFusion: DataFrameWriteOptions</a>
 */
public final class DataFrameWriteOptions {
  private final boolean singleFileOutput;
  private final InsertOp insertOp;
  private final List<String> partitionBy;

  private DataFrameWriteOptions(Builder builder) {
    this.singleFileOutput = builder.singleFileOutput;
    this.insertOp = builder.insertOp;
    this.partitionBy = Collections.unmodifiableList(new ArrayList<>(builder.partitionBy));
  }

  /**
   * Creates a new builder with default values.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  boolean singleFileOutput() {
    return singleFileOutput;
  }

  InsertOp insertOp() {
    return insertOp;
  }

  List<String> partitionBy() {
    return partitionBy;
  }

  /** Builder for {@link DataFrameWriteOptions}. */
  public static final class Builder {
    private boolean singleFileOutput = true;
    private InsertOp insertOp = InsertOp.APPEND;
    private List<String> partitionBy = new ArrayList<>();

    private Builder() {}

    /**
     * Set whether to write a single file. When true, an output file is always created even if the
     * DataFrame is empty. Default: true.
     *
     * @param singleFileOutput true to write a single file
     * @return this builder
     */
    public Builder singleFileOutput(boolean singleFileOutput) {
      this.singleFileOutput = singleFileOutput;
      return this;
    }

    /**
     * Set the insert operation mode. Default: {@link InsertOp#APPEND}.
     *
     * @param insertOp the insert operation mode
     * @return this builder
     */
    public Builder insertOp(InsertOp insertOp) {
      this.insertOp = insertOp;
      return this;
    }

    /**
     * Set columns to partition by. Default: empty (no partitioning).
     *
     * @param partitionBy the column names to partition by
     * @return this builder
     */
    public Builder partitionBy(List<String> partitionBy) {
      this.partitionBy = new ArrayList<>(partitionBy);
      return this;
    }

    /**
     * Builds the {@link DataFrameWriteOptions}.
     *
     * @return the built options
     */
    public DataFrameWriteOptions build() {
      return new DataFrameWriteOptions(this);
    }
  }
}
