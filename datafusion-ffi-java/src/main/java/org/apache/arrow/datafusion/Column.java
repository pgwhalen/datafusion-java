package org.apache.arrow.datafusion;

/**
 * A logical column reference, corresponding to DataFusion's {@code datafusion_common::Column}.
 *
 * @param name the column name
 * @param relation the optional table reference for this column (may be null)
 * @param spans the source code spans associated with this column reference
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/column/struct.Column.html">Rust
 *     DataFusion: Column</a>
 */
public record Column(String name, TableReference relation, Spans spans) {

  /**
   * Returns the column name.
   *
   * @return the column name
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/column/struct.Column.html#method.name">Rust
   *     DataFusion: Column::name</a>
   */
  public String name() {
    return name;
  }

  /**
   * Returns the optional table reference for this column.
   *
   * @return the table reference, or null if unqualified
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/column/struct.Column.html#structfield.relation">Rust
   *     DataFusion: Column::relation</a>
   */
  public TableReference relation() {
    return relation;
  }

  /**
   * Returns the source code spans associated with this column reference.
   *
   * @return the spans
   * @see <a
   *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/column/struct.Column.html#method.spans">Rust
   *     DataFusion: Column::spans</a>
   */
  public Spans spans() {
    return spans;
  }
}
