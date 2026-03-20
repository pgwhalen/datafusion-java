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
public record Column(String name, TableReference relation, Spans spans) {}
