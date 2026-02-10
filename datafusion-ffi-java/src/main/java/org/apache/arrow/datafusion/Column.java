package org.apache.arrow.datafusion;

/**
 * A logical column reference, corresponding to DataFusion's {@code datafusion_common::Column}.
 *
 * @param name the column name
 * @param relation the optional table reference for this column (may be null)
 * @param spans the source code spans associated with this column reference
 */
public record Column(String name, TableReference relation, Spans spans) {}
