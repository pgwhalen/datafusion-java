package org.apache.arrow.datafusion.logical_expr;

/**
 * Options for a wildcard expression, corresponding to DataFusion's {@code WildcardOptions}.
 *
 * @param qualifier the optional table qualifier for the wildcard
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/logical_expr/expr/struct.WildcardOptions.html">Rust
 *     DataFusion: WildcardOptions</a>
 */
public record WildcardOptions(String qualifier) {}
