package org.apache.arrow.datafusion.logical_expr;

/**
 * A column assignment for an UPDATE statement, pairing a column name with a new value expression.
 *
 * <p>In Rust DataFusion, this is represented as a {@code (String, Expr)} tuple in {@code
 * Vec<(String, Expr)>} within the {@code TableProvider::update} method signature.
 *
 * @param column the name of the column to update
 * @param value the expression providing the new value
 * @see <a
 *     href="https://docs.rs/datafusion-catalog/53.1.0/datafusion_catalog/trait.TableProvider.html#method.update">Rust
 *     DataFusion: TableProvider::update</a>
 */
public record ColumnAssignment(String column, Expr value) {}
