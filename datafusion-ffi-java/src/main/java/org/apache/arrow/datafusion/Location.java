package org.apache.arrow.datafusion;

/**
 * A source code location within an expression, consisting of a line and column.
 *
 * @param line the line number
 * @param column the column number
 */
public record Location(long line, long column) {}
