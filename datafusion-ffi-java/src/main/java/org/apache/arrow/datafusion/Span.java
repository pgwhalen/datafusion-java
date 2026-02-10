package org.apache.arrow.datafusion;

/**
 * A span in source code, defined by a start and end location.
 *
 * @param start the start location
 * @param end the end location
 */
public record Span(Location start, Location end) {}
