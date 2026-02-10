package org.apache.arrow.datafusion;

import java.util.List;

/**
 * A collection of source code spans associated with an expression.
 *
 * @param spans the list of spans
 */
public record Spans(List<Span> spans) {
  public Spans {
    spans = List.copyOf(spans);
  }
}
