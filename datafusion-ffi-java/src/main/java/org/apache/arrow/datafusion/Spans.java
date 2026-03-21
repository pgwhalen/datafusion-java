package org.apache.arrow.datafusion;

import java.util.List;

/**
 * A collection of source code spans associated with an expression.
 *
 * @param spans the list of spans
 * @see <a
 *     href="https://docs.rs/datafusion-common/52.1.0/datafusion_common/spans/struct.Spans.html">Rust
 *     DataFusion: Spans</a>
 */
public record Spans(List<Span> spans) {
  public Spans {
    spans = List.copyOf(spans);
  }
}
