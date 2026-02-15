package org.apache.arrow.datafusion;

/**
 * Indicates whether a filter expression can be handled by a {@link TableProvider}.
 *
 * <p>This enum is returned from {@link TableProvider#supportsFiltersPushdown} to tell DataFusion
 * how a provider handles each filter:
 *
 * <ul>
 *   <li>{@link #UNSUPPORTED} — The provider cannot apply this filter. DataFusion will apply it
 *       after scan.
 *   <li>{@link #INEXACT} — The provider may partially apply this filter. DataFusion will also apply
 *       it after scan to ensure correctness.
 *   <li>{@link #EXACT} — The provider fully applies this filter. DataFusion will not re-apply it.
 * </ul>
 */
public enum FilterPushDown {
  /** The provider cannot apply this filter at all. */
  UNSUPPORTED,
  /** The provider may partially apply this filter; DataFusion will re-check. */
  INEXACT,
  /** The provider fully applies this filter; DataFusion will not re-check. */
  EXACT;
}
