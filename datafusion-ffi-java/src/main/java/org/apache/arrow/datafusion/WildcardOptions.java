package org.apache.arrow.datafusion;

/**
 * Options for a wildcard expression, corresponding to DataFusion's {@code WildcardOptions}.
 *
 * @param qualifier the optional table qualifier for the wildcard
 */
public record WildcardOptions(String qualifier) {}
