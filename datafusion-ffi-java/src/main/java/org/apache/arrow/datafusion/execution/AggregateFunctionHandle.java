package org.apache.arrow.datafusion.execution;

/**
 * Placeholder handle for a registered aggregate UDF entry returned by {@link
 * TaskContext#aggregateFunctions()}.
 *
 * <p>Rust DataFusion's {@code TaskContext::aggregate_functions()} returns a {@code
 * HashMap<String, Arc<AggregateUDF>>}. The Java bindings currently only surface the set of
 * registered names; the value in the returned map is this empty handle. A future change will
 * replace it with a real wrapper over the Rust-side {@code AggregateUDF}.
 */
public record AggregateFunctionHandle() {}
