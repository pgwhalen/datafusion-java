package org.apache.arrow.datafusion.execution;

/**
 * Placeholder handle for a registered scalar UDF entry returned by {@link
 * TaskContext#scalarFunctions()}.
 *
 * <p>Rust DataFusion's {@code TaskContext::scalar_functions()} returns a {@code HashMap<String,
 * Arc<ScalarUDF>>}. The Java bindings currently only surface the set of registered names; the
 * value in the returned map is this empty handle. A future change will replace it with a real
 * wrapper over the Rust-side {@code ScalarUDF}.
 */
public record ScalarFunctionHandle() {}
