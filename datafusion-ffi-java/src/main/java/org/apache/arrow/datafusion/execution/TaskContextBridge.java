package org.apache.arrow.datafusion.execution;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.arrow.datafusion.common.DataFusionError;
import org.apache.arrow.datafusion.generated.DfMemoryPool;
import org.apache.arrow.datafusion.generated.DfRuntimeEnv;
import org.apache.arrow.datafusion.generated.DfSessionConfig;
import org.apache.arrow.datafusion.generated.DfStringArray;
import org.apache.arrow.datafusion.generated.DfTaskContext;

/** Bridge between the public {@link TaskContext} API and Diplomat-generated DfTaskContext. */
final class TaskContextBridge implements AutoCloseable {
  private final DfTaskContext df;
  private volatile boolean closed = false;

  TaskContextBridge(DfTaskContext df) {
    this.df = df;
  }

  /** Adopt ownership of a raw DfTaskContext pointer produced by the Rust execute() upcall. */
  static TaskContextBridge adopt(long addr) {
    return new TaskContextBridge(DfTaskContext.takeFromRaw(addr));
  }

  String sessionId() {
    ensureOpen();
    return df.sessionId();
  }

  Optional<String> taskId() {
    ensureOpen();
    if (!df.hasTaskId()) {
      return Optional.empty();
    }
    return Optional.of(df.taskId());
  }

  SessionConfigBridge sessionConfig() {
    ensureOpen();
    DfSessionConfig cfg = df.sessionConfig();
    return new SessionConfigBridge(cfg);
  }

  RuntimeEnvBridge runtimeEnv() {
    ensureOpen();
    DfRuntimeEnv rt = df.runtimeEnv();
    return new RuntimeEnvBridge(rt);
  }

  MemoryPoolBridge memoryPool() {
    ensureOpen();
    DfMemoryPool pool = df.memoryPool();
    return new MemoryPoolBridge(pool);
  }

  Map<String, ScalarFunctionHandle> scalarFunctions() {
    ensureOpen();
    try (DfStringArray names = df.scalarFunctionNames()) {
      return toHandleMap(names, ScalarFunctionHandle::new);
    }
  }

  Map<String, AggregateFunctionHandle> aggregateFunctions() {
    ensureOpen();
    try (DfStringArray names = df.aggregateFunctionNames()) {
      return toHandleMap(names, AggregateFunctionHandle::new);
    }
  }

  TaskContextBridge withSessionConfig(SessionConfigBridge cfg) {
    ensureOpen();
    return new TaskContextBridge(df.withSessionConfig(cfg.dfSessionConfig()));
  }

  TaskContextBridge withRuntime(RuntimeEnvBridge rt) {
    ensureOpen();
    return new TaskContextBridge(df.withRuntime(rt.dfRuntimeEnv()));
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("TaskContext has been closed");
    }
  }

  private static <V> Map<String, V> toHandleMap(
      DfStringArray names, java.util.function.Supplier<V> factory) {
    int len = (int) names.len();
    if (len == 0) {
      return Map.of();
    }
    Map<String, V> out = new LinkedHashMap<>(len);
    for (int i = 0; i < len; i++) {
      out.put(names.get(i), factory.get());
    }
    return Map.copyOf(out);
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      try {
        df.close();
      } catch (Exception e) {
        throw new DataFusionError("Failed to close TaskContext", e);
      }
    }
  }
}
