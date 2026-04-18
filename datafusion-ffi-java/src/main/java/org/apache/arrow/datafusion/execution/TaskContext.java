package org.apache.arrow.datafusion.execution;

import java.util.Map;
import java.util.Optional;

/**
 * Task-level execution context handed to a Java {@link
 * org.apache.arrow.datafusion.physical_plan.ExecutionPlan} during {@code execute}.
 *
 * <p>Mirrors (a subset of) Rust DataFusion's {@code TaskContext}. Callers can read the session
 * id, task id, session config, runtime environment, memory pool, and registered UDF/UDAF names,
 * and derive new task contexts via {@link #withSessionConfig(SessionConfig)} / {@link
 * #withRuntime(RuntimeEnv)}.
 *
 * <p>The TaskContext passed into {@code ExecutionPlan.execute} is owned by the adapter and closed
 * after execute returns; implementations should not retain the reference. In contrast, instances
 * produced by {@code with*} methods are owned by the caller and must be closed.
 *
 * <p>Note on {@code with*} semantics: Rust's {@code with_session_config} / {@code with_runtime}
 * consume {@code self}. The Java bindings deliberately do NOT close {@code this} — a new
 * TaskContext is returned alongside the original so that the adapter-owned execute() parameter
 * remains valid.
 *
 * <p>Example:
 *
 * {@snippet :
 * public RecordBatchReader execute(int partition, TaskContext taskCtx, BufferAllocator allocator) {
 *     String sid = taskCtx.sessionId();
 *     Map<String, String> opts = taskCtx.sessionConfig().options();
 *     return new MyBatchReader(data);
 * }
 * }
 *
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html">Rust
 *     DataFusion: TaskContext</a>
 */
public class TaskContext implements AutoCloseable {
  private final TaskContextBridge bridge;

  /** Package-private constructor — Task contexts come from the FFI adapter or {@code with*}. */
  TaskContext(TaskContextBridge bridge) {
    this.bridge = bridge;
  }

  /**
   * Internal FFI entry point: adopt ownership of a raw DfTaskContext pointer. Not intended for
   * user code.
   *
   * @param addr a non-zero raw pointer produced by the Rust execute() upcall
   * @return a TaskContext that owns and will close the underlying DfTaskContext
   */
  public static TaskContext fromNativeAddress(long addr) {
    return new TaskContext(TaskContextBridge.adopt(addr));
  }

  /**
   * Session ID for the session that produced this task.
   *
   * <p>See {@link TaskContext} class-level example.
   *
   * @return the session id
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.session_id">Rust
   *     DataFusion: TaskContext::session_id</a>
   */
  public String sessionId() {
    return bridge.sessionId();
  }

  /**
   * Task identifier, if one was assigned.
   *
   * <p>Example:
   *
   * {@snippet :
   * Optional<String> tid = taskCtx.taskId();
   * tid.ifPresent(id -> log.info("task {}", id));
   * }
   *
   * @return the task id, or {@link Optional#empty()} if none
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.task_id">Rust
   *     DataFusion: TaskContext::task_id</a>
   */
  public Optional<String> taskId() {
    return bridge.taskId();
  }

  /**
   * Snapshot of the session configuration.
   *
   * <p>Example:
   *
   * {@snippet :
   * try (SessionConfig cfg = taskCtx.sessionConfig()) {
   *     String batch = cfg.options().get("datafusion.execution.batch_size");
   * }
   * }
   *
   * @return a new SessionConfig (must be closed)
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.session_config">Rust
   *     DataFusion: TaskContext::session_config</a>
   */
  public SessionConfig sessionConfig() {
    return new SessionConfig(bridge.sessionConfig());
  }

  /**
   * Runtime environment used to execute this task.
   *
   * <p>Example:
   *
   * {@snippet :
   * try (RuntimeEnv rt = taskCtx.runtimeEnv()) {
   *     // use rt
   * }
   * }
   *
   * @return a new RuntimeEnv handle (must be closed)
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.runtime_env">Rust
   *     DataFusion: TaskContext::runtime_env</a>
   */
  public RuntimeEnv runtimeEnv() {
    return new RuntimeEnv(bridge.runtimeEnv());
  }

  /**
   * Memory pool associated with this task's runtime.
   *
   * <p>Example:
   *
   * {@snippet :
   * try (MemoryPool pool = taskCtx.memoryPool()) {
   *     long bytes = pool.reserved();
   * }
   * }
   *
   * @return a new MemoryPool handle (must be closed)
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.memory_pool">Rust
   *     DataFusion: TaskContext::memory_pool</a>
   */
  public MemoryPool memoryPool() {
    return new MemoryPool(bridge.memoryPool());
  }

  /**
   * Registered scalar UDFs, keyed by name.
   *
   * <p>Values are placeholder {@link ScalarFunctionHandle} records — only the key set is
   * meaningful today. See {@link ScalarFunctionHandle} for the planned evolution.
   *
   * <p>Example:
   *
   * {@snippet :
   * boolean hasAbs = taskCtx.scalarFunctions().containsKey("abs");
   * }
   *
   * @return map of scalar function names to placeholder handles
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.scalar_functions">Rust
   *     DataFusion: TaskContext::scalar_functions</a>
   */
  public Map<String, ScalarFunctionHandle> scalarFunctions() {
    return bridge.scalarFunctions();
  }

  /**
   * Registered aggregate UDFs, keyed by name.
   *
   * <p>Values are placeholder {@link AggregateFunctionHandle} records — only the key set is
   * meaningful today. See {@link AggregateFunctionHandle} for the planned evolution.
   *
   * <p>Example:
   *
   * {@snippet :
   * boolean hasSum = taskCtx.aggregateFunctions().containsKey("sum");
   * }
   *
   * @return map of aggregate function names to placeholder handles
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.aggregate_functions">Rust
   *     DataFusion: TaskContext::aggregate_functions</a>
   */
  public Map<String, AggregateFunctionHandle> aggregateFunctions() {
    return bridge.aggregateFunctions();
  }

  /**
   * Returns a new TaskContext with the given session config.
   *
   * <p>Unlike Rust's consuming form, {@code this} is NOT closed. The returned instance is owned
   * by the caller.
   *
   * <p>Example:
   *
   * {@snippet :
   * try (TaskContext overridden = taskCtx.withSessionConfig(newCfg)) {
   *     // overridden reflects the new config
   * }
   * }
   *
   * @param config the new session config
   * @return a new TaskContext (must be closed)
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.with_session_config">Rust
   *     DataFusion: TaskContext::with_session_config</a>
   */
  public TaskContext withSessionConfig(SessionConfig config) {
    return new TaskContext(bridge.withSessionConfig(config.bridge));
  }

  /**
   * Returns a new TaskContext with the given runtime environment.
   *
   * <p>Unlike Rust's consuming form, {@code this} is NOT closed. The returned instance is owned
   * by the caller.
   *
   * <p>Example:
   *
   * {@snippet :
   * try (TaskContext overridden = taskCtx.withRuntime(newRuntime)) {
   *     // overridden uses newRuntime
   * }
   * }
   *
   * @param runtime the new runtime environment
   * @return a new TaskContext (must be closed)
   * @see <a
   *     href="https://docs.rs/datafusion/52.1.0/datafusion/execution/struct.TaskContext.html#method.with_runtime">Rust
   *     DataFusion: TaskContext::with_runtime</a>
   */
  public TaskContext withRuntime(RuntimeEnv runtime) {
    return new TaskContext(bridge.withRuntime(runtime.bridge));
  }

  @Override
  public void close() {
    bridge.close();
  }
}
