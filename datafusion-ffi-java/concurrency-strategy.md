# Concurrency Strategy: Rust Tokio <-> Java FFI Boundary

## Executive Summary

The new FFM-based bindings (`datafusion-ffi-java`) already use the right concurrency model: **synchronous, blocking APIs** with Rust's Tokio runtime hidden behind `block_on` at the FFI boundary. The legacy JNI bindings (`datafusion-java`) used `CompletableFuture` everywhere, but this is the wrong direction for modern Java. The new API should remain synchronous, and the legacy `CompletableFuture` pattern should **not** be adopted.

This document explains why, grounded in how Tokio works on the Rust side, how virtual threads work on the Java side, and what the maintainers of both ecosystems recommend.

---

## Part 1: How Tokio Works (Rust Side)

### The Runtime

Tokio provides a multi-threaded async runtime built on a **work-stealing scheduler**. When you create a `Runtime::new()`, Tokio spawns a pool of worker threads (defaulting to the number of CPU cores). Each worker has its own local task queue, plus a shared global injection queue. When a worker runs out of tasks, it steals from other workers' queues.

- Source: [Tokio Tutorial: Spawning](https://tokio.rs/tokio/tutorial/spawning)
- Source: [Tokio Blog: Making the Tokio scheduler 10x faster](https://tokio.rs/blog/2019-10-scheduler)

### `block_on` at the FFI Boundary

`Runtime::block_on(future)` is the standard mechanism for bridging synchronous code to asynchronous code. It takes an async future, submits it to the runtime, and **blocks the calling thread** until the future completes. Tokio's official documentation describes this as the correct pattern for FFI boundaries:

> "If your use of `block_on` is the entry point from synchronous to asynchronous code, then that is fine."
>
> -- [Tokio: Bridging with sync code](https://tokio.rs/tokio/topics/bridging)

This is exactly what `datafusion-ffi-native` does. Every `extern "C"` function that wraps an async DataFusion operation calls `runtime.block_on(async { ... })`:

```rust
// data_frame.rs, lines 45-50
runtime.block_on(async {
    match dataframe.clone().execute_stream().await {
        Ok(stream) => Box::into_raw(Box::new(stream)) as *mut c_void,
        Err(e) => set_error_return_null(error_out, &format!("Execute stream failed: {}", e)),
    }
})
```

The Java calling thread is **not** a Tokio worker thread, so `block_on` is safe and cannot cause the "nested runtime" panic. The async work executes on Tokio's worker thread pool while the Java thread parks until completion.

- Source: [Tokio API: Runtime::block_on](https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.block_on)
- Source: [Alice Ryhl: Async: What is blocking?](https://ryhl.io/blog/async-what-is-blocking/)

### The Nested Runtime Trap

Calling `block_on` from within an already-active Tokio runtime **panics**:

> "Cannot start a runtime from within a runtime."

This would happen if, say, a Rust callback into Java triggered another FFI call that tried `block_on` on the same runtime. The current architecture avoids this because Java callbacks (upcalls) are synchronous -- they return data directly without needing async.

- Source: [Tokio: Bridging with sync code](https://tokio.rs/tokio/topics/bridging)

### What `block_on` Gives Us

When the Java thread calls `block_on`, Tokio's full work-stealing runtime is active. The async work (SQL planning, query execution, file I/O) runs on Tokio's thread pool with full parallelism. Concurrency within a single DataFusion query (parallel partitions, async I/O) is handled entirely by Tokio. The Java caller doesn't need to know about any of this -- it just waits for the result.

---

## Part 2: How Virtual Threads Work (Java Side)

### What Virtual Threads Are

Virtual threads (JEP 444, finalized in JDK 21) are lightweight threads that don't maintain a 1:1 correspondence with OS threads. Instead, the JVM multiplexes many virtual threads onto fewer **carrier threads** (platform/OS threads). When a virtual thread blocks on I/O, it **unmounts** from its carrier, freeing that OS thread for other virtual threads.

| Property | Platform Thread | Virtual Thread |
|----------|----------------|----------------|
| OS thread per lifetime | Yes | No (mounts/unmounts) |
| Creation cost | ~1MB stack, kernel object | Cheap; millions possible |
| Blocking cost | Expensive (holds OS thread) | Cheap (unmounts from carrier) |
| Pooling recommended | Yes | No -- one per task |

> "Virtual threads are lightweight threads that dramatically reduce the effort of writing, maintaining, and observing high-throughput concurrent applications."
>
> -- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)

### The Key Insight: Blocking Is Cheap Again

The entire point of virtual threads is that **blocking a thread is no longer expensive**. This means the reactive/async programming style that was necessary to avoid blocking platform threads is no longer needed for scalability:

> "Because virtual threads can be plentiful, blocking them is cheap and encouraged. Therefore, you should write code in the straightforward synchronous style and use blocking I/O APIs."
>
> -- [Oracle JDK Virtual Threads Documentation](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)

### CompletableFuture Is Explicitly Discouraged

Oracle's own documentation labels `CompletableFuture` chains as **"NOT RECOMMENDED with virtual threads"**:

```java
// NOT RECOMMENDED with virtual threads
CompletableFuture.supplyAsync(info::getUrl, pool)
   .thenCompose(url -> getBodyAsync(url, HttpResponse.BodyHandlers.ofString()))
   .thenApply(info::findImage)
   ...
```

The recommended replacement is straightforward synchronous code running on virtual threads:

```java
// RECOMMENDED
Thread.startVirtualThread(() -> {
    String url = info.getUrl();
    String body = httpClient.send(request, ofString()).body();
    String image = info.findImage(body);
    // ...
});
```

- Source: [Oracle JDK 25 Virtual Threads Documentation](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)

### What JDK Maintainers Say

**Ron Pressler** (Project Loom technical lead):

> "Creating a virtual thread is cheap -- have millions, and don't pool them! Blocking a virtual thread is cheap -- be synchronous!"
>
> -- [State of Loom, Part 2](https://cr.openjdk.org/~rpressler/loom/loom/sol1_part2.html)

> "The asynchronous programming style fights the design of the Java platform at every turn and pays a high price in maintainability and observability."
>
> -- [State of Loom, Part 1](https://cr.openjdk.org/~rpressler/loom/loom/sol1_part1.html)

**Brian Goetz** (Java Language Architect):

> "I think Loom is going to kill reactive programming."
>
> -- [Interview, cited by Backendhance](https://backendhance.com/en/blog/2023/i-think-loom-is-going-to-kill-reactive-programming-brian-goetz/)

His argument is that reactive programming was a workaround for threads being expensive. Virtual threads solve the root cause, making the workaround unnecessary.

### Structured Concurrency (JEP 462/480/499/505/525)

Structured concurrency (`StructuredTaskScope`) is a preview feature that treats groups of concurrent tasks as a single unit of work:

```java
try (var scope = StructuredTaskScope.open(Joiner.allSuccessfulOrThrow())) {
    Subtask<ArrowReader> query1 = scope.fork(() -> ctx1.sql("SELECT ...").executeStream(alloc));
    Subtask<ArrowReader> query2 = scope.fork(() -> ctx2.sql("SELECT ...").executeStream(alloc));
    scope.join();
    // Both queries completed -- process results
}
```

Critically, `StructuredTaskScope` is designed for the **blocking paradigm**, not the async paradigm:

> "`CompletableFuture` is designed for the asynchronous programming paradigm, whereas `StructuredTaskScope` encourages the blocking paradigm."
>
> -- [JEP 505: Structured Concurrency](https://openjdk.org/jeps/505)

A synchronous DataFusion API composes naturally with structured concurrency. A `CompletableFuture`-based API does not.

---

## Part 3: FFM + Virtual Threads: The Pinning Question

### The Constraint

When a virtual thread executes a foreign function call (via the FFM API), it **pins** to its carrier thread -- meaning it cannot unmount, and the carrier OS thread is held for the duration of the call:

> "A virtual thread is pinned when it runs a `native` method or a foreign function."
>
> -- [Oracle JDK 25 Virtual Threads Documentation](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)

This means that while a virtual thread is blocked inside `runtime.block_on(async { ... })`, the carrier thread is consumed. If many virtual threads simultaneously make FFI calls, carrier threads can be exhausted.

### Why This Doesn't Change the Recommendation

1. **The JVM compensates automatically.** When pinning is detected, the JVM's scheduler can temporarily grow the carrier pool (up to 256 threads by default, configurable via `jdk.virtualThreadScheduler.maxPoolSize`).

2. **This is a known JVM limitation, not a library design problem.** JEP 491 (JDK 24) fixed `synchronized`-based pinning but explicitly left FFM/native pinning as a future item, noting it "should rarely cause issues."

3. **A `CompletableFuture` API would not eliminate the pinning.** The initial FFI call to submit the async work would still pin during the downcall. You'd just be trading one long pin for many short pins plus a complex callback infrastructure.

4. **The real concurrency happens inside Tokio.** A single DataFusion query already exploits parallelism through Tokio's work-stealing runtime (parallel partitions, async I/O). The Java side doesn't need to add concurrency -- it just needs to wait for the result.

5. **Monitoring exists.** The `jdk.VirtualThreadPinned` JFR event (enabled by default, 20ms threshold) reports pinning. Users who encounter scalability issues can detect and respond to them.

- Source: [JEP 491: Synchronize Virtual Threads without Pinning](https://openjdk.org/jeps/491)
- Source: [Java 24 Thread Pinning Revisited](https://mikemybytes.com/2025/04/09/java24-thread-pinning-revisited/)

### Projected Improvement

The JDK team has indicated they will revisit FFM/native pinning in future JDK releases. When this is resolved, synchronous FFI calls from virtual threads will unmount during the native call, and the current synchronous API will automatically benefit -- with zero code changes. A `CompletableFuture` API would not gain additional benefit because it's already structured around non-blocking semantics.

---

## Part 4: What Other Projects Do

### The Dominant Pattern: Block at the Boundary

Virtually every Rust+foreign-language FFI project uses `block_on` at the boundary for query engine workloads:

- **datafusion-python** uses synchronous wrappers over async DataFusion, with `block_on` in Rust. Async iteration via `FfiFuture` is reserved for record batch streaming.
- **PyO3 + Tokio** provides async bridges, but this is because Python's asyncio is single-threaded and event-loop-based -- blocking the main thread is fatal. Java has no such constraint.
- **NAPI-RS** (Node.js + Rust) auto-converts async Rust to JavaScript Promises, again because Node.js is single-threaded.
- **Apache OpenDAL** is the notable exception -- its Java binding returns `CompletableFuture` because its primary use case is storage I/O (S3, GCS) where thousands of operations may be in flight. This is a fundamentally different workload from a query engine.

The key pattern: **event-loop-based runtimes (Python asyncio, Node.js) need async bridges; thread-based runtimes (Java) don't.**

- Source: [Greptime: How to Supercharge Your Java Project with Rust](https://greptime.com/blogs/2025-05-13-java-rust-jni-integration-example)
- Source: [Tokio: Bridging with sync code](https://tokio.rs/tokio/topics/bridging)
- Source: [Rust wg-async: Using JNI](https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/aws_engineer/using_jni.html)

### Why the Legacy `CompletableFuture` API Was Wrong

The legacy `datafusion-java` bindings (JNI-based) wrapped everything in `CompletableFuture`:

```java
// Legacy API
CompletableFuture<DataFrame> sql(String sql);
CompletableFuture<ArrowReader> collect(BufferAllocator allocator);
CompletableFuture<Void> show();
```

This was likely modeled after JavaScript/Node.js patterns where async APIs are standard. But in Java:

1. **It forces async composition on every caller**, even those who just want sequential execution.
2. **It breaks structured concurrency** -- `CompletableFuture` escapes lexical scope, making it impossible to bound task lifetimes.
3. **It produces worse stack traces** -- errors originate in `CompletableFuture` completion handlers, not at the call site.
4. **It adds complexity with no benefit** -- the underlying Rust code uses `block_on` anyway, so the Java thread was already blocked. The `CompletableFuture` just wraps a synchronous call in unnecessary machinery.

---

## Part 5: Recommendation

### Keep the Current Synchronous API

The new `datafusion-ffi-java` API is already correct:

```java
// Current API (correct)
public DataFrame sql(String query) { ... }
public RecordBatchStream executeStream(BufferAllocator allocator) { ... }
```

Do **not** change these to return `CompletableFuture`. The synchronous API:

1. **Is what JDK maintainers recommend** for libraries that do I/O or call native code.
2. **Composes naturally with virtual threads** -- callers who need concurrency can run queries on virtual threads.
3. **Composes naturally with structured concurrency** -- callers can fork queries in a `StructuredTaskScope`.
4. **Produces clear stack traces** -- errors appear at the call site with the full context.
5. **Is simpler to implement and maintain** -- no future registry, no callback infrastructure, no concurrent coordination.

### How Callers Achieve Concurrency

Users who want parallel queries use Java's concurrency primitives, not library-provided futures:

```java
// Virtual threads (JDK 21+)
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    Future<RecordBatchStream> f1 = executor.submit(() -> ctx.sql("SELECT ...").executeStream(alloc));
    Future<RecordBatchStream> f2 = executor.submit(() -> ctx.sql("SELECT ...").executeStream(alloc));
    // ...
}

// Structured concurrency (preview, JDK 21+)
try (var scope = StructuredTaskScope.open(Joiner.allSuccessfulOrThrow())) {
    var query1 = scope.fork(() -> ctx.sql("SELECT ...").executeStream(alloc));
    var query2 = scope.fork(() -> ctx.sql("SELECT ...").executeStream(alloc));
    scope.join();
}

// Platform threads (JDK 8+)
ExecutorService pool = Executors.newFixedThreadPool(4);
Future<RecordBatchStream> f1 = pool.submit(() -> ctx.sql("SELECT ...").executeStream(alloc));
```

In all three cases, the **caller** chooses the concurrency model. The library doesn't force a choice.

### The Principle

**Libraries should block. Applications should choose concurrency.**

This is the consensus from:
- Ron Pressler (Project Loom lead): "Be synchronous."
- Brian Goetz (Java Language Architect): "Loom will kill reactive programming."
- Tokio maintainers: "`block_on` at the FFI boundary is the right pattern."
- Real-world projects: The blocking wrapper over async is the dominant pattern for Rust+Java FFI.

### Document the Threading Model

What would be valuable is documenting the threading model for users, specifically:

1. Each `SessionContext` owns a Tokio runtime with a multi-threaded work-stealing scheduler.
2. A single query already exploits parallelism internally via Tokio.
3. For multiple concurrent queries, use separate threads (virtual or platform).
4. FFM calls pin virtual threads to their carrier -- this is a JVM limitation, not a bug. The JVM compensates by growing the carrier pool.
5. Users experiencing pinning-related throughput issues can increase `jdk.virtualThreadScheduler.maxPoolSize`.

---

## Sources

### Tokio / Rust
- [Tokio Tutorial: Spawning](https://tokio.rs/tokio/tutorial/spawning)
- [Tokio Tutorial: Bridging with sync code](https://tokio.rs/tokio/topics/bridging)
- [Tokio Blog: Making the Tokio scheduler 10x faster](https://tokio.rs/blog/2019-10-scheduler)
- [Tokio API: Runtime::block_on](https://docs.rs/tokio/latest/tokio/runtime/struct.Runtime.html#method.block_on)
- [Alice Ryhl: Async: What is blocking?](https://ryhl.io/blog/async-what-is-blocking/)
- [Rust wg-async: Using JNI (Status Quo)](https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/aws_engineer/using_jni.html)

### Java / Project Loom
- [JEP 444: Virtual Threads](https://openjdk.org/jeps/444)
- [Oracle JDK 25: Virtual Threads Documentation](https://docs.oracle.com/en/java/javase/25/core/virtual-threads.html)
- [State of Loom, Part 1 (Ron Pressler)](https://cr.openjdk.org/~rpressler/loom/loom/sol1_part1.html)
- [State of Loom, Part 2 (Ron Pressler)](https://cr.openjdk.org/~rpressler/loom/loom/sol1_part2.html)
- [JEP 505: Structured Concurrency (Fifth Preview)](https://openjdk.org/jeps/505)
- [JEP 491: Synchronize Virtual Threads without Pinning](https://openjdk.org/jeps/491)
- [Brian Goetz: "Loom is going to kill reactive programming"](https://backendhance.com/en/blog/2023/i-think-loom-is-going-to-kill-reactive-programming-brian-goetz/)

### FFI Bridging Patterns
- [Greptime: Java + Rust JNI Integration](https://greptime.com/blogs/2025-05-13-java-rust-jni-integration-example)
- [Greptime: Bridging Async and Sync Rust](https://greptime.com/blogs/2023-03-09-bridging-async-and-sync-rust)
- [Apache OpenDAL Java Bindings](https://opendal.apache.org/bindings/java/)
- [DataFusion Python FFI Guide](https://datafusion.apache.org/python/contributor-guide/ffi.html)
- [Java 24 Thread Pinning Revisited](https://mikemybytes.com/2025/04/09/java24-thread-pinning-revisited/)
- [NullDeref: Supporting Both Async and Sync in Rust](https://nullderef.com/blog/rust-async-sync/)
