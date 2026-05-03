# Splitting Rust-Backed Providers Into Separate JARs

**Question.** Can the providers covered by [`rust-table-providers.md`](../rules/rust-table-providers.md)
(`flight`, `sqlite`, `postgres`, `mysql`, `duckdb`, …) be moved into separate published JARs, each
with its own native build, while still being linkable and easily usable at runtime by Java
consumers?

**Short answer.** Yes, several different ways, with very different cost/benefit profiles. The
cheapest win is a "Java-only split" that keeps a single fat dylib but publishes per-provider
JARs. A full split (separate dylibs that interoperate at runtime) is also feasible because the
existing architecture already isolates the cross-cutting state (Tokio runtime, `DfRustTableProvider`,
`DfRustCatalogProvider`) behind a Diplomat-shaped C ABI — but it requires a real architectural
change and ongoing maintenance cost.

This doc enumerates the options so we can pick one consciously.

---

## 1. What "split into separate JARs" can mean

Three independent axes. Any sensible plan picks a position on each.

| Axis | Coarse → Fine |
| --- | --- |
| **Java artifact split** | Single JAR with all wrappers / Per-provider JAR (one published artifact per feature) |
| **Native artifact split** | Single fat `cdylib` shipped in the core JAR / Per-provider `cdylib` shipped alongside its Java JAR |
| **Runtime separability** | Compile-time fat build / Runtime-loadable plugins (drop a JAR on the classpath, no rebuild) |

Today we sit at "single Java JAR (with per-feature source sets), single fat dylib, compile-time
choice via Cargo features". The user's question really targets the third column: can a downstream
consumer pull only `datafusion-ffi-java-sqlite` and have it work without dragging in postgres /
mysql / flight binary code?

## 2. The hard parts (specific to this codebase)

The architecture already does most of the work needed for a clean split, but there are real
coupling points to address.

### 2a. Shared singleton: the Tokio runtime

`factory_runtime.rs` exposes a process-global multi-thread Tokio runtime that every provider's
`open_pool_impl` / `open_table_impl` / `open_catalog_impl` enters via `block_on`. Each Rust
`cdylib` that includes its own copy of `tokio` would have its own runtime, its own thread pool,
its own `tokio::runtime::current_thread::TLS`. That's not catastrophic — the runtimes don't
have to share — but it's wasteful (N thread pools instead of one) and it leaks across the FFI
when something like `datafusion-table-providers` keeps a `JoinHandle` alive. **Solution:** runtime
must live in core; plugins acquire a `Handle` via a C ABI to schedule on it.

### 2b. Shared types: `DfRustTableProvider` / `DfRustCatalogProvider`

Every provider returns these core-defined Diplomat opaques (see `sqlite_factory.rs:46`,
`bridge.rs:2171`). They're `Arc<dyn TableProvider>` and `Arc<dyn CatalogProvider>` wrappers; the
trait objects come from the `datafusion` crate, which has *no stable Rust ABI*. Rust crate types
cannot safely cross between two independently-compiled `cdylib`s.

**Implication.** A plugin dylib cannot just `Box::new(DfRustTableProvider(arc))` and hand the
pointer to core. Either (a) plugin and core share the same `datafusion` crate version *and* are
compiled with identical compiler/codegen settings (fragile and ABI-unsafe), or (b) core exposes
a pure-C constructor that plugins call: `df_make_rust_table_provider(opaque_factory_callback)`.

### 2c. Federation wiring lives in `bridge.rs` (always-on)

`flight_sql_federation.rs` is `#[cfg(feature = "flight")]` *but* it's referenced from `bridge.rs`
(always-on). The `sqlite-federation` / `postgres-federation` / `mysql-federation` features on
`datafusion-table-providers` similarly bake federation analyzers into the core
`SessionContext`. A clean split would have to expose a registration entry point in core and let
each provider plugin call it on load.

### 2d. Diplomat emits all `Df*` classes into one Java package, unconditionally

Per `rust-table-providers.md`'s "Constraints" section, the Diplomat-generated
`org.apache.arrow.datafusion.generated.Df<Name>ConnectionPool` class is emitted regardless of
which Cargo feature is on, and crashes at static-init when its symbol isn't present. The current
mitigation is "keep references inside the per-feature source set so the runtime hazard tracks the
classpath." For a split, we'd want either (a) the generated class to live in the per-provider
JAR, or (b) the static-init failure to be a per-method failure with a clearer error.

### 2e. Single `NativeLoader`

`diplomat-config.toml`'s `clib_initializer` wires every generated class to one global
`NativeLoader`. Splitting across dylibs requires either multiple loaders or one composite
`SymbolLookup` that can be extended at runtime when plugins announce themselves.

## 3. Ecosystem patterns survey

There is no single "obvious" pattern in the JVM ecosystem — different projects pick different
points on the three axes above. The relevant prior art:

### 3a. Netty native transports (`netty-transport-native-epoll`, `netty-transport-native-kqueue`)

- **Java split:** yes, one Maven artifact per transport (`netty-transport-native-epoll`,
  `netty-transport-native-kqueue`, `netty-transport-native-io_uring`).
- **Native split:** yes, one `.so` per artifact, classifier-qualified per platform
  (`netty-transport-native-epoll-4.1.107.Final-linux-x86_64.jar`).
- **Runtime story:** each transport is independent; they don't talk to each other or share state
  beyond what's already provided by Java. They each expose their own `EventLoopGroup`
  implementation. Core Netty is pure Java.
- **Lesson:** clean per-feature JARs work great when **the native side has no cross-feature
  state**. Our case is harder — `tokio`, `SessionContext`, federation analyzers all want to be
  shared.

### 3b. JavaCPP / `bytedeco/javacpp-presets`

- **Java split:** yes, one artifact per preset (`opencv-platform`, `ffmpeg-platform`,
  `openblas-platform`, …).
- **Native split:** yes, each preset bundles its own dylibs.
- **Runtime story:** `org.bytedeco.javacpp.Loader.load(...)` walks a preset's declared
  dependencies, extracts dylibs to a tempdir in topological order, and `dlopen`s them. Cross-JAR
  dependencies are real — `libopencv_core.dylib` references symbols in
  `libopenblas.dylib`. JavaCPP makes this work by extracting all dylibs into the same tempdir and
  setting `RTLD_GLOBAL` so symbols resolve forward.
- **Lesson:** a runtime loader that owns dylib extraction order is the de facto Java solution
  for "plugin dylibs that depend on a core dylib." It's not exotic; it's how Java has shipped C
  libraries with cross-library dependencies for ~15 years.

### 3c. LWJGL 3

- **Java split:** yes, modules like `lwjgl-glfw`, `lwjgl-opengl`, `lwjgl-openal`.
- **Native split:** yes, classifier-qualified (`lwjgl-glfw-3.3.3-natives-macos-arm64.jar`).
- **Runtime story:** each module is a binding to a system or vendored C library. Cross-module
  state-sharing is minimal because the underlying C libraries are themselves independent. Where
  state is shared, it's done via the OS (a GLFW window referenced by an OpenGL context).
- **Lesson:** like Netty — natural fit when each module is a leaf binding, not a deep collaboration.

### 3d. AWS CRT for Java (`aws-crt-java`)

- **Java split:** **no**. Single artifact bundles all CRT components (HTTP, MQTT, S3, auth, IO,
  …) and a single fat dylib.
- **Why it didn't split:** the C components depend tightly on each other — `aws-c-s3` calls into
  `aws-c-http`, which calls into `aws-c-io`. AWS chose a fat-jar over coordinating a multi-dylib
  runtime story.
- **Lesson:** when the C side has tight intra-feature coupling, the cost of a runtime split
  often isn't worth the binary size win.

### 3e. Apache Arrow Java's Gandiva (`arrow-gandiva`)

- **Java split:** yes, separate Maven artifact for the LLVM-backed expression evaluator.
- **Native split:** yes, `libgandiva_jni.so` ships in that artifact.
- **Runtime story:** Gandiva talks to Arrow's C++ core via a process-internal C++ ABI; the
  `arrow-gandiva` JAR transitively loads `libarrow.so`. Cross-library state IS shared (an
  `arrow::Schema` constructed in core is consumed by Gandiva), but the projects are co-released
  with matching ABI and bundle their dylibs in the same JAR set.
- **Lesson:** even tight C++-level coupling can be split per-JAR if the dylibs are co-versioned
  and the loader extracts them next to each other. This is closest to our situation.

### 3f. RocksDB JNI, snappy-java, sqlite-jdbc, DuckDB Java, Tensorflow Java

- All single-artifact, single-dylib. Each is a binding to one C/C++ library; no need to split.
- **Lesson:** if all you ship is one binding, don't split.

### 3g. JNA / JNR-FFI

- Pure-Java FFI loaders. No code generation; you `dlopen` any dylib by name and dispatch
  reflectively or via `MethodHandle`s built on the fly. Used by JRuby, Redisson, etc.
- **Why it doesn't apply directly:** Diplomat is a code generator that produces typed Java
  bindings backed by FFM `MethodHandle`s. We've already paid the cost of typed bindings; we don't
  want to throw that away. But JNA's *dispatch* model — symbols looked up at runtime, library
  located by classpath search — is a useful conceptual reference for "runtime plugin dylib
  registration."

### 3h. Conscrypt

- Two-artifact pattern: `conscrypt-openjdk` (small, requires native lib on system) vs
  `conscrypt-openjdk-uber` (carries the dylib for all platforms in one artifact).
- **Lesson:** "uber" classifier for "everything bundled" is a recognizable convention. We could
  publish `datafusion-ffi-java` (slim) + `datafusion-ffi-java-uber` (everything) the same way.

### 3i. FFM-specific patterns

- `SymbolLookup.libraryLookup(Path, Arena)` — explicit per-library lookup.
- `SymbolLookup.loaderLookup()` — finds symbols in any library that the current class loader
  loaded via `System.loadLibrary`.
- `SymbolLookup.or()` — composes lookups. **This is the FFM-native answer to "I have multiple
  dylibs, find a symbol in any of them."**
- The `Linker` itself is stateless. Each call to `linker.downcallHandle(symbol, descriptor)`
  resolves at that moment against the lookup you pass.

The FFM API is *more* friendly to multi-dylib plugin schemes than JNI was. JNI's
`System.loadLibrary` registered functions in a single process-wide lookup, with no concept of
"load this dylib only for code that asks for it." FFM's per-arena, per-`SymbolLookup` model
inverts that.

## 4. Options for this project

Listed cheapest first.

### Option A — Status quo (do nothing)

- **What:** keep one published JAR with all the Java wrappers compiled in, one fat dylib, and let
  the slim build (`-PnoDefaultFeatures -Pfeatures=sqlite`) be a build-from-source story.
- **Pros:** zero work; matches AWS CRT, RocksDB JNI, snappy-java, DuckDB.
- **Cons:** every published consumer pays for every provider's binary size and JAR weight, even
  if they only want sqlite. CI takes longer; users without Docker can't easily skip the
  postgres/mysql code.
- **When this is right:** if no one's asking for a slim consumer experience and the binary size
  hasn't tipped past ~tolerable.

### Option B — Java-only split, single fat dylib (cheap and effective)

- **What:** publish multiple Maven artifacts, all of which depend on a `datafusion-ffi-java-core`
  artifact that owns the dylib. `datafusion-ffi-java-sqlite` contains only the per-feature
  Java sources currently in `src/sqlite/java`. The dylib still has every provider compiled in.
- **Pros:**
  - Almost no native-side work. Cargo build stays the same. The fat dylib lives in core; provider
    JARs are tiny pure-Java JARs.
  - Each provider JAR can carry its testcontainers / JDBC dependency declaration, so consumers
    don't drag in `org.postgresql:postgresql` unless they use `datafusion-ffi-java-postgres`.
  - Allows per-provider versioning if we ever want to release a fix to one provider without
    re-cutting all of them.
- **Cons:**
  - Doesn't reduce native binary size for downstream users. Someone who pulls only
    `…-sqlite` still loads the same fat dylib that has flight/postgres/mysql in it.
  - The Diplomat-generated `DfSqliteConnectionPool` is in core (`generated/`), and it'll fail at
    static init if the dylib was built with `--no-default-features --features postgres` (i.e.
    sqlite not compiled in). We need to either (a) move generated classes to per-provider JARs
    via `diplomat-config.toml` configuration, or (b) accept that the published fat dylib always
    includes every default-feature provider.
  - Per-provider tests still need the per-provider JDBC driver, which we already wire via
    `featureTestDeps`.
- **Effort:** ~1 week of Gradle + publishing work. Maven coordinate scheme needs to be agreed
  (`datafusion-ffi-java-core`, `…-sqlite`, …, `…-uber` (alias for everything)).
- **Closest ecosystem match:** this is essentially the Netty pattern with a fat shared dylib —
  similar to how Conscrypt uses the `-uber` classifier vs the slim variant.

### Option C — Java split + per-provider dylib, statically linked (one dylib per JAR, no shared core dylib)

- **What:** each provider crate produces its own `cdylib` that statically links the core code (a
  new `datafusion-ffi-core` Rust library crate, type `rlib`/`staticlib`). The provider JARs each
  ship their own dylib.
- **Pros:**
  - True binary-size payoff: a sqlite-only consumer downloads ~one provider's worth of native
    code.
  - No runtime cross-dylib coordination needed (each plugin's dylib is self-contained).
- **Cons (large):**
  - Every provider has its own copy of: the `tokio` runtime, the DataFusion core, `arrow`,
    `prost`, `bridge.rs` state. **Multiple provider JARs in the same process means multiple
    tokio runtimes and multiple SessionContext-side singletons.**
  - `DfRustTableProvider` defined in core: each plugin dylib has its own copy with its own
    layout. Pointers cannot cross between two provider dylibs in the same process. This breaks
    the entire "register provider in user-code SessionContext" model — there'd be N
    SessionContexts, one per dylib, that don't talk to each other.
  - Workable only if a consumer is guaranteed to use exactly *one* provider per process. That's
    a very narrow use case.
- **Verdict:** **don't do this.** The architecture already shares state (Tokio, federation,
  `DfRustTableProvider`) and that sharing is load-bearing. Statically linking core into every
  plugin breaks the sharing without offering enough binary-size savings to justify it.

### Option D — Core dylib + per-provider plugin dylibs that link against core via a C ABI

- **What:** the canonical "plugin architecture" answer. Core stays a `cdylib`; each provider
  becomes its own `cdylib` that **does not contain `datafusion`** but rather calls back into
  core via a C ABI. The provider JAR ships its own small dylib alongside core's dylib.
- **Architecture:**
  - Core exports a stable C-ABI registration table: `df_core_v1_get_runtime_handle()`,
    `df_core_v1_make_rust_table_provider(opaque_factory_callback) -> *mut DfRustTableProvider`,
    `df_core_v1_register_federation_analyzer(...)`, etc.
  - Provider dylibs use `extern "C"` to call into core. They carry their own provider-specific
    deps (`datafusion-table-providers/sqlite`, `rusqlite`, …) and *no* `datafusion` core.
  - On the Java side, core and each provider get their own `SymbolLookup` (via
    `SymbolLookup.libraryLookup(...)`). Diplomat bindings for core resolve against core's
    lookup; each provider's bindings resolve against its plugin lookup.
  - Plugin loading: `<Name>ConnectionPool.<staticInit>` triggers extraction + load of its dylib,
    then calls a `df_<provider>_v1_register(core_table)` entry point so the plugin can grab
    the handles it needs from core.
- **Pros:**
  - Real binary-size win (each provider plugin is small; the fat parts live in core which is
    shared).
  - State sharing works because everything goes through the C ABI in core.
  - This is the most ecosystem-native pattern (JavaCPP-presets, Apache Arrow Gandiva, OpenSSL
    engines, PostgreSQL extensions).
- **Cons:**
  - Substantial Rust refactor. We'd need:
    1. Split `datafusion-ffi-native` into a workspace: `core` (cdylib) + per-provider crates
       (each cdylib).
    2. Define a versioned C ABI (`df_core_v1_*`) that providers depend on instead of importing
       core's Rust types.
    3. Delete `crate::rust_table_provider::ffi::DfRustTableProvider` direct uses from provider
       crates — they call `df_core_v1_make_rust_table_provider(...)` instead.
    4. Modify Diplomat config so per-provider generated classes are emitted into per-provider
       output dirs (or modify the config-file to allow multiple `clib_initializer`s).
    5. Ship a custom `NativeLoader` that knows how to load core first, then per-provider dylibs,
       coordinating extraction via `Arena.ofShared()` and a per-plugin `SymbolLookup`.
  - Versioning discipline: any new symbol added to the core C ABI requires a v1 → v2 bump or
    additive `df_core_v2_*` symbols. Changing a struct layout = ABI break.
  - Federation wiring becomes more awkward — currently `bridge.rs` knows about
    `flight_sql_federation`. With a plugin model, federation registration becomes a plugin
    side-effect at load time, called via the registration table.
- **Effort:** ~4–6 weeks for one engineer plus testing across all platforms (especially Windows,
  where `dllimport`/`dllexport` require explicit annotations and the rpath story is different).
- **Closest ecosystem match:** JavaCPP-presets with cross-jar dylib dependencies + a custom
  `Loader.load()` that handles ordering. Also similar to Apache Arrow's
  `arrow-core` + `arrow-gandiva` pairing.

### Option E — Per-provider OS process via Arrow Flight (we already have most of this)

- **What:** every "provider" runs as a separate OS process; the JVM connects via Arrow Flight
  SQL. The only dylib loaded into the JVM is core + the `flight` provider.
- **Pros:**
  - No native cross-feature concerns; providers can be implemented in any language.
  - Already half-built — `flight_sql_test_server.rs` and the upstream `datafusion-table-providers/flight`
    feature do this for the test path.
- **Cons:**
  - Massive per-query overhead (process boundary + Arrow IPC serialization for every batch).
  - Doesn't help the user who wants `sqlite` zero-copy access to a local `.db` file.
  - Operational complexity: now consumers manage external processes.
- **When this is right:** for new providers backed by something that's already a network service
  (Snowflake, BigQuery, Databricks). Not a replacement for the in-process JNI model.

### Option F — Hybrid: Option B today, leave the door open for Option D later

- **What:** ship Option B's Java-only split now (artifacts: `…-core`, `…-sqlite`, `…-postgres`,
  …, `…-uber`). Internally, keep the single fat-dylib build but configure Diplomat to emit each
  provider's generated bindings into a `org.apache.arrow.datafusion.generated.<provider>`
  subpackage so the per-provider JAR can carry them.
- Then, in a separate later effort, transition the dylib build to Option D (separate plugin
  dylibs), without changing the public Java artifact surface. Consumers' Maven coordinates and
  imports stay stable across the migration.
- **Pros:** delivers the visible improvement (slim JARs, per-provider dependency declarations,
  per-provider versioning) cheaply, while preserving the option to do the harder work if/when
  binary size becomes a real complaint.
- **Cons:** none beyond Option B's, except the upfront Diplomat config tweak to per-provider
  packages.
- **Recommendation:** this is the right default if we want to ship something visible in 1–2
  weeks but keep the long-term door open.

## 5. Comparison table

| Option | JAR split | Native split | Runtime savings | Architectural cost | Notes |
| --- | --- | --- | --- | --- | --- |
| A. Status quo | None | None | None | None | Don't ship per-provider |
| B. Java-only split | Per provider | Single fat | None | Low (1 wk) | Slim JARs, fat dylib; closest to Netty's setup |
| C. Per-jar dylib, statically-linked core | Per provider | Per provider, fat | Some (size) | Medium | **Breaks runtime state-sharing** — don't do this |
| D. Core dylib + plugin dylibs | Per provider | Per provider, slim | Best | High (4–6 wk) | True plugin model; matches JavaCPP-presets / Gandiva |
| E. Out-of-process via Flight | N/A | N/A | None (different shape) | High operational | Use for cloud-DB providers, not local ones |
| F. Option B now, Option D later | Per provider | Single fat (now) → split (later) | Eventually best | Low up-front, deferred medium-high | Recommended |

## 6. Recommendation

Pursue **Option F**: ship the Java-only split first (Option B as the first deliverable), with
artifact coordinates that anticipate a later native split.

Concretely:

1. **Now (Option B work):**
   - Restructure `datafusion-ffi-java` into a Gradle multi-module: `…-core`, `…-flight`,
     `…-sqlite`, `…-postgres`, `…-mysql`, `…-duckdb`. Each provider module's source is what's
     today under `src/<feature>/java`.
   - Publish a `…-uber` BOM-like artifact that pulls the headline default set (matches today's
     fat JAR) for backwards compatibility.
   - Reconfigure Diplomat to emit per-provider generated classes into provider-specific Java
     subpackages (`org.apache.arrow.datafusion.generated.flight`,
     `…generated.sqlite`, …) so each provider JAR carries its own generated classes.
   - Keep building one fat dylib in the core JAR. Document that consumers using a slim JAR set
     are still loading a fat dylib until a future release.
   - Move per-provider testcontainers / JDBC dependency declarations to the per-provider
     `…Test` modules so consumers don't pull them transitively.

2. **Later, if binary-size complaints land (Option D work):**
   - Carve `datafusion-ffi-native` into a workspace with `core` cdylib + per-provider cdylibs.
   - Define `df_core_v1_*` C ABI (runtime handle, RustTableProvider/CatalogProvider factories,
     federation registration).
   - Ship per-provider dylibs in their existing JAR coordinates from step (1). No public Java
     API change.
   - Extend the loader to extract core dylib first, then provider dylibs, with `RTLD_GLOBAL`-
     equivalent symbol visibility (FFM gives this naturally via composed `SymbolLookup`).

This sequencing buys the visible improvements quickly and defers the architecturally invasive
work until there's evidence it's worth doing.
