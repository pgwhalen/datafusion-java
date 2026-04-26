# Rust Table Providers

This repo exposes a family of `TableProvider` / `CatalogProvider` implementations that live in Rust (mostly thin wrappers over `datafusion-table-providers`). Each provider is shipped as its own **optional Cargo feature** with matching Gradle source sets, so a slim build can pick only what it needs and a default build still ships the headline set. **Every new Rust-backed provider** MUST follow this pattern.

## Scope

Applies to:

- Rust factory modules in `datafusion-ffi-native/src/<name>_factory.rs` (and adjuncts like `flight_sql_federation.rs`, `flight_sql_test_server.rs`).
- Their Java wrappers under `datafusion-ffi-java/src/<feature>/java/org/apache/arrow/datafusion/providers/<feature>/`.
- Their Java tests under `datafusion-ffi-java/src/<feature>Test/java/org/apache/arrow/datafusion/providers/<feature>/`.

Does NOT apply to always-on infrastructure (`bridge.rs`, `rust_table_provider.rs`, `RustTableProvider` / `RustCatalogProvider` base classes, federation wiring in `bridge.rs`).

## Provider Catalog

| Feature       | In default? | Cargo deps wired in                                                | Java production dir         | Java test dir            |
|---------------|-------------|---------------------------------------------------------------------|------------------------------|---------------------------|
| `flight`      | yes         | `arrow-flight`, `tonic`, `datafusion-table-providers/flight`        | `src/flight/java`            | `src/flightTest/java`     |
| `sqlite`      | yes         | `datafusion-table-providers/sqlite-bundled` + `…/sqlite-federation` | `src/sqlite/java`            | `src/sqliteTest/java`     |
| `postgres`    | yes         | `datafusion-table-providers/postgres` + `…/postgres-federation`     | `src/postgres/java`          | `src/postgresTest/java`   |
| `mysql`       | yes         | `datafusion-table-providers/mysql` + `…/mysql-federation`           | `src/mysql/java`             | `src/mysqlTest/java`      |
| `duckdb`      | no          | `datafusion-table-providers/duckdb-federation`                      | `src/duckdb/java`            | `src/duckdbTest/java`     |
| `test-server` | yes         | `arrow-flight`, `tonic`, `uuid`, `dashmap` (implies `flight`)       | n/a — used by `flight` tests | n/a                       |

The default list is duplicated in two places — both must agree:

- `datafusion-ffi-native/Cargo.toml` `[features.default]`
- `datafusion-ffi-java/build.gradle` `defaultJavaFeatures` (minus `test-server`, which is Rust-side only).

## Layout

### Rust (`datafusion-ffi-native/`)

- `Cargo.toml` declares each feature in `[features]`. Anything that's only useful when one provider is on (e.g. `arrow-flight`, `tonic`) is an `optional = true` dep, pulled in via `dep:` from inside the feature's list. `[features.default]` lists the providers that ship by default.
- `src/<name>_factory.rs` holds one provider's Diplomat bridge + factory functions. `sqlite_factory.rs` is the canonical template — copy its shape (Diplomat opaque, `open_pool_impl`, `open_table_impl`, `open_catalog_impl`).
- `src/lib.rs` declares each provider module behind `#[cfg(feature = "<name>")]`. The cfg goes on the `mod` line, not inside the module.

### Java (`datafusion-ffi-java/`)

Standard Gradle named source sets, one pair per feature:

```
src/<feature>/java/org/apache/arrow/datafusion/providers/<feature>/<Name>ConnectionPool.java
src/<feature>Test/java/org/apache/arrow/datafusion/providers/<feature>/<Name>ProviderTest.java
```

Per-feature wrappers extend the always-on `RustTableProvider` / `RustCatalogProvider` base classes in `src/main/java/org/apache/arrow/datafusion/providers/`.

`build.gradle` registers a feature's source sets only when its name is in `enabledFeatureSet` (= `defaultJavaFeatures` ∪ `-Pfeatures=...`, minus everything if `-PnoDefaultFeatures` is set). Per-feature test deps go in the `featureTestDeps` map; tests run inside the main `test` task — don't add a `<feature>Test` Gradle task.

## Build-Time Switches

| Property                                | Effect                                                                                                                  |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| (none)                                  | Builds Cargo's `[features.default]`. Java compiles `defaultJavaFeatures` source sets.                                   |
| `-Pfeatures=duckdb`                     | Adds `--features duckdb` to cargo (in addition to defaults). Java also compiles `src/duckdb/java` + `src/duckdbTest/java`. |
| `-PnoDefaultFeatures`                   | Adds `--no-default-features` to cargo. Java skips all `defaultJavaFeatures` source sets.                                |
| `-PnoDefaultFeatures -Pfeatures=sqlite` | Slim build: only `sqlite` enabled, both Rust + Java.                                                                    |

## Adding a New Provider

1. **`Cargo.toml`** — declare the feature, add to `[features.default]` if it should ship by default, wire any new optional `dep:` entries.
2. **`src/<name>_factory.rs`** — implement the factory module. Use `#[diplomat::bridge]` and the helpers in `factory_runtime.rs` / `diplomat_util.rs` exactly like `sqlite_factory.rs`.
3. **`src/lib.rs`** — add `#[cfg(feature = "<name>")] mod <name>_factory;`.
4. **`build.gradle`** — if it's on by default, add the name to `defaultJavaFeatures`. Add an entry to `featureTestDeps` (empty list `[]` if no extra JARs).
5. **`src/<name>/java/.../<Name>ConnectionPool.java`** — public API class extending `RustTableProvider` / `RustCatalogProvider`.
6. **`src/<name>Test/java/.../<Name>ProviderTest.java`** — at least one end-to-end test.
7. **CI** — for opt-in (non-default) providers, extend the `optional-features-test` job in `.github/workflows/publish-ffi.yml`.

## Constraints

- **Don't put feature-gated code under `src/main/java/`.** It would compile in every build (including slim ones) and break when its Diplomat bindings aren't there.
- **The `#[cfg]` belongs at the `mod` declaration in `lib.rs`, not inside the module.** Mixing both makes it easy to leak unguarded `use` statements that break slim builds.
- **`test-server` implies `flight`.** Don't reverse the dependency. Anything else that uses `arrow-flight` directly should also depend on `flight`.
- **Don't reach into `org.apache.arrow.datafusion.generated.*`** from anywhere outside a feature's source set. Diplomat emits the `Df<Name>ConnectionPool` classes unconditionally, and they throw at static-init time if the matching cargo feature wasn't built. Keeping those references inside `src/<feature>/java` makes the runtime hazard track the source set.

## Verification

```bash
# Default features — what most users will run
./gradlew :datafusion-ffi-java:test

# Add an opt-in feature
./gradlew :datafusion-ffi-java:test -Pfeatures=duckdb

# Slim build: only one feature, no defaults
./gradlew :datafusion-ffi-java:test -PnoDefaultFeatures -Pfeatures=sqlite

# Cargo-only sanity (the slim build above also catches this)
cargo build --no-default-features --features sqlite \
  --manifest-path datafusion-ffi-native/Cargo.toml
```

Any change to a provider's Rust module, Java wrapper, test deps, or default-set membership MUST be exercised by at least one of the above invocations before merge.

### Testcontainers

`postgres` and `mysql` tests need a real database, so they spin up the official upstream image via [testcontainers-java](https://java.testcontainers.org/) in `@BeforeAll`:

```java
container = new PostgreSQLContainer<>(DockerImageName.parse("postgres:<tag>"))
    .withDatabaseName("test").withUsername("test").withPassword("test");
container.start();
```

Each test class manages its own container lifecycle (`@BeforeAll start`, `@AfterAll stop`), then seeds via the corresponding JDBC driver (`org.postgresql:postgresql`, `com.mysql:mysql-connector-j`) before opening a `*ConnectionPool` against `container.getMappedPort(...)`. testcontainers + JDBC deps are wired conditionally in `build.gradle`'s `featureTestDeps` map and the `needsTestcontainers` block.

Requirements & escape hatches:

- **Docker daemon must be running.** Testcontainers fails fast otherwise. On macOS, that means Docker Desktop is up and the socket at `~/.docker/run/docker.sock` (symlinked from `/var/run/docker.sock`) is reachable. Verify with `docker info`.
- **`SKIP_TESTCONTAINERS=true`** — set this env var to skip the test class (the test uses `Assumptions.assumeFalse(...)` in `@BeforeAll`). Use this in CI environments without Docker.
- **First run pulls images.** Images are downloaded once and cached locally; subsequent runs start in a few seconds.
- **One container per class.** Don't switch to per-test containers — the existing `@BeforeAll` start + `@AfterAll` stop is the contract.

Other providers don't need Docker:

- `sqlite` seeds via `org.xerial:sqlite-jdbc` against a temp file.
- `duckdb` seeds via `org.duckdb:duckdb_jdbc` against a temp file.
- `flight` uses the in-process `DfFlightSqlTestServer` exposed by the `test-server` cargo feature.
