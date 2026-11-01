---
paths:
  - "core/src/main/java/org/apache/datafusion/**/*.java"
---

# Public API

The public Java API mirrors the upstream Rust DataFusion API. Before adding a public type or method,
find its Rust counterpart and match it.

- Use the Rust type, trait, method, and enum names: `snake_case` becomes `camelCase`, and enum
  variants become `SCREAMING_SNAKE_CASE` (`JoinType::LeftSemi` becomes `JoinType.LEFT_SEMI`).
- Don't invent a Java-only name or type when Rust already has one.
- Rust traits map to Java interfaces. Optional trait methods are `default` methods.
- Covering only part of a Rust API is fine. When you leave something out, note the gap rather than
  reshaping the API around it.
- Java-only additions are OK when they're needed for Java idioms (varargs, `Optional`). Say in the
  Javadoc that the addition is Java-specific.
- Each public type's Javadoc names its Rust counterpart ("Mirrors DataFusion's {@code ScanArgs}").
  A docs.rs link is welcome but not required.
- A Java method whose Rust counterpart returns `Result` throws the unchecked `DataFusionException`
  subclass matching the `DataFusionError` variant (`PlanException`, `ExecutionException`, …). Don't
  add checked exceptions.
- Never expose JNI details in public signatures: no native handles, no `byte[]` marshalling
  payloads, no codes for crossing the JNI boundary. Keep helpers like `toBytes()` package-private.
- A type that holds a native handle (`SessionContext`, `DataFrame`) implements `AutoCloseable`, and
  `close()` frees the native resource. Plain value types and Java-implemented interfaces don't.
