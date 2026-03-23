# Public API

The public API (every public Java class, interface, enum, or record in `datafusion-ffi-java`) should be **AS SIMILAR AS
POSSIBLE** to types in the upstream Rust DataFusion crate. Claude should stop and ask for clarification when it wants
to add a feature that doesn't adhere to these rules.

## Package Structure

Java packages map directly to Rust DataFusion module paths:

| Java Package | Rust Module |
|---|---|
| `org.apache.arrow.datafusion.execution` | `datafusion::execution` |
| `org.apache.arrow.datafusion.logical_expr` | `datafusion::logical_expr` |
| `org.apache.arrow.datafusion.physical_plan` | `datafusion::physical_plan` |
| `org.apache.arrow.datafusion.catalog` | `datafusion_catalog` |
| `org.apache.arrow.datafusion.common` | `datafusion_common` |
| `org.apache.arrow.datafusion.config` | `datafusion_common::config` |
| `org.apache.arrow.datafusion.datasource` | `datafusion::datasource` |

## Naming

- `snake_case` methods/fields -> `camelCase` (e.g., `with_column_renamed` -> `withColumnRenamed`)
- Rust enum variants stay `SCREAMING_SNAKE_CASE` in Java enums (e.g., `JoinType::LeftSemi` -> `JoinType.LEFT_SEMI`)
- Rust `new()` / `new_with_*()` -> Java static factory methods (e.g., `SessionContext.create()`, `SessionContext.newWithConfig()`)
- Rust `Default` trait -> Java default (no-arg) constructor for records, or no-arg static factory for classes. Do not create a `defaults()` static method -- use the language's native default construction instead.

## Type Mappings

| Rust | Java | Notes |
|---|---|---|
| `Option<T>` | `Optional<T>` or nullable wrapper type | `Optional` for return types; nullable params documented with `@param` |
| `Vec<T>` | `List<T>` | Immutable lists preferred |
| Simple enum (no data) | `enum` | e.g., `JoinType`, `AggregateFunction` |
| Enum with variant data | `sealed interface` + `record` variants | e.g., `Expr`, `ScalarValue`, `TableReference` |
| Struct (value type) | `record` | e.g., `SortExpr`, `WindowFrame` |
| Struct (resource owner) | `class implements AutoCloseable` | e.g., `SessionContext`, `DataFrame` |
| Rust traits | Java `interface` | e.g., `TableProvider`, `CatalogProvider` |

## Rust Enum Variant Mapping

**Simple enums** (no associated data) become Java enums directly:

```java
// Rust: pub enum JoinType { Inner, Left, ... }
public enum JoinType { INNER, LEFT, RIGHT, FULL, LEFT_SEMI, LEFT_ANTI, ... }
```

**Enums with variant data** become sealed interfaces with record implementations:

```java
// Rust: pub enum WindowFrameBound { CurrentRow, Preceding(ScalarValue), Following(ScalarValue) }
public sealed interface WindowFrameBound {
    record CurrentRow() implements WindowFrameBound {}
    record Preceding(ScalarValue value) implements WindowFrameBound {}
    record Following(ScalarValue value) implements WindowFrameBound {}
}
```

## Resource Management

- Types that own native resources implement `AutoCloseable`
- **Consuming methods** (e.g., `DataFrame.filter()`, `.select()`, `.sort()`) close the source and return a new instance -- this enables fluent chaining: `ctx.sql(...).filter(...).select(...).count()`
- **Non-consuming methods** (e.g., `.show()`, `.count()`, `.schema()`) do NOT close the source

## Callback Interfaces

Java interfaces for custom data sources mirror Rust traits:

- `CatalogProvider` -> Rust `CatalogProvider` trait
- `SchemaProvider` -> Rust `SchemaProvider` trait
- `TableProvider` -> Rust `TableProvider` trait
- `ExecutionPlan` -> Rust `ExecutionPlan` trait
- `RecordBatchReader` -> Rust `RecordBatchReader` trait

Default method implementations are permitted for optional trait methods (e.g., `TableProvider.supportsFiltersPushdown()`).

## Builder / Configuration Patterns

Two patterns are used, matching whichever Rust uses:

1. **Builder pattern** for config structs with many optional fields:
   ```java
   ConfigOptions.builder().execution(ExecutionOptions.builder().batchSize(4096).build()).build();
   ```

2. **Fluent `with*` setters** for types that use builder-like chaining in Rust:
   ```java
   new ListingTableConfig(url).withListingOptions(options).withSchema(mySchema);
   ```

## Java-Specific Additions

These have no direct Rust equivalent but are permitted:

- `Functions` utility class with static factory methods (`col()`, `lit()`, `avg()`, etc.) -- mirrors Rust's `datafusion::prelude` free functions
- `CaseBuilder` / `WhenThen` for fluent CASE expression construction
- Builder classes for config types
- Sealed subinterfaces for typed access to `ScalarValue` variants (e.g., `DecimalValue`, `TimestampValue`)

## Functionality

Missing functionality is permitted but should be highlighted with a comment or TODO. Do not implement things differently from how Rust DataFusion does them -- if the Rust API uses a different approach, match it.

## Test Coverage Requirement

Every public class, public field, and public method **MUST** be referenced in at least one test. When adding new public API surface, you **MUST** also add a test that exercises it. This ensures the entire public API is validated across the FFI boundary. See `.claude/rules/testing.md` for details on coverage tooling and data verification requirements.

## What NOT to Do

- Do not invent new public API surface that diverges from Rust DataFusion's design
- Do not use Java generics to represent Rust generics -- use sealed hierarchies instead
- Do not expose FFM types (`MemorySegment`, etc.) in public API signatures
- Do not add convenience overloads without a clear Rust counterpart unless they are trivial wrappers
- Do not add public API without corresponding test coverage
