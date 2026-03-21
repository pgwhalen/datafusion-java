# Documentation

## docs.rs Links in Public Javadoc

Every public Java class, interface, enum, or record in `datafusion-ffi-java` that maps to a Rust DataFusion type MUST have a `@see` link to the corresponding docs.rs page. Every public method that maps to a Rust method MUST also have a method-level `@see` link.

### Version

DataFusion version: **52.1.0**

When upgrading DataFusion, update all docs.rs URLs to the new version.

### URL Patterns

| Rust Item Type | URL Pattern |
|---|---|
| Struct | `https://docs.rs/{crate}/{version}/{crate_underscore}/path/struct.{Name}.html` |
| Trait | `https://docs.rs/{crate}/{version}/{crate_underscore}/path/trait.{Name}.html` |
| Enum | `https://docs.rs/{crate}/{version}/{crate_underscore}/path/enum.{Name}.html` |
| Function | `https://docs.rs/{crate}/{version}/{crate_underscore}/path/fn.{name}.html` |
| Method | Append `#method.{rust_name}` to the type URL |
| Variant | Append `#variant.{Name}` to the enum URL |

### Crate Base URLs

| Crate | Base URL |
|---|---|
| `datafusion` | `https://docs.rs/datafusion/52.1.0/datafusion` |
| `datafusion-expr` | `https://docs.rs/datafusion-expr/52.1.0/datafusion_expr` |
| `datafusion-catalog` | `https://docs.rs/datafusion-catalog/52.1.0/datafusion_catalog` |
| `datafusion-datasource` | `https://docs.rs/datafusion-datasource/52.1.0/datafusion_datasource` |
| `datafusion-common` | `https://docs.rs/datafusion-common/52.1.0/datafusion_common` |
| `datafusion-physical-expr` | `https://docs.rs/datafusion-physical-expr/52.1.0/datafusion_physical_expr` |

### Class-Level Format

```java
/**
 * Description of the Java type.
 *
 * @see <a href="URL">Rust DataFusion: TypeName</a>
 */
```

### Method-Level Format

Every public method that maps to a Rust method MUST have a `@see` link using the `#method.{rust_name}` anchor. Use the Rust snake_case name (e.g., `with_column_renamed` for Java `withColumnRenamed`). Java overloads that map to the same Rust method all link to the same anchor.

```java
/**
 * Description of the method.
 *
 * @param foo param description
 * @see <a
 *     href="https://docs.rs/datafusion/52.1.0/datafusion/dataframe/struct.DataFrame.html#method.select">Rust
 *     DataFusion: DataFrame::select</a>
 */
```

Skip `@see` only for methods with no Rust equivalent (e.g., `close()`, `toString()`, pure Java helpers).

### Special Cases

- **Pure Java helpers** (e.g., `CaseBuilder`, `WhenThen`, builder classes): No `@see` link needed.
- **Callback interfaces** (e.g., `TableProvider`, `CatalogProvider`): Link to the Rust trait.
- **Enums**: Link to the Rust enum.
- **Package-private classes** (`*Bridge`, `*Adapter`, `*Converter`, `*Ffi`, `*Handle`): No `@see` link.

### Verification

Docs.rs links are validated by `DocsLinkValidationDoclet.java`, a custom Doclet that uses the compiler-backed Doclet API (`jdk.javadoc.doclet`) to access types, methods, and parsed `@see` tags — no regex-based source parsing needed.

Validation runs automatically:

- **`verifyDocsLinks`** (offline, fast) runs as a dependency of the `test` task — presence, format, and staleness checks run on every test invocation.
- **`verifyDocsLinksHttp`** (requires internet) runs in the Publish FFI CI workflow (`build-jar` job) to validate each URL returns HTTP 200 before release.

```bash
# Runs automatically with tests:
./gradlew :datafusion-ffi-java:test

# Run link checks manually:
./gradlew :datafusion-ffi-java:verifyDocsLinks

# Run HTTP validation manually:
./gradlew :datafusion-ffi-java:verifyDocsLinksHttp
```

The doclet checks:

1. Every public type (excluding internal and pure-Java helpers) has a class-level `@see` docs.rs link
2. URL format matches `https://docs.rs/datafusion*/{version}/...`
3. Rust type label in `@see` matches the Java class name (with configurable exceptions)
4. Public methods on types with class-level links have method-level `@see` links (when not deferred)
5. Method-level URLs contain `#method.` anchors

**MUST be run** when:
- Any public Java class's or method's `@see` documentation changes
- During a DataFusion version upgrade (after updating URLs)

Name mismatches that are intentional (e.g., Java `Functions` vs Rust `datafusion::prelude`) are listed in `NAME_EXCEPTIONS` in the doclet. The doclet also validates that all exclusion entries (`NO_SEE_LINK_REQUIRED`, `CLASS_METHOD_EXCLUSIONS`, etc.) still reference real types and methods — stale entries cause build failures.
