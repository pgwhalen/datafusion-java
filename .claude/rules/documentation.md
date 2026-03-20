# Documentation

## docs.rs Links in Public Javadoc

Every public Java class, interface, enum, or record in `datafusion-ffi-java` that maps to a Rust DataFusion type MUST have a `@see` link to the corresponding docs.rs page.

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

Only add method-level `@see` when the mapping is non-obvious or the method name differs from Rust.

### Special Cases

- **Pure Java helpers** (e.g., `CaseBuilder`, `WhenThen`, builder classes): No `@see` link needed.
- **Callback interfaces** (e.g., `TableProvider`, `CatalogProvider`): Link to the Rust trait.
- **Enums**: Link to the Rust enum.
- **Package-private classes** (`*Bridge`, `*Adapter`, `*Converter`, `*Ffi`, `*Handle`): No `@see` link.

### Verification Script

Run `./scripts/verify-docs-links.sh` to validate all docs.rs links. It checks:

1. Every URL returns HTTP 200
2. The Rust type name in the `@see` label matches the Java class name

**MUST be run** when:
- Any public Java class's `@see` documentation changes
- During a DataFusion version upgrade (after updating URLs)

Name mismatches that are intentional (e.g., Java `ScalarUdf` vs Rust `ScalarUDF`) are listed in the `EXCEPTIONS` table inside the script. Add new entries there when a Java class intentionally uses a different name than its Rust counterpart.
