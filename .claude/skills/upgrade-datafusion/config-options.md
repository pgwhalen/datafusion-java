# Updating Java SessionConfig Option Records

The Java option records mirror DataFusion's `ConfigOptions` from `datafusion-common/src/config.rs`.
Their Javadoc is copied from Rust doc comments.

## Files to update

- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/CatalogOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/ExecutionOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/ExplainOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/FormatOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/OptimizerOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/ParquetOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/SqlParserOptions.java`
- `datafusion-ffi-java/src/main/java/org/apache/arrow/datafusion/config/SessionConfig.java`

## What to compare

Compare the `config_namespace!` macro invocations in the new `config.rs` against the current Java records:

- **Added options**: Add new fields to the Java record and its Builder
- **Removed options**: Remove the Java field
- **Renamed options**: Update the config key in `writeTo()` and rename the Java field
- **New option groups**: Create a new `*Options.java` record and add it to `SessionConfig.Builder`

Copy Javadoc from the Rust doc comments for any new fields.

## Updating tests

Update `datafusion-ffi-java/src/test/java/org/apache/arrow/datafusion/config/SessionConfigTest.java`:

- Each `testAll*OptionsViaShow()` test sets every field via the builder and asserts the value via `SHOW`
- Add new fields to the appropriate test method's builder and a corresponding `assertShow` call
- Remove deleted fields from both the builder and assertions
- If a new option group was added, create a new `testAll*OptionsViaShow()` test method following the same pattern
