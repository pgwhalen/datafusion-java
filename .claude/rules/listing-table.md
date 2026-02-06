# Listing Table

This document describes the listing table FFI bindings, which allow Java users to implement custom file formats and register file-backed tables with DataFusion.

## Design Goals

1. **Java API should mirror DataFusion's real trait hierarchy** -- The Java interfaces should look like the Rust traits a DataFusion developer would recognize. A flat `FileFormat.openFile()` collapses too many concerns; instead the API uses a multi-level chain (`FileFormat` -> `FileSource` -> `FileOpener` -> `RecordBatchReader`) so Java programmers can wire things together as they like.

2. **Move logic into Java, keep Rust thin** -- The Rust side should be a thin FFI adapter. The Java side owns the interface decomposition and object creation. This keeps the Rust module (`listing_table.rs`) focused on callback plumbing rather than business logic.

3. **Reuse the same multi-level callback pattern as catalogs** -- The `FileFormat` -> `FileSource` -> `FileOpener` chain follows the exact same pattern as `CatalogProvider` -> `SchemaProvider` -> `TableProvider` -> `ExecutionPlan` -> `RecordBatchReader`: each level has a Java interface, a Handle class (FFI bridge), a Rust callback struct, and a Rust wrapper struct.

4. **Pure Java config classes** -- `ListingTableUrl`, `ListingOptions`, and `ListingTable` are pure Java data carriers. They don't wrap native pointers. All native object creation happens in a single FFI registration call (`datafusion_context_register_listing_table`).

5. **Builder methods mirror Rust's `ListingTableConfig`** -- `ListingTable.builder(url)` mirrors `ListingTableConfig::new(table_path)`, `ListingTable.builderWithMultiPaths(urls)` mirrors `ListingTableConfig::new_with_multi_paths(table_paths)`, and builder methods `withListingOptions()` / `withSchema()` mirror `with_listing_options()` / `with_schema()`. The `ListingTable` stores a `List<ListingTableUrl> tablePaths` (not a single URL) to support multiple paths natively.
