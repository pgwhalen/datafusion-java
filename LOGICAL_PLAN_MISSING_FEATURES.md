# LogicalPlan: Missing Features & How to Add Them

This document describes features of Rust DataFusion's `LogicalPlan` enum that are **not yet exposed** in the Java sealed interface, along with implementation guidance for each.

## Overview

The Java `LogicalPlan` sealed interface exposes all 25 Rust variants and common methods (`schema()`, `inputs()`, `display*()`, `maxRows()`, `containsOuterReference()`). The items below represent more advanced functionality that is feasible to add but was deferred from the initial implementation.

---

## 1. Tree Traversal Methods

**Rust methods:** `apply()`, `transform()`, `transform_down()`, `transform_up()`, `visit()`, `map_children()`, `rewrite()`

**Why missing:** These methods take closures (`F: FnMut(&LogicalPlan) -> Result<TreeNodeRecursion>`) that cannot cross the FFI boundary directly.

**How to add:**
- **Option A (Java-side traversal):** Implement traversal purely in Java using the already-exposed `inputs()` method. Define a `TreeNodeVisitor<LogicalPlan>` interface and implement `apply()` / `transform()` as recursive Java methods that walk the sealed interface tree. This requires no Rust changes.
- **Option B (Native traversal with callback):** Add a Diplomat trait for the visitor callback, register a Java upcall stub, and call Rust's `apply()` with the upcall. This is complex but preserves Rust's optimized traversal.

**Recommended:** Option A. Since `fromBridge()` already materializes the full tree, Java-side traversal is straightforward and avoids FFI complexity.

---

## 2. Expression Mutation: `with_new_exprs()`

**Rust method:** `with_new_exprs(exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan>`

**Why missing:** Creates a new plan node with replaced expressions and inputs. Requires serializing expressions back to Rust and constructing a new native plan.

**How to add:**
1. Add a Rust method `DfLogicalPlan::with_new_exprs(expr_bytes: &[u8], input_plans: ...)` that deserializes expressions and calls the Rust `with_new_exprs()`.
2. Add a Java method on the sealed interface that serializes `List<Expr>` to proto bytes and passes the child bridges.
3. Return a new `LogicalPlan` variant from the result.

---

## 3. Parameter Handling

**Rust methods:** `with_param_values()`, `get_parameter_names()`, `get_parameter_types()`, `get_parameter_fields()`, `replace_params_with_values()`

**Why missing:** Moderate complexity; parameter types need schema resolution.

**How to add:**
1. Add `DfLogicalPlan::get_parameter_names(write: &mut DiplomatWrite)` to return parameter names as a comma-separated string or `DfStringArray`.
2. Add `DfLogicalPlan::with_param_values(values_bytes: &[u8])` to replace parameters with values (values serialized as proto `ScalarValue` list).
3. Wrap in bridge and public API methods.

---

## 4. Schema Recomputation

**Rust method:** `recompute_schema(self) -> Result<LogicalPlan>`

**Why missing:** Consumes the plan and returns a new one. Requires careful ownership transfer.

**How to add:**
1. Add `DfLogicalPlan::recompute_schema() -> Result<Box<DfLogicalPlan>, Box<DfError>>` in Rust. This clones internally and recomputes.
2. Java: call from bridge, construct new sealed interface variant from result.

---

## 5. Subquery-Aware Traversal

**Rust methods:** `visit_with_subqueries()`, `rewrite_with_subqueries()`, `apply_with_subqueries()`, `apply_subqueries()`, `map_subqueries()`

**Why missing:** Same closure-across-FFI challenge as tree traversal. Additionally, the `Subquery` variant's `outer_ref_columns` and `Spans` fields are not yet extracted.

**How to add:** Same approach as Section 1. For full subquery support, also expose:
- `Subquery.outer_ref_columns` (via `expressions_proto()` or dedicated method)
- `Subquery.spans` (via `DfSpan` struct already in Diplomat)

---

## 6. Invariant Checking

**Rust method:** `check_invariants(&self, check: InvariantLevel) -> Result<()>`

**Why missing:** Requires passing an `InvariantLevel` enum and handling the error result.

**How to add:**
1. Add `DfInvariantLevel` Diplomat enum (`Always`, `Executable`).
2. Add `DfLogicalPlan::check_invariants(level: DfInvariantLevel) -> Result<(), Box<DfError>>`.
3. Java: wrap and throw `DataFusionError` on failure.

---

## 7. Advanced Analysis Methods

**Rust methods:**
- `columnized_output_exprs()` - maps output expressions to columns
- `using_columns()` - extracts JOIN USING columns
- `head_output_expr()` - first output expression
- `all_out_ref_exprs()` - all outer reference expressions

**Why missing:** Return complex types (`Vec<(&Expr, Column)>`, `Vec<HashSet<Column>>`) that require custom serialization.

**How to add:** For each method, define a proto message or use the existing `Expr`/`Column` proto types. Add Rust methods that serialize the result, and Java methods that deserialize.

---

## 8. Variant-Specific Missing Fields

### Statement Sub-Variants
**Rust type:** `Statement` enum with `TransactionStart`, `TransactionEnd`, `SetVariable`, `ResetVariable`, `Prepare`, `Execute`, `Deallocate`.

**How to add:** Model as a sealed sub-interface of `LogicalPlan.Statement`, similar to `Distinct.All`/`Distinct.On`. Add a `DfLogicalPlan::statement_kind()` Diplomat enum and variant-specific accessors.

### DmlStatement Sub-Variants
**Rust type:** `DmlStatement` with fields: `table_name`, `op` (Insert/Update/Delete), `schema`, `input`.

**How to add:**
1. Add `DfDmlOp` Diplomat enum.
2. Add `dml_op()`, `dml_table_name()` methods on `DfLogicalPlan`.
3. Expose in the `Dml` record.

### DdlStatement Sub-Variants
**Rust type:** `DdlStatement` enum with 11 variants (CreateExternalTable, CreateView, DropTable, etc.).

**How to add:** Model as a sealed sub-interface with a `DfDdlKind` Diplomat enum. Each sub-variant exposes its specific fields via dedicated methods.

### Extension: UserDefinedLogicalNode
**Rust field:** `node: Arc<dyn UserDefinedLogicalNode>`

**How to add:** Expose `name()`, `inputs()`, `expressions()`, and `schema()` via Diplomat methods that delegate to the trait methods. Full trait support (custom `fmt_for_explain`, `with_exprs_and_inputs`) would require Diplomat trait callbacks.

### Repartition: Partitioning Scheme
**Rust field:** `partitioning_scheme: Partitioning` (RoundRobinBatch, Hash, DistributeBy)

**How to add:** Add `DfPartitioningKind` Diplomat enum and `repartition_kind()`, `repartition_count()` methods. For Hash partitioning, expose the partition expressions via proto.

### Unnest: Column Mapping
**Rust fields:** `exec_columns`, `list_type_columns`, `struct_type_columns`, `dependency_indices`, `options`

**How to add:** Add methods for each field. Column indices are simple integers (use `DfExprBytes` with raw u32 encoding). `UnnestOptions` can be exposed via individual boolean/config methods.

### Subquery: Outer Ref Columns and Spans
**Rust fields:** `outer_ref_columns: Vec<Expr>`, `spans: Spans`

**How to add:** `outer_ref_columns` via `expressions_proto()`. `Spans` via the existing `DfSpan` struct.

### Copy: Output Configuration
**Rust fields:** `input`, `output_url`, `file_type`, `partition_by`

**How to add:** Add string accessors for `output_url`, `file_type`. `partition_by` via `DfStringArray`.

### Explain: Full Details
**Rust fields:** `explain_format: ExplainFormat`, `stringified_plans: Vec<StringifiedPlan>`, `logical_optimization_succeeded: bool`

**How to add:** Add a `DfExplainFormat` enum, `explain_format()` and `explain_optimization_succeeded()` boolean. Stringified plans could be returned as a formatted string.

### Values: Java-Side Construction
Currently Values can only be created via SQL parsing. To construct them programmatically, add a `LogicalPlanBuilder.values()` method.

### Limit: Skip/Fetch Type Helpers
**Rust methods:** `get_skip_type()`, `get_fetch_type()` - classify skip/fetch as Literal or Expression.

**How to add:** The existing `skipExpr()` and `fetchExpr()` already return the full expression, so callers can check `instanceof Expr.LiteralExpr` in Java. Dedicated helpers are optional.

---

## Implementation Priority

| Priority | Feature | Effort |
|----------|---------|--------|
| High | Java-side tree traversal (Section 1) | Low |
| High | DML/DDL sub-variants (Section 8) | Medium |
| Medium | Parameter handling (Section 3) | Medium |
| Medium | Repartition/Unnest details (Section 8) | Low |
| Medium | Extension node access (Section 8) | Medium |
| Low | `with_new_exprs()` (Section 2) | Medium |
| Low | Subquery-aware traversal (Section 5) | Low (if Java-side) |
| Low | Invariant checking (Section 6) | Low |
| Low | Advanced analysis (Section 7) | High |
