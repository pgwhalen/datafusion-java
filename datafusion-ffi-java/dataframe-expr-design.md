# DataFrame & Expression API Design

## Status

Draft proposal. This document describes the target API for programmatic DataFrame manipulation and expression building in `datafusion-ffi-java`.

## Motivation

Today, `DataFrame` only supports `executeStream()`. Users must write raw SQL strings to do any query manipulation:

```java
DataFrame df = ctx.sql("SELECT a, b FROM t WHERE a > 10 ORDER BY b LIMIT 5");
```

This is limiting. Users cannot compose queries programmatically, conditionally add filters, or build expressions from Java objects. The Rust DataFusion API provides a rich DataFrame builder with `select`, `filter`, `aggregate`, `join`, `sort`, `limit`, and more -- all operating on `Expr` expression trees. We need an equivalent Java API.

## Design Principles

1. **Mirror Rust DataFusion's API** -- Method names, parameter order, and semantics should match the Rust `DataFrame` and `Expr` APIs as closely as possible. A user reading DataFusion's Rust docs should be able to translate to Java mechanically.

2. **Deviate only where Java demands it** -- Java has no operator overloading, no macros, no implicit conversions, and no traits. Where these force a deviation, keep the deviation contained (e.g., in a static utility class) rather than letting it reshape the entire API.

3. **Expressions are the unit of computation** -- Following both DataFusion and Polars, every transformation takes `Expr` objects. The expression builder is the core API; `DataFrame` methods are just containers for expression lists.

4. **Leverage existing infrastructure** -- The existing `Expr` sealed interface, `Operator` enum, `ScalarValue`, and protobuf serialization (`ExprProtoConverter`) already model the full expression AST. The builder layer constructs these same records. No new AST types are needed.

5. **Lazy evaluation** -- All DataFrame transformation methods return a new `DataFrame` without executing. Only terminal operations (`executeStream`, `collect`, `show`, `count`) trigger execution.

## Research Summary

The following APIs were studied. Each influenced specific design choices:

| Library | Key Influence |
|---------|--------------|
| **DataFusion (Rust)** | Primary model. Method names, parameter shapes, and expression functions are taken directly from here. |
| **Spark (Java)** | Validates that `col()`/`lit()` + method chaining works well in Java. Dual `String`/`Expr` overloads are borrowed from Spark. Static `Functions` class pattern is proven at scale. |
| **jOOQ** | Proves that named comparison methods (`.eq()`, `.gt()`) read naturally in Java. Short+long method name pairs (`.eq()`/`.equalTo()`) are a good pattern. |
| **Polars** | Validates expression-centric design. `with_columns` as a distinct operation from `select`. `when/then/otherwise` conditional expression pattern. |
| **Tablesaw** | Shows that Java-native column-type-specific methods are verbose but clear. Confirms that `summarize().by()` is less intuitive than `groupBy().agg()`. |
| **QueryDSL** | `BooleanBuilder` for dynamic predicate construction. Typed expression subtypes are elegant but require schema knowledge at column-reference time (deferred to future work). |
| **Pandas** | Negative example: multiple overlapping ways to do the same thing creates confusion. We will have exactly one way to do each operation. |

## API Overview

### Layer 1: Expression Builders (static factory methods)

```java
import static org.apache.arrow.datafusion.Functions.*;

// Column references and literals
col("age")                        // Expr referencing column "age"
lit(42)                           // Expr wrapping literal int
lit("hello")                      // Expr wrapping literal string
lit(3.14)                         // Expr wrapping literal double

// Comparison (methods on Expr)
col("age").gt(lit(18))            // age > 18
col("name").eq(lit("Alice"))      // name = 'Alice'

// Boolean logic
col("a").gt(lit(0)).and(col("b").lt(lit(10)))

// Arithmetic
col("price").multiply(col("quantity")).alias("total")

// Aggregates
avg(col("salary"))
count(col("id"))
sum(col("amount")).alias("total_amount")

// Sorting
col("age").sortAsc()              // SortExpr(age, asc=true, nullsFirst=false)
col("age").sortDesc()             // SortExpr(age, asc=false, nullsFirst=true)
```

### Layer 2: DataFrame transformations (method chaining)

```java
DataFrame result = ctx.sql("SELECT * FROM employees")
    .filter(col("age").gt(lit(30)))
    .select(col("name"), col("department"), col("salary"))
    .aggregate(
        List.of(col("department")),
        List.of(avg(col("salary")).alias("avg_salary"), count(col("name")).alias("headcount"))
    )
    .sort(col("avg_salary").sortDesc())
    .limit(0, 10);
```

### Layer 3: Terminal operations

```java
RecordBatchStream stream = result.executeStream(allocator);  // existing -- lazy streaming
RecordBatchStream buffered = result.collect(allocator);       // new -- all results materialized on return
result.show();                                                // new
long rowCount = result.count();                               // new
```

---

## Detailed Design

### 1. Expression Building: `Functions` Static Utility Class

A single static utility class provides all expression factory methods. With `import static org.apache.arrow.datafusion.Functions.*`, expression code reads like SQL.

**Why a static class?** This matches DataFusion's `prelude` module, Spark's `functions` class, and jOOQ's `DSL` class. It's the proven pattern for Java expression DSLs.

**Why named `Functions`?** It parallels Rust's `datafusion::functions` module family. `Expressions` was considered but is verbose and less intuitive for aggregate/window functions. `Dsl` was considered but feels jOOQ-specific.

```java
package org.apache.arrow.datafusion;

public final class Functions {
    private Functions() {}

    // ── Column references ──

    /** Reference a column by name. Equivalent to Rust's col(). */
    public static Expr col(String name) {
        return new Expr.ColumnExpr(new Column(name, null, null));
    }

    /** Reference a column with table qualification. */
    public static Expr col(String relation, String name) {
        return new Expr.ColumnExpr(
            new Column(name, new TableReference.Bare(relation), null));
    }

    // ── Literals ──

    /** Wrap a Java value as a literal expression. */
    public static Expr lit(int value) {
        return new Expr.LiteralExpr(new ScalarValue.Int32(value));
    }

    public static Expr lit(long value) {
        return new Expr.LiteralExpr(new ScalarValue.Int64(value));
    }

    public static Expr lit(float value) {
        return new Expr.LiteralExpr(new ScalarValue.Float32(value));
    }

    public static Expr lit(double value) {
        return new Expr.LiteralExpr(new ScalarValue.Float64(value));
    }

    public static Expr lit(String value) {
        return new Expr.LiteralExpr(new ScalarValue.Utf8(value));
    }

    public static Expr lit(boolean value) {
        return new Expr.LiteralExpr(new ScalarValue.BooleanValue(value));
    }

    /** Null literal. */
    public static Expr litNull() {
        return new Expr.LiteralExpr(new ScalarValue.Null());
    }

    // ── Aggregate functions ──

    public static Expr avg(Expr expr)   { return aggFn("avg", expr); }
    public static Expr sum(Expr expr)   { return aggFn("sum", expr); }
    public static Expr count(Expr expr) { return aggFn("count", expr); }
    public static Expr min(Expr expr)   { return aggFn("min", expr); }
    public static Expr max(Expr expr)   { return aggFn("max", expr); }
    public static Expr median(Expr expr) { return aggFn("median", expr); }
    public static Expr stddev(Expr expr) { return aggFn("stddev", expr); }
    public static Expr variance(Expr expr) { return aggFn("variance", expr); }

    public static Expr countDistinct(Expr expr) {
        return new Expr.AggregateFunctionExpr(
            "count", List.of(expr), true, null, List.of(), null);
    }

    public static Expr countAll() {
        return aggFn("count", new Expr.LiteralExpr(new ScalarValue.Int64(1)));
    }

    // ── Conditional expressions ──

    /** Start a CASE WHEN chain. Equivalent to Rust's when(). */
    public static CaseBuilder when(Expr condition, Expr then) {
        return new CaseBuilder().when(condition, then);
    }

    // ── Subqueries ──

    public static Expr scalarSubquery(LogicalPlan subquery) { ... }
    public static Expr exists(LogicalPlan subquery) { ... }
    public static Expr notExists(LogicalPlan subquery) { ... }
    public static Expr inSubquery(Expr expr, LogicalPlan subquery) { ... }

    // ── Utility ──

    public static Expr cast(Expr expr, ArrowType dataType) {
        return new Expr.CastExpr(expr, dataType);
    }

    public static Expr tryCast(Expr expr, ArrowType dataType) {
        return new Expr.TryCastExpr(expr, dataType);
    }

    public static Expr not(Expr expr) {
        return new Expr.NotExpr(expr);
    }

    // ── Private helpers ──

    private static Expr aggFn(String name, Expr expr) {
        return new Expr.AggregateFunctionExpr(
            name, List.of(expr), false, null, List.of(), null);
    }
}
```

### 2. Fluent Methods on `Expr`

The existing `Expr` sealed interface gains default methods for expression composition. These methods construct new `Expr` record instances -- they are pure Java, no FFI involved.

**Design choice: default methods on the sealed interface vs. a wrapper class.**

A wrapper class (like Spark's `Column`) would require wrapping and unwrapping at every API boundary. Since `Expr` is already the currency type throughout the codebase (proto serialization, AST inspection), adding methods directly to it is simpler and avoids a parallel type hierarchy. Sealed interfaces in Java support default methods, so this works cleanly.

```java
public sealed interface Expr {

    // ── Comparison ──

    /** Equality: this = other */
    default Expr eq(Expr other) {
        return new BinaryExpr(this, Operator.Eq, other);
    }

    /** Not equal: this != other */
    default Expr notEq(Expr other) {
        return new BinaryExpr(this, Operator.NotEq, other);
    }

    /** Less than: this < other */
    default Expr lt(Expr other) {
        return new BinaryExpr(this, Operator.Lt, other);
    }

    /** Less than or equal: this <= other */
    default Expr ltEq(Expr other) {
        return new BinaryExpr(this, Operator.LtEq, other);
    }

    /** Greater than: this > other */
    default Expr gt(Expr other) {
        return new BinaryExpr(this, Operator.Gt, other);
    }

    /** Greater than or equal: this >= other */
    default Expr gtEq(Expr other) {
        return new BinaryExpr(this, Operator.GtEq, other);
    }

    // ── Logical ──

    /** Logical AND: this AND other */
    default Expr and(Expr other) {
        return new BinaryExpr(this, Operator.And, other);
    }

    /** Logical OR: this OR other */
    default Expr or(Expr other) {
        return new BinaryExpr(this, Operator.Or, other);
    }

    /** Logical NOT: NOT this */
    default Expr not() {
        return new NotExpr(this);
    }

    // ── Arithmetic ──

    /** Addition: this + other */
    default Expr plus(Expr other) {
        return new BinaryExpr(this, Operator.Plus, other);
    }

    /** Subtraction: this - other */
    default Expr minus(Expr other) {
        return new BinaryExpr(this, Operator.Minus, other);
    }

    /** Multiplication: this * other */
    default Expr multiply(Expr other) {
        return new BinaryExpr(this, Operator.Multiply, other);
    }

    /** Division: this / other */
    default Expr divide(Expr other) {
        return new BinaryExpr(this, Operator.Divide, other);
    }

    /** Modulo: this % other */
    default Expr modulo(Expr other) {
        return new BinaryExpr(this, Operator.Modulo, other);
    }

    /** Unary negation: -this */
    default Expr negate() {
        return new NegativeExpr(this);
    }

    // ── Pattern matching ──

    /** SQL LIKE pattern match */
    default Expr like(Expr pattern) {
        return new LikeExpr(false, this, pattern, null, false);
    }

    /** SQL NOT LIKE pattern match */
    default Expr notLike(Expr pattern) {
        return new LikeExpr(true, this, pattern, null, false);
    }

    /** Case-insensitive LIKE (ILIKE) */
    default Expr ilike(Expr pattern) {
        return new LikeExpr(false, this, pattern, null, true);
    }

    // ── NULL checks ──

    default Expr isNull()    { return new IsNullExpr(this); }
    default Expr isNotNull() { return new IsNotNullExpr(this); }
    default Expr isTrue()    { return new IsTrueExpr(this); }
    default Expr isFalse()   { return new IsFalseExpr(this); }

    // ── IN list ──

    /** IN (value1, value2, ...) */
    default Expr inList(List<Expr> values) {
        return new InListExpr(this, values, false);
    }

    /** NOT IN (value1, value2, ...) */
    default Expr notInList(List<Expr> values) {
        return new InListExpr(this, values, true);
    }

    // ── BETWEEN ──

    default Expr between(Expr low, Expr high) {
        return new BetweenExpr(this, false, low, high);
    }

    default Expr notBetween(Expr low, Expr high) {
        return new BetweenExpr(this, true, low, high);
    }

    // ── Aliasing ──

    /** Alias this expression: expr AS name */
    default Expr alias(String name) {
        return new AliasExpr(this, name, List.of());
    }

    // ── Sorting ──

    /** Sort ascending, nulls last (default) */
    default SortExpr sortAsc() {
        return new SortExpr(this, true, false);
    }

    /** Sort descending, nulls first (default) */
    default SortExpr sortDesc() {
        return new SortExpr(this, false, true);
    }

    /** Sort with explicit control */
    default SortExpr sort(boolean asc, boolean nullsFirst) {
        return new SortExpr(this, asc, nullsFirst);
    }

    // ... existing record declarations unchanged ...
}
```

**Method naming rationale:**

| Rust method | Java method | Notes |
|------------|------------|-------|
| `.eq()` | `.eq()` | Same. Short form from jOOQ. |
| `.not_eq()` | `.notEq()` | camelCase conversion. |
| `.lt()` / `.gt()` | `.lt()` / `.gt()` | Same. Universally understood. |
| `.lt_eq()` / `.gt_eq()` | `.ltEq()` / `.gtEq()` | camelCase conversion. |
| `.and()` / `.or()` | `.and()` / `.or()` | Same. |
| `+ - * / %` (operators) | `.plus()` `.minus()` `.multiply()` `.divide()` `.modulo()` | Named methods. Matches Spark's `Column`. |
| `.alias()` | `.alias()` | Same. |
| `.sort(asc, nulls_first)` | `.sort(asc, nullsFirst)` | Same, plus `.sortAsc()` / `.sortDesc()` convenience. |

### 3. `CaseBuilder` for CASE/WHEN Expressions

Rust DataFusion provides `when(condition, then)` which returns a `CaseBuilder` with `.when()` and `.otherwise()` methods. We mirror this exactly:

```java
public final class CaseBuilder {
    private final List<WhenThen> branches = new ArrayList<>();

    public CaseBuilder when(Expr condition, Expr then) {
        branches.add(new WhenThen(condition, then));
        return this;
    }

    /** Terminal: build the CASE expression with an ELSE clause. */
    public Expr otherwise(Expr elseExpr) {
        return new Expr.CaseExpr(null, List.copyOf(branches), elseExpr);
    }

    /** Terminal: build the CASE expression without an ELSE clause (result is NULL). */
    public Expr end() {
        return new Expr.CaseExpr(null, List.copyOf(branches), null);
    }
}
```

Usage:
```java
when(col("status").eq(lit("active")), lit(1))
    .when(col("status").eq(lit("pending")), lit(0))
    .otherwise(lit(-1))
```

### 4. `DataFrame` Transformation Methods

Each method returns a new `DataFrame` wrapping a new native pointer. The FFI layer serializes expression lists to protobuf, passes them to Rust, which deserializes them and builds the logical plan.

```java
public class DataFrame implements AutoCloseable {
    private final DataFrameFfi ffi;

    // ── Existing ──

    public RecordBatchStream executeStream(BufferAllocator allocator) { ... }

    // ── Projection ──

    /** Select expressions. Equivalent to Rust's df.select(vec![...]). */
    public DataFrame select(Expr... exprs) {
        return new DataFrame(ffi.select(List.of(exprs)));
    }

    /** Select expressions from a list. */
    public DataFrame select(List<Expr> exprs) {
        return new DataFrame(ffi.select(exprs));
    }

    /** Select columns by name. Equivalent to Rust's df.select_columns(&[...]). */
    public DataFrame selectColumns(String... columns) {
        List<Expr> exprs = Arrays.stream(columns)
            .map(Functions::col)
            .collect(Collectors.toList());
        return select(exprs);
    }

    /** Add or replace a column. Equivalent to Rust's df.with_column(name, expr). */
    public DataFrame withColumn(String name, Expr expr) {
        return new DataFrame(ffi.withColumn(name, expr));
    }

    /** Rename a column. */
    public DataFrame withColumnRenamed(String oldName, String newName) {
        return new DataFrame(ffi.withColumnRenamed(oldName, newName));
    }

    /** Drop columns by name. */
    public DataFrame dropColumns(String... columns) {
        return new DataFrame(ffi.dropColumns(List.of(columns)));
    }

    // ── Filtering ──

    /** Filter rows matching a predicate. Equivalent to Rust's df.filter(expr). */
    public DataFrame filter(Expr predicate) {
        return new DataFrame(ffi.filter(predicate));
    }

    // ── Sorting ──

    /** Sort by expressions with explicit sort parameters. */
    public DataFrame sort(SortExpr... sortExprs) {
        return new DataFrame(ffi.sort(List.of(sortExprs)));
    }

    /** Sort by expressions with explicit sort parameters. */
    public DataFrame sort(List<SortExpr> sortExprs) {
        return new DataFrame(ffi.sort(sortExprs));
    }

    // ── Limiting ──

    /**
     * Limit the number of rows returned.
     *
     * @param skip number of rows to skip
     * @param fetch maximum number of rows to return, or null for no limit
     */
    public DataFrame limit(int skip, Integer fetch) {
        return new DataFrame(ffi.limit(skip, fetch));
    }

    // ── Aggregation ──

    /**
     * Aggregate with grouping.
     *
     * @param groupExprs GROUP BY expressions (empty list for global aggregation)
     * @param aggrExprs  aggregate expressions (e.g., avg(col("salary")))
     */
    public DataFrame aggregate(List<Expr> groupExprs, List<Expr> aggrExprs) {
        return new DataFrame(ffi.aggregate(groupExprs, aggrExprs));
    }

    // ── Joins ──

    /**
     * Join with another DataFrame on column names.
     *
     * @param right     the right DataFrame
     * @param joinType  the join type
     * @param leftCols  left join column names
     * @param rightCols right join column names
     */
    public DataFrame join(
            DataFrame right, JoinType joinType,
            List<String> leftCols, List<String> rightCols) {
        return new DataFrame(ffi.join(right.ffi, joinType, leftCols, rightCols, null));
    }

    /**
     * Join with another DataFrame on column names with an additional filter.
     *
     * @param right     the right DataFrame
     * @param joinType  the join type
     * @param leftCols  left join column names
     * @param rightCols right join column names
     * @param filter    additional join filter expression
     */
    public DataFrame join(
            DataFrame right, JoinType joinType,
            List<String> leftCols, List<String> rightCols, Expr filter) {
        return new DataFrame(ffi.join(right.ffi, joinType, leftCols, rightCols, filter));
    }

    /**
     * Join with arbitrary expressions.
     *
     * @param right    the right DataFrame
     * @param joinType the join type
     * @param onExprs  join condition expressions
     */
    public DataFrame joinOn(
            DataFrame right, JoinType joinType, List<Expr> onExprs) {
        return new DataFrame(ffi.joinOn(right.ffi, joinType, onExprs));
    }

    // ── Set operations ──

    public DataFrame union(DataFrame other) {
        return new DataFrame(ffi.union(other.ffi));
    }

    public DataFrame unionDistinct(DataFrame other) {
        return new DataFrame(ffi.unionDistinct(other.ffi));
    }

    public DataFrame intersect(DataFrame other) {
        return new DataFrame(ffi.intersect(other.ffi));
    }

    public DataFrame except(DataFrame other) {
        return new DataFrame(ffi.except(other.ffi));
    }

    // ── Distinct ──

    public DataFrame distinct() {
        return new DataFrame(ffi.distinct());
    }

    // ── Terminal operations ──

    /** Execute and stream results. */
    public RecordBatchStream executeStream(BufferAllocator allocator) { ... }

    /**
     * Execute the query, buffer all results, and return a RecordBatchStream.
     *
     * <p>Unlike {@link #executeStream}, all computation is complete when this method
     * returns. The returned stream lets the caller iterate batches via
     * {@code loadNextBatch()} / {@code getVectorSchemaRoot()} as usual, but the
     * data is already fully materialized in memory.
     */
    public RecordBatchStream collect(BufferAllocator allocator) { ... }

    /** Execute and print results to stdout. */
    public void show() { ... }

    /** Execute and return the row count. */
    public long count() { ... }

    /** Get the schema of this DataFrame. */
    public Schema schema() { ... }

    // ── Plan access ──

    /** Get the underlying logical plan. Useful for subqueries. */
    public LogicalPlan logicalPlan() { ... }

    @Override
    public void close() { ... }
}
```

### 5. `JoinType` Enum

```java
public enum JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    LEFT_SEMI,
    LEFT_ANTI,
    RIGHT_SEMI,
    RIGHT_ANTI
}
```

Matches Rust's `JoinType` variants. Uses an enum (not strings as in Spark) for type safety.

### 6. `SessionContext` Additions

```java
public class SessionContext implements AutoCloseable {

    // ── Existing ──
    public DataFrame sql(String query) { ... }

    // ── New: table reference ──

    /**
     * Create a DataFrame for a registered table.
     * Equivalent to Rust's ctx.table("name").
     */
    public DataFrame table(String name) {
        return new DataFrame(ffi.table(name));
    }

    // ── New: file readers ──

    /** Read a Parquet file/directory into a DataFrame. */
    public DataFrame readParquet(String path) {
        return new DataFrame(ffi.readParquet(path));
    }

    /** Read a CSV file/directory into a DataFrame. */
    public DataFrame readCsv(String path) {
        return new DataFrame(ffi.readCsv(path));
    }

    /** Read a JSON file/directory into a DataFrame. */
    public DataFrame readJson(String path) {
        return new DataFrame(ffi.readJson(path));
    }
}
```

---

## FFI Strategy

### Expression Serialization

Expressions cross the FFI boundary via protobuf serialization. This reuses the existing `ExprProtoConverter` infrastructure:

```
Java: List<Expr>  →  ExprProtoConverter.toProtoBytes()  →  byte[]
                         ↓ (FFI: pass byte[] to Rust)
Rust: byte[]  →  datafusion-proto deserialization  →  Vec<datafusion::Expr>
```

**Why protobuf?** The round-trip serialization is already implemented and tested (40+ tests in `ExprTest.java`). It handles all expression variants including nested expressions, aggregates, window functions, and case expressions. Adding a second serialization format would be redundant.

**Performance note:** Protobuf serialization adds overhead per DataFrame operation. For simple expressions this is negligible. If profiling shows this is a bottleneck for tight loops, a direct FFI path (constructing `Expr` enum discriminants in native memory) could be added later as an optimization. The public API would not change.

### DataFrame Operations

Each DataFrame transformation calls a Rust FFI function that:
1. Accepts the current DataFrame pointer
2. Accepts serialized expression bytes
3. Calls the corresponding Rust DataFrame method
4. Returns a new DataFrame pointer (the old one remains valid)

Example Rust FFI function for `filter`:

```rust
#[no_mangle]
pub unsafe extern "C" fn datafusion_dataframe_filter(
    rt: *mut c_void,
    df: *mut c_void,
    predicate_bytes: *const u8,
    predicate_len: usize,
    df_out: *mut *mut c_void,
    error_out: *mut *mut c_char,
) -> i32 {
    clear_error(error_out);
    // Deserialize predicate from proto bytes
    let predicate = deserialize_expr(predicate_bytes, predicate_len)?;
    // Call DataFrame::filter
    let new_df = runtime.block_on(df.filter(predicate))?;
    *df_out = Box::into_raw(Box::new(new_df)) as *mut c_void;
    0
}
```

---

## Complete Examples

### Example 1: Filter and Select (mirrors Rust example)

**Rust DataFusion:**
```rust
parquet_df
    .select_columns(&["car", "speed", "time"])?
    .filter(col("speed").gt(lit(1)))?
    .show()
    .await?;
```

**Java (this design):**
```java
import static org.apache.arrow.datafusion.Functions.*;

ctx.readParquet("/path/to/data")
    .selectColumns("car", "speed", "time")
    .filter(col("speed").gt(lit(1)))
    .show();
```

### Example 2: In-Memory Data + Filter

**Rust DataFusion:**
```rust
ctx.register_batch("t", batch)?;
let df = ctx.table("t").await?;
let filter = col("b").eq(lit(10));
let df = df.select_columns(&["a", "b"])?.filter(filter)?;
df.show().await?;
```

**Java:**
```java
ctx.registerTable("t", root, allocator);
DataFrame df = ctx.table("t")
    .selectColumns("a", "b")
    .filter(col("b").eq(lit(10)));
df.show();
```

### Example 3: Aggregation

**Rust DataFusion:**
```rust
df.aggregate(
    vec![col("department")],
    vec![avg(col("salary")), count(col("id"))]
)?
```

**Java:**
```java
df.aggregate(
    List.of(col("department")),
    List.of(avg(col("salary")), count(col("id")))
)
```

### Example 4: Join

**Rust DataFusion:**
```rust
left.join(right, JoinType::Inner, &["id"], &["id"], None)?
```

**Java:**
```java
left.join(right, JoinType.INNER,
    List.of("id"), List.of("id"))
```

### Example 5: Complex Query with Sort and Limit

**Rust DataFusion:**
```rust
df.filter(col("age").gt(lit(30)))?
    .select(vec![col("name"), col("salary"), col("dept")])?
    .aggregate(
        vec![col("dept")],
        vec![avg(col("salary")).alias("avg_salary"), count(col("name")).alias("n")]
    )?
    .sort(vec![col("avg_salary").sort(false, true)])?
    .limit(0, Some(10))?
    .show().await?;
```

**Java:**
```java
df.filter(col("age").gt(lit(30)))
    .select(col("name"), col("salary"), col("dept"))
    .aggregate(
        List.of(col("dept")),
        List.of(avg(col("salary")).alias("avg_salary"), count(col("name")).alias("n"))
    )
    .sort(col("avg_salary").sortDesc())
    .limit(0, 10)
    .show();
```

### Example 6: CASE/WHEN Expression

**Rust DataFusion:**
```rust
when(col("status").eq(lit("active")), lit(1))
    .when(col("status").eq(lit("pending")), lit(0))
    .otherwise(lit(-1))?
```

**Java:**
```java
when(col("status").eq(lit("active")), lit(1))
    .when(col("status").eq(lit("pending")), lit(0))
    .otherwise(lit(-1))
```

### Example 7: Scalar Subquery

**Rust DataFusion:**
```rust
ctx.table("t1").await?
    .filter(
        scalar_subquery(Arc::new(
            ctx.table("t2").await?
                .filter(out_ref_col(DataType::Utf8, "t1.car").eq(col("t2.car")))?
                .aggregate(vec![], vec![avg(col("t2.speed"))])?
                .select(vec![avg(col("t2.speed"))])?
                .into_unoptimized_plan(),
        ))
        .gt(lit(0.0)),
    )?
```

**Java:**
```java
DataFrame t2Subquery = ctx.table("t2")
    .filter(col("t1", "car").eq(col("t2", "car")))
    .aggregate(List.of(), List.of(avg(col("t2", "speed"))))
    .select(avg(col("t2", "speed")));

ctx.table("t1")
    .filter(scalarSubquery(t2Subquery.logicalPlan()).gt(lit(0.0)))
```

### Example 8: with_column and Conditional

```java
df.withColumn("salary_band",
    when(col("salary").gt(lit(100000)), lit("high"))
        .when(col("salary").gt(lit(50000)),  lit("mid"))
        .otherwise(lit("low")))
```

### Example 9: Multiple Aggregates with Expressions

```java
df.aggregate(
    List.of(col("department"), col("region")),
    List.of(
        sum(col("revenue")).alias("total_revenue"),
        avg(col("price").multiply(col("quantity"))).alias("avg_order_value"),
        countDistinct(col("customer_id")).alias("unique_customers"),
        max(col("order_date")).alias("latest_order")
    )
)
```

---

## Deviations from Rust API

| Aspect | Rust | Java | Reason |
|--------|------|------|--------|
| Arithmetic operators | `expr1 + expr2` | `expr1.plus(expr2)` | Java has no operator overloading. Named methods follow Spark/jOOQ precedent. |
| Varargs vs Vec | `vec![col("a"), col("b")]` | `col("a"), col("b")` or `List.of(...)` | Java varargs for convenience; `List` overloads for programmatic use. `List<String>` preferred over `String[]` to avoid verbose `new String[]{...}` at call sites. |
| `select_columns` naming | `select_columns(&[...])` | `selectColumns(...)` | camelCase convention. |
| Sort expression | `col("x").sort(false, true)` | `col("x").sortDesc()` or `col("x").sort(false, true)` | Added convenience methods; raw `sort(asc, nullsFirst)` also available. |
| `show()` is async | `df.show().await?` | `df.show()` | Java FFI boundary is synchronous (Rust Tokio runtime is internal). |
| Result types | `Result<DataFrame>` | `DataFrame` (throws) | Java uses exceptions, not Result. `DataFusionException` on error. |
| `lit()` overloads | `lit(value)` with Into trait | `lit(int)`, `lit(long)`, `lit(String)`, ... | Java has no trait-based generic dispatch. Method overloads cover common types. |
| `Option<usize>` for limit | `limit(0, Some(3))` | `limit(0, 3)` with `Integer` type | Java `Integer` nullable serves as `Option`. |
| Arc/ownership | `Arc::new(plan)` | Direct reference | Java has GC; no ownership concerns. |
| `Expr` construction | Enum variants constructed directly | Same, via sealed interface records | Direct 1:1 mapping. |

---

## What Is NOT in Scope

The following features exist in Rust DataFusion but are deferred to future work:

1. **Window functions** -- `df.window(vec![...])` and window expression builders (`.over()`, `.partition_by()`, `.order_by()`). The `Expr.WindowFunctionExpr` record already exists in the AST; what's missing is the builder ergonomics.

2. **Typed expression subtypes** -- QueryDSL-style `StringExpr`, `NumericExpr` with type-specific methods. Requires schema knowledge at expression creation time. Can be layered on later without breaking changes.

3. **`with_columns`** -- Polars-style "add columns while preserving all originals." DataFusion Rust exposes `with_column` (singular). Multi-column `with_columns` could be a Java-side convenience that calls `with_column` repeatedly.

4. **Write operations** -- `write_parquet`, `write_csv`, `write_json`, `write_table`. Important but orthogonal to the query API.

5. **`describe()`** -- Statistical summary. Useful but not core to expression building.

6. **Dynamic predicate builder** -- QueryDSL-style `BooleanBuilder` for conditionally accumulating predicates. Easy to add later as a utility.

7. **SQL expression parsing on Expr** -- `df.parse_sql_expr(sql)` already exists on `SessionContext`; no additional work needed.

8. **User-defined functions via expressions** -- Creating UDFs/UDAFs from expressions. The existing `ScalarUdf` registration API covers this use case differently.

---

## Implementation Order

Phase 1: Core expression building
- Add default methods to `Expr` (comparison, logical, arithmetic, alias, sort)
- Add `Functions` static utility class (`col`, `lit`, aggregate functions, `when`)
- Add `CaseBuilder`
- Add `JoinType` enum
- Pure Java, no FFI changes needed

Phase 2: DataFrame transformations
- Add Rust FFI functions: `filter`, `select`, `aggregate`, `sort`, `limit`
- Add Java FFI bridge methods in `DataFrameFfi`
- Add public API methods on `DataFrame`
- Add `show()`, `collect()`, `count()`, `schema()` terminal operations

Phase 3: Joins and set operations
- Add Rust FFI functions: `join`, `join_on`, `union`, `intersect`, `except`, `distinct`
- Add Java API methods

Phase 4: SessionContext readers
- Add `readParquet`, `readCsv`, `readJson`, `table` methods
- Add corresponding Rust FFI functions

Phase 5: Advanced features
- `withColumn`, `withColumnRenamed`, `dropColumns`
- `logicalPlan()` for subqueries
- Window function builders
