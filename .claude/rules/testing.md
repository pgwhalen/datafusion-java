# Testing

## Test Coverage Requirements

Every public Java method in `datafusion-ffi-java` **MUST** have test coverage. This ensures that all FFI calls are properly exercised and that the Java-Rust boundary works correctly.

## Data Verification

JUnit tests for `datafusion-ffi-java` must **always verify the actual data** in a DataFusion result (DataFrame, record batch, VectorSchemaRoot, etc.), not just the row count. Row count assertions alone are insufficient because they can pass even when data is corrupted, columns are missing, or values are wrong.

### Bad: Row count only

```java
// DO NOT do this
try (RecordBatchStream stream = df.execute(allocator)) {
    VectorSchemaRoot root = stream.next();
    assertEquals(3, root.getRowCount());
}
```

### Good: Verify actual data

```java
// DO this - verify column values
try (RecordBatchStream stream = df.execute(allocator)) {
    VectorSchemaRoot root = stream.next();
    assertEquals(3, root.getRowCount());
    IntVector col = (IntVector) root.getVector("id");
    assertEquals(1, col.get(0));
    assertEquals(2, col.get(1));
    assertEquals(3, col.get(2));
}
```

Row count checks are fine as an **additional** assertion alongside data verification, but never as the sole assertion.
