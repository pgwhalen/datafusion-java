package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class DiplomatBridgeTest {

  @Test
  void testCreateAndDestroyContext() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      assertNotNull(ctx);
    }
  }

  @Test
  void testSessionId() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      String id = ctx.sessionId();
      assertNotNull(id);
      assertFalse(id.isEmpty());
    }
  }

  @Test
  void testSessionStartTimeMillis() {
    long before = System.currentTimeMillis();
    try (DfSessionContext ctx = new DfSessionContext()) {
      long startTime = ctx.sessionStartTimeMillis();
      long after = System.currentTimeMillis();
      assertTrue(startTime >= before - 1000);
      assertTrue(startTime <= after + 1000);
    }
  }

  @Test
  void testSqlSuccess() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      try (DfDataFrame df = ctx.sql("SELECT 1 + 1 AS result")) {
        assertNotNull(df);
      }
    }
  }

  @Test
  void testSqlError() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      DfError error =
          assertThrows(DfError.class, () -> ctx.sql("SELECT * FROM nonexistent_table_xyz"));
      String msg = error.toDisplay();
      assertTrue(msg.contains("nonexistent_table_xyz"), "Error: " + msg);
      error.close();
    }
  }

  @Test
  void testSessionIdStableAcrossOperations() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      String id1 = ctx.sessionId();
      try (DfDataFrame df = ctx.sql("SELECT 42 AS answer")) {
        assertNotNull(df);
      }
      String id2 = ctx.sessionId();
      assertEquals(id1, id2);
    }
  }

  @Test
  void testCollectToString() {
    try (DfSessionContext ctx = new DfSessionContext()) {
      try (DfDataFrame df = ctx.sql("SELECT 1 AS a, 2 AS b")) {
        String output = df.collectToString();
        assertTrue(output.contains("a"), "Output should contain column 'a': " + output);
        assertTrue(output.contains("b"), "Output should contain column 'b': " + output);
        assertTrue(output.contains("1"), "Output should contain value 1: " + output);
        assertTrue(output.contains("2"), "Output should contain value 2: " + output);
      }
    }
  }

  @Test
  void testRegisterTableAndQuery() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      VectorSchemaRoot root = createTestData(allocator);
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);
        try (DfArrowBatch batch =
            DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
          ctx.registerTable("test", batch);
        }
      }

      try (DfDataFrame df = ctx.sql("SELECT x, y FROM test WHERE x > 1")) {
        String output = df.collectToString();
        // Should contain x=2,y=20 and x=3,y=30 but not x=1,y=10
        assertTrue(output.contains("2"), "Output should contain x=2: " + output);
        assertTrue(output.contains("3"), "Output should contain x=3: " + output);
        assertTrue(output.contains("20"), "Output should contain y=20: " + output);
        assertTrue(output.contains("30"), "Output should contain y=30: " + output);
      }

      root.close();
    }
  }

  @Test
  void testRegisterTableAggregate() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      VectorSchemaRoot root = createTestData(allocator);
      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);
        try (DfArrowBatch batch =
            DfArrowBatch.fromAddresses(ffiSchema.memoryAddress(), ffiArray.memoryAddress())) {
          ctx.registerTable("test", batch);
        }
      }

      try (DfDataFrame df = ctx.sql("SELECT SUM(y) AS total FROM test")) {
        String output = df.collectToString();
        // 10 + 20 + 30 = 60
        assertTrue(output.contains("60"), "Output should contain sum 60: " + output);
      }

      root.close();
    }
  }

  @Test
  void testParseSqlExpr() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema =
          new Schema(
              List.of(
                  Field.nullable("a", new ArrowType.Int(32, true)),
                  Field.nullable("b", new ArrowType.Int(32, true))));

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);
        try (DfArrowSchema dfSchema = DfArrowSchema.fromAddress(ffiSchema.memoryAddress());
            DfExprBytes exprBytes = ctx.parseSqlExpr("a + b", dfSchema)) {

          long len = exprBytes.len();
          assertTrue(len > 0, "Expression bytes should not be empty");

          try (Arena arena = Arena.ofConfined()) {
            MemorySegment dest = arena.allocate(len);
            exprBytes.copyTo(dest.address(), len);
            byte[] bytes = dest.toArray(ValueLayout.JAVA_BYTE);

            List<Expr> exprs = ExprProtoConverter.fromProtoBytes(bytes);
            assertEquals(1, exprs.size());
            assertInstanceOf(Expr.BinaryExpr.class, exprs.get(0));
            Expr.BinaryExpr bin = (Expr.BinaryExpr) exprs.get(0);
            assertEquals(Operator.Plus, bin.op());
            assertInstanceOf(Expr.ColumnExpr.class, bin.left());
            assertInstanceOf(Expr.ColumnExpr.class, bin.right());
            assertEquals("a", ((Expr.ColumnExpr) bin.left()).column().name());
            assertEquals("b", ((Expr.ColumnExpr) bin.right()).column().name());
          }
        }
      }
    }
  }

  @Test
  void testParseSqlExprError() {
    try (BufferAllocator allocator = new RootAllocator();
        DfSessionContext ctx = new DfSessionContext()) {

      Schema schema = new Schema(List.of());

      try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, schema, null, ffiSchema);
        try (DfArrowSchema dfSchema = DfArrowSchema.fromAddress(ffiSchema.memoryAddress())) {
          DfError error =
              assertThrows(DfError.class, () -> ctx.parseSqlExpr("+++invalid", dfSchema));
          String msg = error.toDisplay();
          assertTrue(msg.length() > 0, "Error message should not be empty");
          error.close();
        }
      }
    }
  }

  private VectorSchemaRoot createTestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("y", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    BigIntVector xVector = (BigIntVector) root.getVector("x");
    BigIntVector yVector = (BigIntVector) root.getVector("y");

    xVector.allocateNew(3);
    yVector.allocateNew(3);

    xVector.set(0, 1);
    xVector.set(1, 2);
    xVector.set(2, 3);

    yVector.set(0, 10);
    yVector.set(1, 20);
    yVector.set(2, 30);

    xVector.setValueCount(3);
    yVector.setValueCount(3);
    root.setRowCount(3);

    return root;
  }
}
