package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for scalar UDF registration and execution. */
class ScalarUdfTest {

  @Test
  void testPowUdf() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data: a=[2.1, 3.1, 4.1, 5.1], b=[1.0, 2.0, 3.0, 4.0]
      VectorSchemaRoot testData = createFloat64TestData(allocator);
      ctx.registerTable("t", testData, allocator);

      // Create pow UDF using the full ScalarUdf interface
      ScalarUdf pow =
          new ScalarUdf() {
            @Override
            public String name() {
              return "pow";
            }

            @Override
            public Volatility volatility() {
              return Volatility.IMMUTABLE;
            }

            @Override
            public Field returnField(List<Field> argFields) {
              return Field.nullable(
                  "pow", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            }

            @Override
            public FieldVector invoke(
                List<FieldVector> args,
                List<Field> argFields,
                int numRows,
                Field returnField,
                BufferAllocator alloc) {
              Float8Vector base = (Float8Vector) args.get(0);
              Float8Vector exp = (Float8Vector) args.get(1);
              Float8Vector result = new Float8Vector("pow", alloc);
              result.allocateNew(numRows);
              for (int i = 0; i < numRows; i++) {
                result.set(i, Math.pow(base.get(i), exp.get(i)));
              }
              result.setValueCount(numRows);
              return result;
            }

            @Override
            public List<Field> coerceTypes(List<Field> argFields) {
              return List.of(
                  Field.nullable(
                      "base", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  Field.nullable(
                      "exp", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
            }
          };

      ctx.registerUdf(pow, allocator);

      try (DataFrame df = ctx.sql("SELECT pow(a, b) FROM t");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(4, root.getRowCount());

        Float8Vector result = (Float8Vector) root.getVector(0);

        // 2.1^1.0 = 2.1
        assertEquals(2.1, result.get(0), 0.0001);
        // 3.1^2.0 = 9.61
        assertEquals(9.61, result.get(1), 0.0001);
        // 4.1^3.0 = 68.921
        assertEquals(68.921, result.get(2), 0.001);
        // 5.1^4.0 = 676.5201
        assertEquals(676.5201, result.get(3), 0.001);

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testSimpleFactoryUdf() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      // Create test data: x=[10, 20, 30]
      VectorSchemaRoot testData = createInt64TestData(allocator);
      ctx.registerTable("t", testData, allocator);

      // Create a simple "double_it" UDF using the factory
      ScalarUdf doubleIt =
          ScalarUdf.simple(
              "double_it",
              Volatility.IMMUTABLE,
              List.of(new ArrowType.Int(64, true)),
              new ArrowType.Int(64, true),
              (args, numRows, alloc) -> {
                BigIntVector input = (BigIntVector) args.get(0);
                BigIntVector result = new BigIntVector("double_it", alloc);
                result.allocateNew(numRows);
                for (int i = 0; i < numRows; i++) {
                  result.set(i, input.get(i) * 2);
                }
                result.setValueCount(numRows);
                return result;
              });

      ctx.registerUdf(doubleIt, allocator);

      try (DataFrame df = ctx.sql("SELECT double_it(x) FROM t");
          RecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(3, root.getRowCount());

        BigIntVector result = (BigIntVector) root.getVector(0);
        assertEquals(20, result.get(0));
        assertEquals(40, result.get(1));
        assertEquals(60, result.get(2));

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testUdfErrorPropagation() {
    try (BufferAllocator allocator = new RootAllocator()) {
      try (SessionContext ctx = new SessionContext()) {
        VectorSchemaRoot testData = createInt64TestData(allocator);
        try {
          ctx.registerTable("t", testData, allocator);

          // Create a UDF that always throws
          ScalarUdf failingUdf =
              ScalarUdf.simple(
                  "fail_udf",
                  Volatility.VOLATILE,
                  List.of(new ArrowType.Int(64, true)),
                  new ArrowType.Int(64, true),
                  (args, numRows, alloc) -> {
                    throw new RuntimeException("Intentional UDF failure");
                  });

          ctx.registerUdf(failingUdf, allocator);

          assertThrows(
              Exception.class,
              () -> {
                try (DataFrame df = ctx.sql("SELECT fail_udf(x) FROM t");
                    RecordBatchStream stream = df.executeStream(allocator)) {
                  stream.getVectorSchemaRoot();
                  stream.loadNextBatch();
                }
              });
        } finally {
          testData.close();
        }
      }
    }
  }

  private VectorSchemaRoot createFloat64TestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field(
                    "a",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null),
                new Field(
                    "b",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    Float8Vector aVector = (Float8Vector) root.getVector("a");
    Float8Vector bVector = (Float8Vector) root.getVector("b");

    aVector.allocateNew(4);
    bVector.allocateNew(4);

    aVector.set(0, 2.1);
    aVector.set(1, 3.1);
    aVector.set(2, 4.1);
    aVector.set(3, 5.1);

    bVector.set(0, 1.0);
    bVector.set(1, 2.0);
    bVector.set(2, 3.0);
    bVector.set(3, 4.0);

    aVector.setValueCount(4);
    bVector.setValueCount(4);
    root.setRowCount(4);

    return root;
  }

  private VectorSchemaRoot createInt64TestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    BigIntVector xVector = (BigIntVector) root.getVector("x");
    xVector.allocateNew(3);
    xVector.set(0, 10);
    xVector.set(1, 20);
    xVector.set(2, 30);
    xVector.setValueCount(3);
    root.setRowCount(3);

    return root;
  }
}
