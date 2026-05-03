package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.arrow.datafusion.common.ScalarValue;
import org.apache.arrow.datafusion.dataframe.DataFrame;
import org.apache.arrow.datafusion.execution.SessionContext;
import org.apache.arrow.datafusion.logical_expr.Accumulator;
import org.apache.arrow.datafusion.logical_expr.AggregateUDF;
import org.apache.arrow.datafusion.logical_expr.Signature;
import org.apache.arrow.datafusion.logical_expr.Volatility;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Tests for aggregate UDF registration and execution. */
class AggregateUDFTest {

  @Test
  void testSumUdaf() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createInt64TestData(allocator);
      ctx.registerBatch("t", testData, allocator);

      AggregateUDF mySum = createSumUdaf();
      ctx.registerUdaf(mySum, allocator);

      try (DataFrame df = ctx.sql("SELECT my_sum(x) FROM t");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        BigIntVector result = (BigIntVector) root.getVector(0);
        // 10 + 20 + 30 = 60
        assertEquals(60, result.get(0));

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testUdafWithGroupBy() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createGroupedTestData(allocator);
      ctx.registerBatch("t", testData, allocator);

      AggregateUDF mySum = createSumUdaf();
      ctx.registerUdaf(mySum, allocator);

      try (DataFrame df = ctx.sql("SELECT grp, my_sum(val) FROM t GROUP BY grp ORDER BY grp");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(2, root.getRowCount());

        VarCharVector grpVec = (VarCharVector) root.getVector(0);
        BigIntVector sumVec = (BigIntVector) root.getVector(1);

        assertEquals("a", grpVec.getObject(0).toString());
        assertEquals(3, sumVec.get(0)); // 1 + 2

        assertEquals("b", grpVec.getObject(1).toString());
        assertEquals(7, sumVec.get(1)); // 3 + 4

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testAvgUdaf() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {

      VectorSchemaRoot testData = createFloat64TestData(allocator);
      ctx.registerBatch("t", testData, allocator);

      AggregateUDF myAvg =
          new AggregateUDF() {
            @Override
            public String name() {
              return "my_avg";
            }

            @Override
            public Signature signature() {
              return new Signature(Volatility.IMMUTABLE);
            }

            @Override
            public Field returnField(List<Field> argFields) {
              return Field.nullable(
                  "my_avg", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            }

            @Override
            public Accumulator createAccumulator() {
              return new AvgAccumulator();
            }

            @Override
            public List<Field> stateFields(Field returnField) {
              return List.of(
                  Field.nullable("sum", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                  Field.nullable("count", new ArrowType.Int(64, true)));
            }

            @Override
            public List<Field> coerceTypes(List<Field> argFields) {
              return List.of(
                  Field.nullable(
                      argFields.get(0).getName(),
                      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
            }
          };

      ctx.registerUdaf(myAvg, allocator);

      try (DataFrame df = ctx.sql("SELECT my_avg(a) FROM t");
          SendableRecordBatchStream stream = df.executeStream(allocator)) {

        VectorSchemaRoot root = stream.getVectorSchemaRoot();
        assertTrue(stream.loadNextBatch());
        assertEquals(1, root.getRowCount());

        Float8Vector result = (Float8Vector) root.getVector(0);
        // avg(2.0, 4.0, 6.0, 8.0) = 5.0
        assertEquals(5.0, result.get(0), 0.0001);

        assertFalse(stream.loadNextBatch());
      }

      testData.close();
    }
  }

  @Test
  void testUdafErrorPropagation() {
    try (BufferAllocator allocator = new RootAllocator();
        SessionContext ctx = new SessionContext()) {
      VectorSchemaRoot testData = createInt64TestData(allocator);
      try {
        ctx.registerBatch("t", testData, allocator);

        AggregateUDF failingUdaf =
            new AggregateUDF() {
              @Override
              public String name() {
                return "fail_udaf";
              }

              @Override
              public Signature signature() {
                return new Signature(Volatility.VOLATILE);
              }

              @Override
              public Field returnField(List<Field> argFields) {
                return Field.nullable("fail_udaf", new ArrowType.Int(64, true));
              }

              @Override
              public Accumulator createAccumulator() {
                return new Accumulator() {
                  @Override
                  public void updateBatch(List<FieldVector> values) {
                    throw new RuntimeException("Intentional UDAF failure");
                  }

                  @Override
                  public ScalarValue evaluate() {
                    return new ScalarValue.Int64(0);
                  }

                  @Override
                  public List<ScalarValue> state() {
                    return List.of(new ScalarValue.Int64(0));
                  }

                  @Override
                  public void mergeBatch(List<FieldVector> states) {}

                  @Override
                  public long size() {
                    return 8;
                  }
                };
              }
            };

        ctx.registerUdaf(failingUdaf, allocator);

        assertThrows(
            Exception.class,
            () -> {
              try (DataFrame df = ctx.sql("SELECT fail_udaf(x) FROM t");
                  SendableRecordBatchStream stream = df.executeStream(allocator)) {
                stream.getVectorSchemaRoot();
                stream.loadNextBatch();
              }
            });
      } finally {
        testData.close();
      }
    }
  }

  // ======== Helper classes ========

  static class SumAccumulator implements Accumulator {
    private long sum = 0;

    @Override
    public void updateBatch(List<FieldVector> values) {
      BigIntVector col = (BigIntVector) values.get(0);
      for (int i = 0; i < col.getValueCount(); i++) {
        if (!col.isNull(i)) {
          sum += col.get(i);
        }
      }
    }

    @Override
    public ScalarValue evaluate() {
      return new ScalarValue.Int64(sum);
    }

    @Override
    public List<ScalarValue> state() {
      return List.of(new ScalarValue.Int64(sum));
    }

    @Override
    public void mergeBatch(List<FieldVector> states) {
      BigIntVector partialSums = (BigIntVector) states.get(0);
      for (int i = 0; i < partialSums.getValueCount(); i++) {
        if (!partialSums.isNull(i)) {
          sum += partialSums.get(i);
        }
      }
    }

    @Override
    public long size() {
      return Long.BYTES;
    }
  }

  static class AvgAccumulator implements Accumulator {
    private double sum = 0.0;
    private long count = 0;

    @Override
    public void updateBatch(List<FieldVector> values) {
      Float8Vector col = (Float8Vector) values.get(0);
      for (int i = 0; i < col.getValueCount(); i++) {
        if (!col.isNull(i)) {
          sum += col.get(i);
          count++;
        }
      }
    }

    @Override
    public ScalarValue evaluate() {
      return count == 0 ? new ScalarValue.Null() : new ScalarValue.Float64(sum / count);
    }

    @Override
    public List<ScalarValue> state() {
      return List.of(new ScalarValue.Float64(sum), new ScalarValue.Int64(count));
    }

    @Override
    public void mergeBatch(List<FieldVector> states) {
      Float8Vector partialSums = (Float8Vector) states.get(0);
      BigIntVector partialCounts = (BigIntVector) states.get(1);
      for (int i = 0; i < partialSums.getValueCount(); i++) {
        if (!partialSums.isNull(i)) {
          sum += partialSums.get(i);
        }
        if (!partialCounts.isNull(i)) {
          count += partialCounts.get(i);
        }
      }
    }

    @Override
    public long size() {
      return Double.BYTES + Long.BYTES;
    }
  }

  // ======== Test data helpers ========

  private static AggregateUDF createSumUdaf() {
    return new AggregateUDF() {
      @Override
      public String name() {
        return "my_sum";
      }

      @Override
      public Signature signature() {
        return new Signature(Volatility.IMMUTABLE);
      }

      @Override
      public Field returnField(List<Field> argFields) {
        return Field.nullable("my_sum", new ArrowType.Int(64, true));
      }

      @Override
      public Accumulator createAccumulator() {
        return new SumAccumulator();
      }

      @Override
      public List<Field> coerceTypes(List<Field> argFields) {
        return List.of(Field.nullable(argFields.get(0).getName(), new ArrowType.Int(64, true)));
      }
    };
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

  private VectorSchemaRoot createGroupedTestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("grp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("val", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    VarCharVector grpVector = (VarCharVector) root.getVector("grp");
    BigIntVector valVector = (BigIntVector) root.getVector("val");

    grpVector.allocateNew(4);
    valVector.allocateNew(4);

    grpVector.set(0, "a".getBytes());
    grpVector.set(1, "a".getBytes());
    grpVector.set(2, "b".getBytes());
    grpVector.set(3, "b".getBytes());

    valVector.set(0, 1);
    valVector.set(1, 2);
    valVector.set(2, 3);
    valVector.set(3, 4);

    grpVector.setValueCount(4);
    valVector.setValueCount(4);
    root.setRowCount(4);

    return root;
  }

  private VectorSchemaRoot createFloat64TestData(BufferAllocator allocator) {
    Schema schema =
        new Schema(
            List.of(
                new Field(
                    "a",
                    FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    Float8Vector aVector = (Float8Vector) root.getVector("a");
    aVector.allocateNew(4);
    aVector.set(0, 2.0);
    aVector.set(1, 4.0);
    aVector.set(2, 6.0);
    aVector.set(3, 8.0);
    aVector.setValueCount(4);
    root.setRowCount(4);

    return root;
  }
}
