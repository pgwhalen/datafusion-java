package org.apache.arrow.datafusion.testutil;

import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.NULL;
import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class VectorSchemaRootAssertTest {

  @Test
  void passingMatchOnAllPrimitiveTypes() {
    try (BufferAllocator alloc = new RootAllocator()) {
      Schema schema =
          new Schema(
              Arrays.asList(
                  field("a", new ArrowType.Int(64, true)),
                  field("b", new ArrowType.Int(32, true)),
                  field("c", new ArrowType.Utf8()),
                  field(
                      "d",
                      new ArrowType.FloatingPoint(
                          org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)),
                  field(
                      "e",
                      new ArrowType.FloatingPoint(
                          org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)),
                  field("f", new ArrowType.Bool()),
                  field("g", new ArrowType.Binary())));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
        BigIntVector a = (BigIntVector) root.getVector("a");
        IntVector b = (IntVector) root.getVector("b");
        VarCharVector c = (VarCharVector) root.getVector("c");
        Float8Vector d = (Float8Vector) root.getVector("d");
        Float4Vector e = (Float4Vector) root.getVector("e");
        BitVector f = (BitVector) root.getVector("f");
        VarBinaryVector g = (VarBinaryVector) root.getVector("g");
        a.allocateNew(2);
        b.allocateNew(2);
        c.allocateNew(2);
        d.allocateNew(2);
        e.allocateNew(2);
        f.allocateNew(2);
        g.allocateNew(2);
        a.set(0, 1L);
        a.set(1, 2L);
        b.set(0, 10);
        b.set(1, 20);
        c.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
        c.setSafe(1, "y".getBytes(StandardCharsets.UTF_8));
        d.set(0, 1.5);
        d.set(1, 2.5);
        e.set(0, 1.5f);
        e.set(1, 2.5f);
        f.set(0, 1);
        f.set(1, 0);
        g.setSafe(0, new byte[] {1, 2});
        g.setSafe(1, new byte[] {3, 4});
        a.setValueCount(2);
        b.setValueCount(2);
        c.setValueCount(2);
        d.setValueCount(2);
        e.setValueCount(2);
        f.setValueCount(2);
        g.setValueCount(2);
        root.setRowCount(2);

        expect("a", "b", "c", "d", "e", "f", "g")
            .row(1L, 10, "x", 1.5, 1.5f, true, new byte[] {1, 2})
            .row(2L, 20, "y", 2.5, 2.5f, false, new byte[] {3, 4})
            .assertMatches(root);
      }
    }
  }

  @Test
  void mismatchRendersDiff() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1, 2}, new String[] {"A", "B"})) {
      AssertionError err =
          assertThrows(
              AssertionError.class,
              () -> expect("id", "name").row(1L, "A").row(2L, "C").assertMatches(root));
      assertTrue(err.getMessage().contains("mismatched cell"), err.getMessage());
      assertTrue(err.getMessage().contains("← differs"), err.getMessage());
    }
  }

  @Test
  void extraRowFails() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root =
            makeIdNameRoot(alloc, new long[] {1, 2, 3}, new String[] {"A", "B", "C"})) {
      AssertionError err =
          assertThrows(
              AssertionError.class,
              () -> expect("id", "name").row(1L, "A").row(2L, "B").assertMatches(root));
      assertTrue(err.getMessage().contains("expected 2 row(s), got 3"), err.getMessage());
    }
  }

  @Test
  void missingRowFails() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1}, new String[] {"A"})) {
      AssertionError err =
          assertThrows(
              AssertionError.class,
              () -> expect("id", "name").row(1L, "A").row(2L, "B").assertMatches(root));
      assertTrue(err.getMessage().contains("expected 2 row(s), got 1"), err.getMessage());
    }
  }

  @Test
  void doubleToleranceDefault() {
    try (BufferAllocator alloc = new RootAllocator()) {
      Schema schema =
          new Schema(
              List.of(
                  field(
                      "v",
                      new ArrowType.FloatingPoint(
                          org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
        Float8Vector v = (Float8Vector) root.getVector("v");
        v.allocateNew(1);
        v.set(0, 1.0 + 1e-15); // within 1e-12 relative tol
        v.setValueCount(1);
        root.setRowCount(1);
        expect("v").row(1.0).assertMatches(root);
      }
    }
  }

  @Test
  void withDeltaAbsolute() {
    try (BufferAllocator alloc = new RootAllocator()) {
      Schema schema =
          new Schema(
              List.of(
                  field(
                      "v",
                      new ArrowType.FloatingPoint(
                          org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
        Float8Vector v = (Float8Vector) root.getVector("v");
        v.allocateNew(1);
        v.set(0, 5000.005);
        v.setValueCount(1);
        root.setRowCount(1);
        expect("v").withDelta(0.01).row(5000.0).assertMatches(root);
      }
    }
  }

  @Test
  void nullSentinel() {
    try (BufferAllocator alloc = new RootAllocator()) {
      Schema schema = new Schema(List.of(field("v", new ArrowType.Utf8())));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
        VarCharVector v = (VarCharVector) root.getVector("v");
        v.allocateNew(2);
        v.setNull(0);
        v.setSafe(1, "present".getBytes(StandardCharsets.UTF_8));
        v.setValueCount(2);
        root.setRowCount(2);
        expect("v").row(NULL).row("present").assertMatches(root);
      }
    }
  }

  @Test
  void unorderedMatch() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {2, 1}, new String[] {"B", "A"})) {
      expect("id", "name").unordered().row(1L, "A").row(2L, "B").assertMatches(root);
    }
  }

  @Test
  void schemaExtraColumnFails() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1}, new String[] {"A"})) {
      AssertionError err =
          assertThrows(AssertionError.class, () -> expect("id").row(1L).assertMatches(root));
      assertTrue(err.getMessage().contains("extra columns"), err.getMessage());
    }
  }

  @Test
  void allowExtraColumnsPasses() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1}, new String[] {"A"})) {
      expect("id").allowExtraColumns().row(1L).assertMatches(root);
    }
  }

  @Test
  void schemaMissingColumnFails() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1}, new String[] {"A"})) {
      AssertionError err =
          assertThrows(
              AssertionError.class, () -> expect("id", "missing").row(1L, "x").assertMatches(root));
      assertTrue(err.getMessage().contains("'missing'"), err.getMessage());
    }
  }

  @Test
  void assertEmptyOnEmptyRoot() {
    try (BufferAllocator alloc = new RootAllocator()) {
      Schema schema = new Schema(List.of(field("id", new ArrowType.Int(64, true))));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
        BigIntVector v = (BigIntVector) root.getVector("id");
        v.allocateNew(0);
        v.setValueCount(0);
        root.setRowCount(0);
        expect("id").assertEmpty(root);
      }
    }
  }

  @Test
  void assertEmptyFailsIfRowsPresent() {
    try (BufferAllocator alloc = new RootAllocator();
        VectorSchemaRoot root = makeIdNameRoot(alloc, new long[] {1}, new String[] {"A"})) {
      assertThrows(AssertionError.class, () -> expect("id", "name").assertEmpty(root));
    }
  }

  @Test
  void dictionaryAutoDecode() {
    try (BufferAllocator alloc = new RootAllocator()) {
      // Build a dictionary [apple, banana] and an encoded vector [0, 1, 0]
      VarCharVector dictVec = new VarCharVector("dict", alloc);
      dictVec.allocateNew(2);
      dictVec.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      dictVec.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      dictVec.setValueCount(2);

      DictionaryEncoding enc = new DictionaryEncoding(7L, false, new ArrowType.Int(32, true));
      Dictionary dict = new Dictionary(dictVec, enc);

      VarCharVector raw = new VarCharVector("fruit", alloc);
      raw.allocateNew(3);
      raw.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      raw.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      raw.setSafe(2, "apple".getBytes(StandardCharsets.UTF_8));
      raw.setValueCount(3);
      IntVector encoded = (IntVector) DictionaryEncoder.encode(raw, dict);
      raw.close();

      Schema schema =
          new Schema(
              List.of(
                  new Field("fruit", new FieldType(true, new ArrowType.Int(32, true), enc), null)));
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, List.of(encoded), 3)) {
        DictionaryProvider.MapDictionaryProvider provider =
            new DictionaryProvider.MapDictionaryProvider();
        provider.put(dict);

        expect("fruit").row("apple").row("banana").row("apple").assertMatches(root, provider);
      }
      dictVec.close();
    }
  }

  @Test
  void compareDictionaryIndicesMode() {
    try (BufferAllocator alloc = new RootAllocator()) {
      VarCharVector dictVec = new VarCharVector("dict", alloc);
      dictVec.allocateNew(2);
      dictVec.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      dictVec.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      dictVec.setValueCount(2);
      DictionaryEncoding enc = new DictionaryEncoding(7L, false, new ArrowType.Int(32, true));
      Dictionary dict = new Dictionary(dictVec, enc);

      VarCharVector raw = new VarCharVector("fruit", alloc);
      raw.allocateNew(2);
      raw.setSafe(0, "apple".getBytes(StandardCharsets.UTF_8));
      raw.setSafe(1, "banana".getBytes(StandardCharsets.UTF_8));
      raw.setValueCount(2);
      IntVector encoded = (IntVector) DictionaryEncoder.encode(raw, dict);
      raw.close();

      Schema schema =
          new Schema(
              List.of(
                  new Field("fruit", new FieldType(true, new ArrowType.Int(32, true), enc), null)));
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, List.of(encoded), 2)) {
        DictionaryProvider.MapDictionaryProvider provider =
            new DictionaryProvider.MapDictionaryProvider();
        provider.put(dict);
        expect("fruit").compareDictionaryIndices().row(0).row(1).assertMatches(root, provider);
      }
      dictVec.close();
    }
  }

  // ---- helpers ----

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  private static VectorSchemaRoot makeIdNameRoot(
      BufferAllocator alloc, long[] ids, String[] names) {
    Schema schema =
        new Schema(
            Arrays.asList(
                field("id", new ArrowType.Int(64, true)), field("name", new ArrowType.Utf8())));
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
    BigIntVector idVec = (BigIntVector) root.getVector("id");
    VarCharVector nameVec = (VarCharVector) root.getVector("name");
    idVec.allocateNew(ids.length);
    nameVec.allocateNew(names.length);
    for (int i = 0; i < ids.length; i++) {
      idVec.set(i, ids[i]);
      nameVec.setSafe(i, names[i].getBytes(StandardCharsets.UTF_8));
    }
    idVec.setValueCount(ids.length);
    nameVec.setValueCount(names.length);
    root.setRowCount(ids.length);
    return root;
  }
}
