package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for listing table with Java-implemented FileFormat. */
public class ListingTableTest {

  @TempDir Path tempDir;

  /** A FileOpener that parses TSV content into Arrow record batches. */
  static class TsvFileOpener implements FileOpener {
    private final Schema schema;
    private final BufferAllocator allocator;

    TsvFileOpener(Schema schema, BufferAllocator allocator) {
      this.schema = schema;
      this.allocator = allocator;
    }

    @Override
    public RecordBatchReader open(PartitionedFile file) {
      String text;
      try {
        text = Files.readString(Path.of(file.path()));
      } catch (IOException e) {
        throw new RuntimeException("Failed to read file: " + file.path(), e);
      }
      String[] lines = text.split("\n");

      // Parse data rows (skip header)
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
      int dataRowCount = lines.length - 1;

      // Allocate vectors once before filling
      for (int col = 0; col < schema.getFields().size(); col++) {
        Field field = schema.getFields().get(col);
        if (field.getType() instanceof ArrowType.Int) {
          ((BigIntVector) root.getVector(col)).allocateNew(dataRowCount);
        } else if (field.getType() instanceof ArrowType.Utf8) {
          ((VarCharVector) root.getVector(col)).allocateNew(dataRowCount);
        }
      }

      // Fill vectors with data
      for (int row = 0; row < dataRowCount; row++) {
        String[] cells = lines[row + 1].split("\t");
        for (int col = 0; col < schema.getFields().size(); col++) {
          String value = cells[col].trim();
          Field field = schema.getFields().get(col);
          if (field.getType() instanceof ArrowType.Int) {
            ((BigIntVector) root.getVector(col)).set(row, Long.parseLong(value));
          } else if (field.getType() instanceof ArrowType.Utf8) {
            ((VarCharVector) root.getVector(col))
                .setSafe(row, value.getBytes(StandardCharsets.UTF_8));
          }
        }
      }

      // Set value counts
      for (int col = 0; col < schema.getFields().size(); col++) {
        Field field = schema.getFields().get(col);
        if (field.getType() instanceof ArrowType.Int) {
          ((BigIntVector) root.getVector(col)).setValueCount(dataRowCount);
        } else if (field.getType() instanceof ArrowType.Utf8) {
          ((VarCharVector) root.getVector(col)).setValueCount(dataRowCount);
        }
      }
      root.setRowCount(dataRowCount);

      // Return single-batch reader
      return new SingleBatchReader(root);
    }
  }

  /** A FileSource that creates TsvFileOpener instances. */
  static class TsvFileSource implements FileSource {
    @Override
    public FileOpener createFileOpener(Schema schema, BufferAllocator allocator) {
      return new TsvFileOpener(schema, allocator);
    }
  }

  /** Trivial TSV format using the 3-level chain: FileFormat -> FileSource -> FileOpener. */
  static class TsvFormat implements FileFormat {
    @Override
    public String getExtension() {
      return ".tsv";
    }

    @Override
    public FileSource fileSource() {
      return new TsvFileSource();
    }
  }

  /** A RecordBatchReader that returns a single pre-built batch. */
  static class SingleBatchReader implements RecordBatchReader {
    private final VectorSchemaRoot root;
    private boolean consumed = false;

    SingleBatchReader(VectorSchemaRoot root) {
      this.root = root;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public boolean loadNextBatch() {
      if (consumed) {
        return false;
      }
      consumed = true;
      return true;
    }

    @Override
    public void close() {
      root.close();
    }
  }

  @Test
  void testListingTableWithTsvFormat() throws IOException {
    // Write a TSV file
    Path tsvFile = tempDir.resolve("data.tsv");
    Files.writeString(tsvFile, "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n3\tCharlie\t300\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      // Use a child allocator so we can check for leaks separately
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        TsvFormat format = new TsvFormat();
        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();
        ListingTableUrl url = ListingTableUrl.parse(tempDir.toUri().toString());
        ListingTable table =
            ListingTable.builder(url).withListingOptions(options).withSchema(schema).build();

        ctx.registerListingTable("tsv_data", table, allocator);

        try (DataFrame df = ctx.sql("SELECT id, name, value FROM tsv_data ORDER BY id");
            RecordBatchStream stream = df.executeStream(allocator)) {

          VectorSchemaRoot root = stream.getVectorSchemaRoot();

          assertTrue(stream.loadNextBatch());
          assertEquals(3, root.getRowCount());

          BigIntVector idVector = (BigIntVector) root.getVector("id");
          VarCharVector nameVector = (VarCharVector) root.getVector("name");
          BigIntVector valueVector = (BigIntVector) root.getVector("value");

          assertEquals(1, idVector.get(0));
          assertEquals("Alice", new String(nameVector.get(0)));
          assertEquals(100, valueVector.get(0));

          assertEquals(2, idVector.get(1));
          assertEquals("Bob", new String(nameVector.get(1)));
          assertEquals(200, valueVector.get(1));

          assertEquals(3, idVector.get(2));
          assertEquals("Charlie", new String(nameVector.get(2)));
          assertEquals(300, valueVector.get(2));

          assertFalse(stream.loadNextBatch());
        }
      }

      allocator.close();
    }
  }

  @Test
  void testListingTableWithMultipleFiles() throws IOException {
    // Write two TSV files
    Files.writeString(tempDir.resolve("part1.tsv"), "id\tname\n1\tAlice\n2\tBob\n");
    Files.writeString(tempDir.resolve("part2.tsv"), "id\tname\n3\tCharlie\n4\tDave\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        TsvFormat format = new TsvFormat();
        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();
        ListingTableUrl url = ListingTableUrl.parse(tempDir.toUri().toString());
        ListingTable table =
            ListingTable.builder(url).withListingOptions(options).withSchema(schema).build();

        ctx.registerListingTable("multi_data", table, allocator);

        try (DataFrame df = ctx.sql("SELECT id, name FROM multi_data ORDER BY id");
            RecordBatchStream stream = df.executeStream(allocator)) {

          VectorSchemaRoot root = stream.getVectorSchemaRoot();

          // Collect all rows across batches
          Set<Long> allIds = new HashSet<>();
          int totalRows = 0;

          while (stream.loadNextBatch()) {
            int rowCount = root.getRowCount();
            totalRows += rowCount;
            BigIntVector idVector = (BigIntVector) root.getVector("id");
            for (int i = 0; i < rowCount; i++) {
              allIds.add(idVector.get(i));
            }
          }

          assertEquals(4, totalRows, "Should have 4 total rows from 2 files");
          assertEquals(Set.of(1L, 2L, 3L, 4L), allIds, "Should have ids 1-4");
        }
      }

      allocator.close();
    }
  }

  @Test
  void testListingTableWithMultiPaths() throws IOException {
    // Create two separate directories with TSV files
    Path dir1 = tempDir.resolve("dir1");
    Path dir2 = tempDir.resolve("dir2");
    Files.createDirectories(dir1);
    Files.createDirectories(dir2);
    Files.writeString(dir1.resolve("data.tsv"), "id\tname\n1\tAlice\n2\tBob\n");
    Files.writeString(dir2.resolve("data.tsv"), "id\tname\n3\tCharlie\n4\tDave\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        TsvFormat format = new TsvFormat();
        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();

        // Use builderWithMultiPaths to register multiple directories
        ListingTableUrl url1 = ListingTableUrl.parse(dir1.toUri().toString());
        ListingTableUrl url2 = ListingTableUrl.parse(dir2.toUri().toString());
        ListingTable table =
            ListingTable.builderWithMultiPaths(List.of(url1, url2))
                .withListingOptions(options)
                .withSchema(schema)
                .build();

        ctx.registerListingTable("multi_path_data", table, allocator);

        try (DataFrame df = ctx.sql("SELECT id, name FROM multi_path_data ORDER BY id");
            RecordBatchStream stream = df.executeStream(allocator)) {

          VectorSchemaRoot root = stream.getVectorSchemaRoot();

          Set<Long> allIds = new HashSet<>();
          int totalRows = 0;

          while (stream.loadNextBatch()) {
            int rowCount = root.getRowCount();
            totalRows += rowCount;
            BigIntVector idVector = (BigIntVector) root.getVector("id");
            for (int i = 0; i < rowCount; i++) {
              allIds.add(idVector.get(i));
            }
          }

          assertEquals(4, totalRows, "Should have 4 total rows from 2 directories");
          assertEquals(Set.of(1L, 2L, 3L, 4L), allIds, "Should have ids 1-4");
        }
      }

      allocator.close();
    }
  }
}
