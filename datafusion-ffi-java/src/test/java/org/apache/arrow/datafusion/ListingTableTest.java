package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * A projection-aware FileOpener that only reads columns specified in the projection list. When
   * DataFusion pushes a projection down, it expects the opener to return only the projected
   * columns.
   */
  static class ProjectionAwareTsvFileOpener implements FileOpener {
    private final Schema schema;
    private final BufferAllocator allocator;
    private final List<Integer> projection;

    ProjectionAwareTsvFileOpener(
        Schema schema, BufferAllocator allocator, List<Integer> projection) {
      this.schema = schema;
      this.allocator = allocator;
      this.projection = projection;
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

      // Determine which original column indices to read.
      // The schema passed here is the full table schema. When projection is
      // non-empty, build a projected schema containing only those columns.
      List<Integer> colIndices;
      Schema outputSchema;
      if (!projection.isEmpty()) {
        colIndices = projection;
        List<Field> projectedFields = new ArrayList<>();
        for (int idx : projection) {
          projectedFields.add(schema.getFields().get(idx));
        }
        outputSchema = new Schema(projectedFields);
      } else {
        colIndices = new ArrayList<>();
        for (int i = 0; i < schema.getFields().size(); i++) {
          colIndices.add(i);
        }
        outputSchema = schema;
      }

      VectorSchemaRoot root = VectorSchemaRoot.create(outputSchema, allocator);
      int dataRowCount = lines.length - 1;

      // Allocate vectors
      for (int outCol = 0; outCol < outputSchema.getFields().size(); outCol++) {
        Field field = outputSchema.getFields().get(outCol);
        if (field.getType() instanceof ArrowType.Int) {
          ((BigIntVector) root.getVector(outCol)).allocateNew(dataRowCount);
        } else if (field.getType() instanceof ArrowType.Utf8) {
          ((VarCharVector) root.getVector(outCol)).allocateNew(dataRowCount);
        }
      }

      // Fill vectors — read from original column positions
      for (int row = 0; row < dataRowCount; row++) {
        String[] cells = lines[row + 1].split("\t");
        for (int outCol = 0; outCol < colIndices.size(); outCol++) {
          int srcCol = colIndices.get(outCol);
          String value = cells[srcCol].trim();
          Field field = outputSchema.getFields().get(outCol);
          if (field.getType() instanceof ArrowType.Int) {
            ((BigIntVector) root.getVector(outCol)).set(row, Long.parseLong(value));
          } else if (field.getType() instanceof ArrowType.Utf8) {
            ((VarCharVector) root.getVector(outCol))
                .setSafe(row, value.getBytes(StandardCharsets.UTF_8));
          }
        }
      }

      // Set value counts
      for (int outCol = 0; outCol < outputSchema.getFields().size(); outCol++) {
        Field field = outputSchema.getFields().get(outCol);
        if (field.getType() instanceof ArrowType.Int) {
          ((BigIntVector) root.getVector(outCol)).setValueCount(dataRowCount);
        } else if (field.getType() instanceof ArrowType.Utf8) {
          ((VarCharVector) root.getVector(outCol)).setValueCount(dataRowCount);
        }
      }
      root.setRowCount(dataRowCount);

      return new SingleBatchReader(root);
    }
  }

  /** A FileSource that creates TsvFileOpener instances. */
  static class TsvFileSource implements FileSource {
    @Override
    public FileOpener createFileOpener(
        Schema schema, BufferAllocator allocator, FileScanContext scanContext) {
      return new TsvFileOpener(schema, allocator);
    }

    @Override
    public String fileType() {
      return "tsv";
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

  @Test
  void testScanContextProjectionReceived() throws IOException {
    // Write a TSV file with 3 columns
    Path tsvFile = tempDir.resolve("data.tsv");
    Files.writeString(tsvFile, "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    // Capture the scan context from the callback
    AtomicReference<FileScanContext> capturedContext = new AtomicReference<>();

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        FileSource contextCapturingSource =
            new FileSource() {
              @Override
              public FileOpener createFileOpener(
                  Schema s, BufferAllocator a, FileScanContext scanContext) {
                capturedContext.set(scanContext);
                // When projection is pushed, we must return only projected columns
                return new ProjectionAwareTsvFileOpener(s, a, scanContext.projection());
              }

              @Override
              public String fileType() {
                return "tsv";
              }
            };
        FileFormat format =
            new FileFormat() {
              @Override
              public String getExtension() {
                return ".tsv";
              }

              @Override
              public FileSource fileSource() {
                return contextCapturingSource;
              }
            };

        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();
        ListingTableUrl url = ListingTableUrl.parse(tempDir.toUri().toString());
        ListingTable table =
            ListingTable.builder(url).withListingOptions(options).withSchema(schema).build();

        ctx.registerListingTable("ctx_data", table, allocator);

        // SELECT only 'id' column — DataFusion should push projection
        try (DataFrame df = ctx.sql("SELECT id FROM ctx_data");
            RecordBatchStream stream = df.executeStream(allocator)) {
          while (stream.loadNextBatch()) {
            // consume all batches
          }
        }
      }

      allocator.close();
    }

    // Verify scan context was received
    FileScanContext scanCtx = capturedContext.get();
    assertNotNull(scanCtx, "Scan context should have been captured");
    // Projection should contain column index 0 (the 'id' column)
    assertFalse(scanCtx.projection().isEmpty(), "Projection should not be empty for column subset");
    assertTrue(scanCtx.projection().contains(0), "Projection should include column 0 (id)");
  }

  @Test
  void testScanContextLimitReceived() throws IOException {
    // Write a TSV file with multiple rows
    Path tsvFile = tempDir.resolve("data.tsv");
    Files.writeString(tsvFile, "id\tname\n1\tAlice\n2\tBob\n3\tCharlie\n4\tDave\n5\tEve\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    List<FileScanContext> capturedContexts = new ArrayList<>();

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        FileSource limitCapturingSource =
            new FileSource() {
              @Override
              public FileOpener createFileOpener(
                  Schema s, BufferAllocator a, FileScanContext scanContext) {
                capturedContexts.add(scanContext);
                return new TsvFileOpener(s, a);
              }

              @Override
              public String fileType() {
                return "tsv";
              }
            };
        FileFormat format =
            new FileFormat() {
              @Override
              public String getExtension() {
                return ".tsv";
              }

              @Override
              public FileSource fileSource() {
                return limitCapturingSource;
              }
            };

        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();
        ListingTableUrl url = ListingTableUrl.parse(tempDir.toUri().toString());
        ListingTable table =
            ListingTable.builder(url).withListingOptions(options).withSchema(schema).build();

        ctx.registerListingTable("limit_data", table, allocator);

        // LIMIT 2 — DataFusion should push limit to scan
        try (DataFrame df = ctx.sql("SELECT * FROM limit_data LIMIT 2");
            RecordBatchStream stream = df.executeStream(allocator)) {
          while (stream.loadNextBatch()) {
            // consume all batches
          }
        }
      }

      allocator.close();
    }

    // Verify at least one scan context was captured with a limit
    assertFalse(capturedContexts.isEmpty(), "At least one scan context should be captured");
    boolean foundLimit =
        capturedContexts.stream().anyMatch(c -> c.limit() != null && c.limit() <= 2);
    assertTrue(foundLimit, "At least one scan context should have limit <= 2");
  }

  @Test
  void testCustomFileType() throws IOException {
    // Write a TSV file
    Path tsvFile = tempDir.resolve("data.tsv");
    Files.writeString(tsvFile, "id\tname\n1\tAlice\n");

    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

    try (BufferAllocator rootAllocator = new RootAllocator()) {
      BufferAllocator allocator = rootAllocator.newChildAllocator("test", 0, Long.MAX_VALUE);

      try (SessionContext ctx = new SessionContext()) {
        FileSource customTypeSource =
            new FileSource() {
              @Override
              public FileOpener createFileOpener(
                  Schema s, BufferAllocator a, FileScanContext scanContext) {
                return new TsvFileOpener(s, a);
              }

              @Override
              public String fileType() {
                return "custom_tab_separated";
              }
            };
        FileFormat format =
            new FileFormat() {
              @Override
              public String getExtension() {
                return ".tsv";
              }

              @Override
              public FileSource fileSource() {
                return customTypeSource;
              }
            };

        ListingOptions options = ListingOptions.builder(format).fileExtension(".tsv").build();
        ListingTableUrl url = ListingTableUrl.parse(tempDir.toUri().toString());
        ListingTable table =
            ListingTable.builder(url).withListingOptions(options).withSchema(schema).build();

        ctx.registerListingTable("custom_type_data", table, allocator);

        // Just verify the query works — the file type flows to Rust but isn't
        // directly observable from Java. The fact that the query succeeds
        // without crashes verifies the string was correctly transmitted.
        try (DataFrame df = ctx.sql("SELECT id, name FROM custom_type_data");
            RecordBatchStream stream = df.executeStream(allocator)) {
          assertTrue(stream.loadNextBatch());
          assertEquals(1, stream.getVectorSchemaRoot().getRowCount());
          assertFalse(stream.loadNextBatch());
        }
      }

      allocator.close();
    }
  }
}
