package org.apache.arrow.datafusion.testutil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import org.apache.arrow.datafusion.physical_plan.SendableRecordBatchStream;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

/**
 * Tabular assertion DSL for Arrow {@link VectorSchemaRoot} contents. Replaces the verbose {@code
 * (Vector) root.getVector(...) + .get(i) + assertEquals(...)} pattern.
 *
 * <p>Static-import {@link #expect} and {@link #NULL} for readability:
 *
 * <pre>{@code
 * import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.expect;
 * import static org.apache.arrow.datafusion.testutil.VectorSchemaRootAssert.NULL;
 *
 * expect("name", "id")
 *     .row("Alice", 1L)
 *     .row("Bob", 2L)
 *     .assertMatches(stream);
 * }</pre>
 *
 * <p>Defaults: schema must match exactly (no extra columns), float comparisons use
 * {@link org.apache.arrow.vector.util.Validator}-style relative tolerance (1e-12 double / 1e-6
 * float), dictionary-encoded VarChar columns are auto-decoded, rows are matched in order, and any
 * extra rows or batches in the actual data fail the assertion. Toggle via {@link
 * #allowExtraColumns()}, {@link #withDelta(double)}, {@link #compareDictionaryIndices()}, {@link
 * #unordered()}, {@link #batch()}.
 */
public final class VectorSchemaRootAssert {

  /**
   * Sentinel for an expected null cell. Bare {@code null} can't be passed through {@code Object...}
   * varargs unambiguously, so callers write {@code .row(NULL, "x")} instead of {@code .row(null,
   * "x")}.
   */
  public static final Object NULL =
      new Object() {
        @Override
        public String toString() {
          return "NULL";
        }
      };

  private final List<String> columnNames;
  private final List<Batch> batches = new ArrayList<>();
  private double delta = -1.0; // -1.0 => use Validator.equalEnough relative tolerance
  private boolean unordered;
  private boolean allowExtraColumns;
  private boolean compareDictionaryIndices;

  private VectorSchemaRootAssert(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  /** Begin an assertion against the named columns, in column order. */
  public static VectorSchemaRootAssert expect(String... columnNames) {
    if (columnNames.length == 0) {
      throw new IllegalArgumentException("expect(...) requires at least one column name");
    }
    return new VectorSchemaRootAssert(Arrays.asList(columnNames));
  }

  /** Add an expected row. Use {@link #NULL} for null cells. */
  public VectorSchemaRootAssert row(Object... values) {
    if (values.length != columnNames.size()) {
      throw new IllegalArgumentException(
          "row() expected " + columnNames.size() + " values, got " + values.length);
    }
    if (batches.isEmpty()) {
      batches.add(new Batch());
    }
    batches.get(batches.size() - 1).rows.add(Arrays.asList(values));
    return this;
  }

  /**
   * Start a new expected batch boundary. Use only when batch boundaries matter (verifies that
   * actual data has the same per-batch grouping). Without {@code batch()}, all rows are compared
   * flat regardless of how the actual stream chunks them.
   */
  public VectorSchemaRootAssert batch() {
    batches.add(new Batch());
    return this;
  }

  /**
   * Use absolute float/double tolerance {@code |a - b| <= delta} instead of the default relative
   * tolerance.
   */
  public VectorSchemaRootAssert withDelta(double delta) {
    this.delta = delta;
    return this;
  }

  /** Match rows in any order. Default is order-sensitive. */
  public VectorSchemaRootAssert unordered() {
    this.unordered = true;
    return this;
  }

  /**
   * Allow the actual schema to contain columns not declared in {@link #expect}. Default fails on
   * any extra column.
   */
  public VectorSchemaRootAssert allowExtraColumns() {
    this.allowExtraColumns = true;
    return this;
  }

  /**
   * Compare dictionary-encoded VarChar columns by their integer indices instead of decoding to
   * strings. Default auto-decodes via the stream/reader's dictionary provider.
   */
  public VectorSchemaRootAssert compareDictionaryIndices() {
    this.compareDictionaryIndices = true;
    return this;
  }

  // ---- Terminal: VectorSchemaRoot ----

  /** Assert against a single in-memory {@link VectorSchemaRoot} (no streaming). */
  public void assertMatches(VectorSchemaRoot root) {
    assertMatches(root, /* dictionaryProvider */ null);
  }

  /** Assert against a single VSR with an explicit dictionary provider for decoding. */
  public void assertMatches(VectorSchemaRoot root, DictionaryProvider dictionaryProvider) {
    if (batches.size() > 1) {
      throw new IllegalStateException(
          "expect(...).batch()...assertMatches(VectorSchemaRoot) is not supported — a "
              + "single VectorSchemaRoot is one batch. Drop the .batch() calls or pass a stream.");
    }
    List<List<Object>> actualRows = extractRows(root, dictionaryProvider);
    List<Batch> actualBatches = List.of(new Batch(actualRows));
    verify(root, actualBatches);
  }

  // ---- Terminal: SendableRecordBatchStream ----

  /** Drain {@code stream} and assert. Fails if any batch remains after the expected rows. */
  public void assertMatches(SendableRecordBatchStream stream) {
    drainAndVerify(stream.getVectorSchemaRoot(), stream::loadNextBatch, stream);
  }

  // ---- Terminal: ArrowReader ----

  /** Drain {@code reader} and assert. */
  public void assertMatches(ArrowReader reader) {
    VectorSchemaRoot root;
    try {
      root = reader.getVectorSchemaRoot();
    } catch (IOException e) {
      throw new AssertionError("ArrowReader.getVectorSchemaRoot failed: " + e.getMessage(), e);
    }
    drainAndVerify(
        root,
        () -> {
          try {
            return reader.loadNextBatch();
          } catch (IOException e) {
            throw new AssertionError("ArrowReader.loadNextBatch failed: " + e.getMessage(), e);
          }
        },
        reader);
  }

  /**
   * Generic stream overload: drains by repeatedly calling {@code loadNextBatch} and reading
   * {@code root} on each iteration. Use this for stream types not directly supported (e.g. the
   * legacy async {@code RecordBatchStream} from {@code datafusion-java}, where the caller wraps
   * {@code .loadNextBatch().join()}).
   */
  public void assertMatches(
      VectorSchemaRoot root, BooleanSupplier loadNextBatch, DictionaryProvider dictionaryProvider) {
    drainAndVerify(root, loadNextBatch, dictionaryProvider);
  }

  private void drainAndVerify(
      VectorSchemaRoot root, BooleanSupplier loadNext, DictionaryProvider dp) {
    List<Batch> actualBatches = new ArrayList<>();
    while (loadNext.getAsBoolean()) {
      actualBatches.add(new Batch(extractRows(root, dp)));
    }
    verify(root, actualBatches);
  }

  // ---- Terminal: assertEmpty variants ----

  /** Assert the stream produces no rows; schema must still match {@link #expect}. */
  public void assertEmpty(SendableRecordBatchStream stream) {
    assertEmpty(stream.getVectorSchemaRoot(), stream::loadNextBatch);
  }

  /** Assert the reader produces no rows; schema must still match. */
  public void assertEmpty(ArrowReader reader) {
    VectorSchemaRoot root;
    try {
      root = reader.getVectorSchemaRoot();
    } catch (IOException e) {
      throw new AssertionError("ArrowReader.getVectorSchemaRoot failed: " + e.getMessage(), e);
    }
    assertEmpty(
        root,
        () -> {
          try {
            return reader.loadNextBatch();
          } catch (IOException e) {
            throw new AssertionError("ArrowReader.loadNextBatch failed: " + e.getMessage(), e);
          }
        });
  }

  /** Assert {@code root} has zero rows; schema must still match. */
  public void assertEmpty(VectorSchemaRoot root) {
    verifySchemaOnly(root);
    if (root.getRowCount() > 0) {
      throw new AssertionError(
          "expected empty VectorSchemaRoot, but it has " + root.getRowCount() + " row(s)");
    }
  }

  /** Generic empty overload (e.g. for the legacy async stream). */
  public void assertEmpty(VectorSchemaRoot root, BooleanSupplier loadNextBatch) {
    int totalRows = 0;
    while (loadNextBatch.getAsBoolean()) {
      totalRows += root.getRowCount();
    }
    verifySchemaOnly(root);
    if (totalRows > 0) {
      throw new AssertionError(
          "expected empty result, but stream produced " + totalRows + " row(s)");
    }
  }

  // ---- Core verification ----

  private void verify(VectorSchemaRoot root, List<Batch> actualBatches) {
    verifySchemaOnly(root);

    List<List<Object>> expectedFlat = flattenRows(batches);
    List<List<Object>> actualFlat = flattenRows(actualBatches);

    boolean enforceBatchBoundaries = batches.size() > 1;
    List<String> errors = new ArrayList<>();

    if (enforceBatchBoundaries) {
      if (batches.size() != actualBatches.size()) {
        errors.add(
            "expected "
                + batches.size()
                + " batch(es), got "
                + actualBatches.size()
                + " batch(es)");
      } else {
        for (int b = 0; b < batches.size(); b++) {
          int eRows = batches.get(b).rows.size();
          int aRows = actualBatches.get(b).rows.size();
          if (eRows != aRows) {
            errors.add("batch " + b + ": expected " + eRows + " row(s), got " + aRows);
          }
        }
      }
    }

    List<int[]> mismatches = new ArrayList<>();
    if (unordered) {
      compareUnordered(expectedFlat, actualFlat, root, errors);
    } else {
      compareOrdered(expectedFlat, actualFlat, root, errors, mismatches);
    }

    if (!errors.isEmpty() || !mismatches.isEmpty()) {
      String message = renderFailure(expectedFlat, actualFlat, mismatches, errors);
      throw new AssertionError(message);
    }
  }

  private void verifySchemaOnly(VectorSchemaRoot root) {
    Set<String> actualNames = new HashSet<>();
    for (FieldVector fv : root.getFieldVectors()) {
      actualNames.add(fv.getField().getName());
    }
    for (String expected : columnNames) {
      if (!actualNames.contains(expected)) {
        throw new AssertionError("column '" + expected + "' not present in actual VSR");
      }
    }
    if (!allowExtraColumns) {
      List<String> extras = new ArrayList<>();
      for (FieldVector fv : root.getFieldVectors()) {
        if (!columnNames.contains(fv.getField().getName())) {
          extras.add(fv.getField().getName());
        }
      }
      if (!extras.isEmpty()) {
        throw new AssertionError(
            "extra columns not declared in expect(...): "
                + extras
                + ". Use .allowExtraColumns() to ignore.");
      }
    }
  }

  private void compareOrdered(
      List<List<Object>> expected,
      List<List<Object>> actual,
      VectorSchemaRoot root,
      List<String> errors,
      List<int[]> mismatches) {
    if (expected.size() != actual.size()) {
      errors.add("expected " + expected.size() + " row(s), got " + actual.size() + " row(s)");
    }
    int common = Math.min(expected.size(), actual.size());
    for (int i = 0; i < common; i++) {
      List<Object> e = expected.get(i);
      List<Object> a = actual.get(i);
      for (int c = 0; c < columnNames.size(); c++) {
        Field field = fieldFor(root, columnNames.get(c));
        if (!cellEquals(field, e.get(c), a.get(c))) {
          mismatches.add(new int[] {i, c});
        }
      }
    }
  }

  private void compareUnordered(
      List<List<Object>> expected,
      List<List<Object>> actual,
      VectorSchemaRoot root,
      List<String> errors) {
    if (expected.size() != actual.size()) {
      errors.add("expected " + expected.size() + " row(s), got " + actual.size() + " row(s)");
    }
    List<List<Object>> remaining = new ArrayList<>(actual);
    List<List<Object>> unmatched = new ArrayList<>();
    for (List<Object> e : expected) {
      int found = -1;
      for (int j = 0; j < remaining.size(); j++) {
        if (rowEquals(root, e, remaining.get(j))) {
          found = j;
          break;
        }
      }
      if (found >= 0) {
        remaining.remove(found);
      } else {
        unmatched.add(e);
      }
    }
    if (!unmatched.isEmpty()) {
      errors.add("unordered: " + unmatched.size() + " expected row(s) had no match in actual data");
      for (List<Object> u : unmatched) {
        errors.add("  unmatched expected: " + formatRow(u));
      }
      for (List<Object> r : remaining) {
        errors.add("  extra actual:       " + formatRow(r));
      }
    }
  }

  private boolean rowEquals(VectorSchemaRoot root, List<Object> e, List<Object> a) {
    for (int c = 0; c < columnNames.size(); c++) {
      Field field = fieldFor(root, columnNames.get(c));
      if (!cellEquals(field, e.get(c), a.get(c))) {
        return false;
      }
    }
    return true;
  }

  // ---- Cell extraction ----

  private List<List<Object>> extractRows(VectorSchemaRoot root, DictionaryProvider provider) {
    int rowCount = root.getRowCount();
    int numCols = columnNames.size();
    List<List<Object>> rows = new ArrayList<>(rowCount);

    // Pre-decode dictionary columns once per batch.
    FieldVector[] decodedCols = new FieldVector[numCols];
    for (int c = 0; c < numCols; c++) {
      FieldVector fv = root.getVector(columnNames.get(c));
      if (fv == null) continue;
      DictionaryEncoding enc = fv.getField().getDictionary();
      if (enc != null && !compareDictionaryIndices && provider != null) {
        Dictionary dict = provider.lookup(enc.getId());
        if (dict != null) {
          decodedCols[c] = (FieldVector) DictionaryEncoder.decode(fv, dict);
        }
      }
    }

    try {
      for (int i = 0; i < rowCount; i++) {
        List<Object> row = new ArrayList<>(numCols);
        for (int c = 0; c < numCols; c++) {
          FieldVector fv = root.getVector(columnNames.get(c));
          if (fv == null) {
            row.add(null);
            continue;
          }
          if (decodedCols[c] != null) {
            row.add(decodedCols[c].getObject(i));
          } else {
            row.add(fv.getObject(i));
          }
        }
        rows.add(row);
      }
    } finally {
      for (FieldVector v : decodedCols) {
        if (v != null) v.close();
      }
    }
    return rows;
  }

  // ---- Cell comparison ----

  private boolean cellEquals(Field field, Object expected, Object actual) {
    if (expected == NULL) {
      return actual == null;
    }
    if (expected == null) {
      return actual == null;
    }
    if (actual == null) {
      return false;
    }

    // Float/Double tolerance.
    if (actual instanceof Double && expected instanceof Number) {
      return doubleEquals(((Double) actual).doubleValue(), ((Number) expected).doubleValue());
    }
    if (actual instanceof Float && expected instanceof Number) {
      return floatEquals(((Float) actual).floatValue(), ((Number) expected).floatValue());
    }

    // Integer-like types — widen both to long for comparison.
    if (actual instanceof Number
        && expected instanceof Number
        && isIntegerLike(actual)
        && isIntegerLike(expected)) {
      return ((Number) actual).longValue() == ((Number) expected).longValue();
    }

    // Strings: actual may be Text (VarChar) or String (decoded dict).
    if (expected instanceof String) {
      if (actual instanceof Text t) return t.toString().equals(expected);
      if (actual instanceof String s) return s.equals(expected);
      if (actual instanceof byte[] b) return new String(b).equals(expected);
    }

    // Binary.
    if (expected instanceof byte[] eb && actual instanceof byte[] ab) {
      return Arrays.equals(eb, ab);
    }

    // Booleans.
    if (expected instanceof Boolean && actual instanceof Boolean) {
      return expected.equals(actual);
    }

    // Lists.
    if (expected instanceof List<?> el && actual instanceof List<?> al) {
      if (el.size() != al.size()) return false;
      for (int i = 0; i < el.size(); i++) {
        if (!cellEquals(field, el.get(i), al.get(i))) return false;
      }
      return true;
    }

    // Maps (struct).
    if (expected instanceof Map<?, ?> em && actual instanceof Map<?, ?> am) {
      if (em.size() != am.size()) return false;
      for (Map.Entry<?, ?> entry : em.entrySet()) {
        if (!am.containsKey(entry.getKey())) return false;
        if (!cellEquals(field, entry.getValue(), am.get(entry.getKey()))) return false;
      }
      return true;
    }

    return Objects.equals(expected, actual);
  }

  private static boolean isIntegerLike(Object o) {
    return o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte;
  }

  private boolean doubleEquals(double a, double e) {
    if (delta >= 0) {
      return Math.abs(a - e) <= delta;
    }
    if (Double.isNaN(a)) return Double.isNaN(e);
    if (Double.isInfinite(a)) {
      return Double.isInfinite(e) && Math.signum(a) == Math.signum(e);
    }
    double avg = Math.abs((a + e) / 2.0);
    double scaled = Math.abs(a - e) / (avg == 0.0 ? 1.0 : avg);
    return scaled < 1.0E-12;
  }

  private boolean floatEquals(float a, float e) {
    if (delta >= 0) {
      return Math.abs(a - e) <= delta;
    }
    if (Float.isNaN(a)) return Float.isNaN(e);
    if (Float.isInfinite(a)) {
      return Float.isInfinite(e) && Math.signum(a) == Math.signum(e);
    }
    float avg = Math.abs((a + e) / 2.0f);
    float scaled = Math.abs(a - e) / (avg == 0.0f ? 1.0f : avg);
    return scaled < 1.0E-6f;
  }

  // ---- Helpers ----

  private static List<List<Object>> flattenRows(List<Batch> batches) {
    List<List<Object>> out = new ArrayList<>();
    for (Batch b : batches) {
      out.addAll(b.rows);
    }
    return out;
  }

  private static Field fieldFor(VectorSchemaRoot root, String name) {
    FieldVector fv = root.getVector(name);
    return fv == null ? null : fv.getField();
  }

  // ---- Failure rendering ----

  private String renderFailure(
      List<List<Object>> expected,
      List<List<Object>> actual,
      List<int[]> mismatches,
      List<String> errors) {
    StringBuilder sb = new StringBuilder();
    Set<Integer> mismatchRows = new HashSet<>();
    for (int[] m : mismatches) {
      mismatchRows.add(m[0]);
    }
    int mismatchCells = mismatches.size();

    sb.append("VectorSchemaRoot mismatch: ");
    sb.append(actual.size())
        .append(" actual row(s), ")
        .append(expected.size())
        .append(" expected row(s)");
    if (mismatchCells > 0) {
      sb.append(", ").append(mismatchCells).append(" mismatched cell(s)");
    }
    sb.append('\n');

    for (String err : errors) {
      sb.append("  ").append(err).append('\n');
    }

    int total = Math.max(expected.size(), actual.size());
    List<Integer> rowIndices = pickRowsToShow(total, mismatchRows);

    String header = renderHeader(columnNames);
    int leftWidth = computeWidth(header, expected) + 2;
    int rightWidth = computeWidth(header, actual) + 2;

    sb.append('\n');
    sb.append(padRight("EXPECTED", leftWidth)).append("    ").append("ACTUAL").append('\n');
    sb.append(padRight(header, leftWidth)).append("    ").append(header).append('\n');
    sb.append("─".repeat(leftWidth)).append("    ").append("─".repeat(rightWidth)).append('\n');

    for (int idx : rowIndices) {
      String left = idx < expected.size() ? renderRow(expected.get(idx)) : "(missing)";
      String right = idx < actual.size() ? renderRow(actual.get(idx)) : "(missing)";
      String prefix = mismatchRows.contains(idx) ? "> " : "  ";
      String suffix = mismatchRows.contains(idx) ? "  ← differs" : "";
      sb.append(prefix).append(padRight(left, leftWidth - 2)).append("    ");
      sb.append(prefix).append(padRight(right, rightWidth - 2));
      sb.append(suffix);
      sb.append('\n');
    }
    return sb.toString();
  }

  private static int computeWidth(String header, List<List<Object>> rows) {
    int w = header.length();
    for (List<Object> r : rows) {
      w = Math.max(w, renderRow(r).length());
    }
    return w;
  }

  private static List<Integer> pickRowsToShow(int total, Set<Integer> mismatchRows) {
    if (total <= 20) {
      List<Integer> r = new ArrayList<>();
      for (int i = 0; i < total; i++) r.add(i);
      return r;
    }
    Set<Integer> show = new HashSet<>();
    for (int i = 0; i < 10; i++) show.add(i);
    for (int i = total - 10; i < total; i++) show.add(i);
    for (int m : mismatchRows) {
      show.add(m);
      if (m - 1 >= 0) show.add(m - 1);
      if (m - 2 >= 0) show.add(m - 2);
      if (m + 1 < total) show.add(m + 1);
      if (m + 2 < total) show.add(m + 2);
    }
    List<Integer> r = new ArrayList<>(show);
    Collections.sort(r);
    return r;
  }

  private static String renderHeader(List<String> cols) {
    return String.join(" | ", cols);
  }

  private static String renderRow(List<Object> row) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < row.size(); i++) {
      if (i > 0) sb.append(" | ");
      sb.append(formatCell(row.get(i)));
    }
    return sb.toString();
  }

  private static String formatRow(List<Object> row) {
    return "[" + renderRow(row) + "]";
  }

  private static String formatCell(Object o) {
    if (o == null || o == NULL) return "NULL";
    if (o instanceof byte[] b) return Arrays.toString(b);
    if (o instanceof Text t) return t.toString();
    return String.valueOf(o);
  }

  private static String padRight(String s, int width) {
    if (s.length() >= width) return s;
    StringBuilder sb = new StringBuilder(s);
    while (sb.length() < width) sb.append(' ');
    return sb.toString();
  }

  private static final class Batch {
    final List<List<Object>> rows;

    Batch() {
      this.rows = new ArrayList<>();
    }

    Batch(List<List<Object>> rows) {
      this.rows = rows;
    }
  }
}
