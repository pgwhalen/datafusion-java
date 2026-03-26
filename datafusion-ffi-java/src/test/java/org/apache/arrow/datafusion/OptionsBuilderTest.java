package org.apache.arrow.datafusion;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.arrow.datafusion.datasource.CsvReadOptions;
import org.junit.jupiter.api.Test;

/**
 * Tests for datasource-level option builder classes: {@link CsvOptions}, {@link ParquetOptions},
 * and {@link CsvReadOptions}.
 */
public class OptionsBuilderTest {

  // ── CsvOptions builder and encodeOptions ──

  @Test
  void testCsvOptionsBuilderAllFields() {
    CsvOptions opts =
        CsvOptions.builder()
            .hasHeader(false)
            .delimiter((byte) '\t')
            .quote((byte) '\'')
            .escape((byte) '\\')
            .terminator((byte) '\n')
            .doubleQuote(false)
            .newlinesInValues(true)
            .dateFormat("%Y-%m-%d")
            .datetimeFormat("%Y-%m-%dT%H:%M:%S")
            .timestampFormat("%Y-%m-%dT%H:%M:%S.%f")
            .timestampTzFormat("%Y-%m-%dT%H:%M:%S%z")
            .timeFormat("%H:%M:%S")
            .nullValue("NA")
            .compressionLevel(6)
            .build();

    byte[] encoded = opts.encodeOptions();
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    // Verify round-trip through proto: parse the bytes and check fields
    var proto =
        assertDoesNotThrow(() -> org.apache.arrow.datafusion.proto.CsvOptions.parseFrom(encoded));
    // header is encoded as single-byte ByteString: 0 = false
    assertEquals(0, proto.getHasHeader().byteAt(0));
    assertEquals('\t', proto.getDelimiter().byteAt(0));
    assertEquals('\'', proto.getQuote().byteAt(0));
    assertEquals('\\', proto.getEscape().byteAt(0));
    assertEquals('\n', proto.getTerminator().byteAt(0));
    assertEquals(0, proto.getDoubleQuote().byteAt(0)); // false
    assertEquals(1, proto.getNewlinesInValues().byteAt(0)); // true
    assertEquals("%Y-%m-%d", proto.getDateFormat());
    assertEquals("%Y-%m-%dT%H:%M:%S", proto.getDatetimeFormat());
    assertEquals("%Y-%m-%dT%H:%M:%S.%f", proto.getTimestampFormat());
    assertEquals("%Y-%m-%dT%H:%M:%S%z", proto.getTimestampTzFormat());
    assertEquals("%H:%M:%S", proto.getTimeFormat());
    assertEquals("NA", proto.getNullValue());
    assertEquals(6, proto.getCompressionLevel());
  }

  @Test
  void testCsvOptionsBuilderDefaults() {
    CsvOptions opts = CsvOptions.builder().build();
    byte[] encoded = opts.encodeOptions();
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    var proto =
        assertDoesNotThrow(() -> org.apache.arrow.datafusion.proto.CsvOptions.parseFrom(encoded));
    // Default: hasHeader = true
    assertEquals(1, proto.getHasHeader().byteAt(0));
    // Default: delimiter = ','
    assertEquals(',', proto.getDelimiter().byteAt(0));
  }

  // ── ParquetOptions builder and encodeOptions ──

  @Test
  void testParquetOptionsBuilderAllFields() {
    ParquetOptions opts =
        ParquetOptions.builder()
            .compression("SNAPPY")
            .writerVersion("2.0")
            .encoding("PLAIN")
            .statisticsEnabled("page")
            .createdBy("test-suite")
            .dictionaryEnabled(false)
            .bloomFilterOnWrite(true)
            .skipArrowMetadata(true)
            .allowSingleFileParallelism(false)
            .dataPagesizeLimit(2097152L)
            .writeBatchSize(512L)
            .dictionaryPageSizeLimit(524288L)
            .maxRowGroupSize(500000L)
            .dataPageRowCountLimit(10000L)
            .maximumParallelRowGroupWriters(2L)
            .maximumBufferedRecordBatchesPerStream(4L)
            .bloomFilterFpp(0.125)
            .bloomFilterNdv(1000000L)
            .build();

    byte[] encoded = opts.encodeOptions();
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    var tableProto =
        assertDoesNotThrow(
            () -> org.apache.arrow.datafusion.proto.TableParquetOptions.parseFrom(encoded));
    assertTrue(tableProto.hasGlobal());
    var global = tableProto.getGlobal();
    assertEquals("SNAPPY", global.getCompression());
    assertEquals("2.0", global.getWriterVersion());
    assertEquals("PLAIN", global.getEncoding());
    assertEquals("page", global.getStatisticsEnabled());
    assertEquals("test-suite", global.getCreatedBy());
    assertFalse(global.getDictionaryEnabled());
    assertTrue(global.getBloomFilterOnWrite());
    assertTrue(global.getSkipArrowMetadata());
    assertFalse(global.getAllowSingleFileParallelism());
    assertEquals(2097152L, global.getDataPagesizeLimit());
    assertEquals(512L, global.getWriteBatchSize());
    assertEquals(524288L, global.getDictionaryPageSizeLimit());
    assertEquals(500000L, global.getMaxRowGroupSize());
    assertEquals(10000L, global.getDataPageRowCountLimit());
    assertEquals(2L, global.getMaximumParallelRowGroupWriters());
    assertEquals(4L, global.getMaximumBufferedRecordBatchesPerStream());
    assertEquals(0.125, global.getBloomFilterFpp(), 0.001);
    assertEquals(1000000L, global.getBloomFilterNdv());
  }

  @Test
  void testParquetOptionsBuilderDefaults() {
    ParquetOptions opts = ParquetOptions.builder().build();
    byte[] encoded = opts.encodeOptions();
    assertNotNull(encoded);

    var tableProto =
        assertDoesNotThrow(
            () -> org.apache.arrow.datafusion.proto.TableParquetOptions.parseFrom(encoded));
    // No global options set when defaults are used
    assertFalse(tableProto.hasGlobal());
  }

  @Test
  void testParquetOptionsWithColumnOptions() {
    ParquetOptions opts =
        ParquetOptions.builder()
            .compression("ZSTD")
            .columnOptions(
                Map.of(
                    "col_a",
                    new ParquetOptions.ParquetColumnOptions(
                        "SNAPPY", "RLE", true, "page", true, 0.01, 500L)))
            .build();

    byte[] encoded = opts.encodeOptions();
    var tableProto =
        assertDoesNotThrow(
            () -> org.apache.arrow.datafusion.proto.TableParquetOptions.parseFrom(encoded));
    assertEquals(1, tableProto.getColumnSpecificOptionsCount());
    var colSpec = tableProto.getColumnSpecificOptions(0);
    assertEquals("col_a", colSpec.getColumnName());
    assertEquals("SNAPPY", colSpec.getOptions().getCompression());
    assertEquals("RLE", colSpec.getOptions().getEncoding());
    assertTrue(colSpec.getOptions().getDictionaryEnabled());
    assertEquals("page", colSpec.getOptions().getStatisticsEnabled());
    assertTrue(colSpec.getOptions().getBloomFilterEnabled());
    assertEquals(0.01, colSpec.getOptions().getBloomFilterFpp(), 0.001);
    assertEquals(500L, colSpec.getOptions().getBloomFilterNdv());
  }

  // ── CsvReadOptions builder and encodeOptions ──

  @Test
  void testCsvReadOptionsBuilderAllFields() {
    CsvReadOptions opts =
        CsvReadOptions.builder()
            .hasHeader(false)
            .delimiter((byte) '|')
            .quote((byte) '`')
            .terminator((byte) '\r')
            .escape((byte) '\\')
            .comment((byte) '#')
            .newlinesInValues(true)
            .schemaInferMaxRecords(500)
            .nullRegex("^(null|NULL|N/A)$")
            .truncatedRows(true)
            .build();

    assertNull(opts.schema());

    byte[] encoded = opts.encodeOptions();
    assertNotNull(encoded);
    assertTrue(encoded.length > 0);

    var proto =
        assertDoesNotThrow(() -> org.apache.arrow.datafusion.proto.CsvOptions.parseFrom(encoded));
    assertEquals(0, proto.getHasHeader().byteAt(0)); // false
    assertEquals('|', proto.getDelimiter().byteAt(0));
    assertEquals('`', proto.getQuote().byteAt(0));
    assertEquals('\r', proto.getTerminator().byteAt(0));
    assertEquals('\\', proto.getEscape().byteAt(0));
    assertEquals('#', proto.getComment().byteAt(0));
    assertEquals(1, proto.getNewlinesInValues().byteAt(0)); // true
    assertEquals(500, proto.getSchemaInferMaxRec());
    assertEquals("^(null|NULL|N/A)$", proto.getNullRegex());
    assertEquals(1, proto.getTruncatedRows().byteAt(0)); // true
  }
}
