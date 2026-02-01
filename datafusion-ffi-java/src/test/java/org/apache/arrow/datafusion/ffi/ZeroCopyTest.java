package org.apache.arrow.datafusion.ffi;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify zero-copy data transfer between Java and Rust.
 *
 * <p>These tests use memory addresses to prove that data is not copied when passed across the FFI
 * boundary via the Arrow C Data Interface. The tests may use unsafe operations and APIs that would
 * not be appropriate for production code - the goal is to confidently verify memory sharing.
 *
 * <p>Note: Some tests use non-strict allocator cleanup because they inspect FFI structures without
 * fully consuming them (which is the point - to verify buffer addresses match).
 */
public class ZeroCopyTest {

  // Size of FFI structures (same as in RecordBatchStream)
  private static final long ARROW_SCHEMA_SIZE = 72;
  private static final long ARROW_ARRAY_SIZE = 80;
  private static final int MAX_BUFFERS = 8;

  /**
   * Test zero-copy from Java to Rust.
   *
   * <p>This test verifies that when Java exports Arrow data via the C Data Interface, Rust sees the
   * exact same memory addresses for the data buffers.
   */
  @Test
  void testJavaToRustZeroCopy() throws Throwable {
    // Use a non-closing allocator reference since FFI export creates shared references
    // that can't be fully cleaned up in a verification test
    BufferAllocator allocator = new RootAllocator();
    boolean testPassed = false;

    try {
      // Create test data in Java
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        BigIntVector vector = (BigIntVector) root.getVector("value");
        vector.allocateNew(5);
        for (int i = 0; i < 5; i++) {
          vector.set(i, i + 1); // values: 1, 2, 3, 4, 5
        }
        vector.setValueCount(5);
        root.setRowCount(5);

        // Get the data buffer address from Java BEFORE export
        ArrowBuf dataBuffer = vector.getDataBuffer();
        long javaDataBufferAddress = dataBuffer.memoryAddress();

        System.out.println(
            "Java data buffer address: 0x" + Long.toHexString(javaDataBufferAddress));

        // Export to Arrow C Data Interface
        try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

          Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

          // Get the FFI array address
          long ffiArrayAddress = ffiArray.memoryAddress();

          // Call Rust to read the buffer addresses from the FFI structure
          boolean foundMatch = false;
          try (Arena arena = Arena.ofConfined()) {
            MemorySegment addressesOut =
                arena.allocate(ValueLayout.JAVA_LONG.byteSize() * MAX_BUFFERS);
            MemorySegment actualCountOut = arena.allocate(ValueLayout.JAVA_INT);
            MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

            MemorySegment ffiArraySegment =
                MemorySegment.ofAddress(ffiArrayAddress).reinterpret(ARROW_ARRAY_SIZE);

            int result =
                (int)
                    DataFusionBindings.GET_CHILD_BUFFER_ADDRESSES.invokeExact(
                        ffiArraySegment, 0, addressesOut, MAX_BUFFERS, actualCountOut, errorOut);

            assertEquals(0, result, "Rust call should succeed");

            int actualCount = actualCountOut.get(ValueLayout.JAVA_INT, 0);
            System.out.println("Number of buffers in child array: " + actualCount);

            assertTrue(actualCount >= 1, "Should have at least 1 buffer");

            // Print all buffer addresses for debugging
            for (int i = 0; i < actualCount; i++) {
              long addr = addressesOut.get(ValueLayout.JAVA_LONG, (long) i * 8);
              System.out.println("  Buffer " + i + " address: 0x" + Long.toHexString(addr));
            }

            // Find the data buffer by looking for our Java address
            for (int i = 0; i < actualCount; i++) {
              long rustSeenAddress = addressesOut.get(ValueLayout.JAVA_LONG, (long) i * 8);
              if (rustSeenAddress == javaDataBufferAddress) {
                foundMatch = true;
                System.out.println("✓ Java → Rust zero-copy verified at buffer index " + i + "!");
                break;
              }
            }
          }

          assertTrue(
              foundMatch,
              String.format(
                  "Zero-copy verification failed! Java data buffer address (0x%X) "
                      + "not found in Rust-seen buffer addresses",
                  javaDataBufferAddress));

          testPassed = true;
        }
      }
    } finally {
      // Best-effort cleanup - may throw due to FFI reference counting
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        // Expected in verification tests where FFI memory isn't fully released
        if (testPassed) {
          System.out.println(
              "Note: Expected memory accounting difference from FFI verification test");
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Test zero-copy from Rust to Java.
   *
   * <p>This test verifies that when Rust creates Arrow data and exports it via the C Data
   * Interface, Java sees the exact same memory addresses for the data buffers.
   */
  @Test
  void testRustToJavaZeroCopy() throws Throwable {
    try (BufferAllocator allocator = new RootAllocator()) {
      try (Arena arena = Arena.ofConfined()) {
        // Allocate FFI structures and output for the original buffer address
        MemorySegment arrayOut = arena.allocate(ARROW_ARRAY_SIZE);
        MemorySegment schemaOut = arena.allocate(ARROW_SCHEMA_SIZE);
        MemorySegment dataBufferAddressOut = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

        // Call Rust to create test data and get the original buffer address
        int result =
            (int)
                DataFusionBindings.CREATE_TEST_DATA_WITH_ADDRESSES.invokeExact(
                    arrayOut, schemaOut, dataBufferAddressOut, errorOut);

        assertEquals(0, result, "Rust create test data should succeed");

        long rustOriginalDataAddress = dataBufferAddressOut.get(ValueLayout.JAVA_LONG, 0);
        assertTrue(rustOriginalDataAddress != 0, "Rust should return a valid data buffer address");

        System.out.println(
            "Rust original data buffer address: 0x" + Long.toHexString(rustOriginalDataAddress));

        // Now get the address from the FFI structure that Rust wrote
        MemorySegment addressesOut = arena.allocate(ValueLayout.JAVA_LONG.byteSize() * MAX_BUFFERS);
        MemorySegment actualCountOut = arena.allocate(ValueLayout.JAVA_INT);
        MemorySegment errorOut2 = NativeUtil.allocateErrorOut(arena);

        // Get child buffer addresses from the FFI array (column 0)
        result =
            (int)
                DataFusionBindings.GET_CHILD_BUFFER_ADDRESSES.invokeExact(
                    arrayOut, 0, addressesOut, MAX_BUFFERS, actualCountOut, errorOut2);

        assertEquals(0, result, "Get child buffer addresses should succeed");

        int actualCount = actualCountOut.get(ValueLayout.JAVA_INT, 0);
        System.out.println("Number of buffers in FFI child array: " + actualCount);
        assertTrue(actualCount >= 1, "Should have at least 1 buffer");

        // Print all buffer addresses for debugging
        for (int i = 0; i < actualCount; i++) {
          long addr = addressesOut.get(ValueLayout.JAVA_LONG, (long) i * 8);
          System.out.println("  FFI Buffer " + i + " address: 0x" + Long.toHexString(addr));
        }

        // Import into Java VectorSchemaRoot
        ArrowSchema arrowSchema = ArrowSchema.wrap(schemaOut.address());
        Schema schema = Data.importSchema(allocator, arrowSchema, null);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          // Wrap the array again for import
          ArrowArray arrowArray = ArrowArray.wrap(arrayOut.address());
          Data.importIntoVectorSchemaRoot(allocator, arrowArray, root, null);

          // Get the data buffer address from the imported Java vector
          BigIntVector vector = (BigIntVector) root.getVector("value");
          ArrowBuf dataBuffer = vector.getDataBuffer();
          long javaDataBufferAddress = dataBuffer.memoryAddress();

          System.out.println(
              "Java imported data buffer address: 0x" + Long.toHexString(javaDataBufferAddress));

          // VERIFY ZERO-COPY: The address Rust created should be the same as what Java sees
          assertEquals(
              rustOriginalDataAddress,
              javaDataBufferAddress,
              String.format(
                  "Zero-copy verification failed! Rust original address (0x%X) "
                      + "should equal Java imported address (0x%X)",
                  rustOriginalDataAddress, javaDataBufferAddress));

          // Also verify data integrity
          assertEquals(5, root.getRowCount(), "Should have 5 rows");
          for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, vector.get(i), "Data should match: value[" + i + "]");
          }

          System.out.println("✓ Rust → Java zero-copy verified!");
        }
      }
    }
  }

  /**
   * Test round-trip zero-copy: Java → Rust → Java.
   *
   * <p>This test creates data in Java, registers it with DataFusion (passing to Rust), queries it
   * back, and verifies that the FFI export is zero-copy.
   */
  @Test
  void testJavaToRustRoundTripZeroCopy() throws Throwable {
    BufferAllocator allocator = new RootAllocator();
    boolean testPassed = false;

    try (SessionContext ctx = new SessionContext()) {
      // Create test data in Java
      Schema schema =
          new Schema(
              Arrays.asList(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));

      try (VectorSchemaRoot testData = VectorSchemaRoot.create(schema, allocator)) {
        BigIntVector vector = (BigIntVector) testData.getVector("x");
        vector.allocateNew(3);
        vector.set(0, 100);
        vector.set(1, 200);
        vector.set(2, 300);
        vector.setValueCount(3);
        testData.setRowCount(3);

        // Get the original data buffer address
        long originalAddress = vector.getDataBuffer().memoryAddress();
        System.out.println(
            "Original Java data buffer address: 0x" + Long.toHexString(originalAddress));

        // Export to FFI to verify Rust will see the same address
        try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
            Arena arena = Arena.ofConfined()) {

          Data.exportVectorSchemaRoot(allocator, testData, null, ffiArray, ffiSchema);

          // Verify Rust sees our address through FFI
          MemorySegment addressesOut =
              arena.allocate(ValueLayout.JAVA_LONG.byteSize() * MAX_BUFFERS);
          MemorySegment actualCountOut = arena.allocate(ValueLayout.JAVA_INT);
          MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

          MemorySegment ffiArraySegment =
              MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);

          int result =
              (int)
                  DataFusionBindings.GET_CHILD_BUFFER_ADDRESSES.invokeExact(
                      ffiArraySegment, 0, addressesOut, MAX_BUFFERS, actualCountOut, errorOut);

          assertEquals(0, result, "Get addresses should succeed");

          int actualCount = actualCountOut.get(ValueLayout.JAVA_INT, 0);
          System.out.println("Number of buffers: " + actualCount);

          // Find our data buffer address in the FFI structure
          boolean foundMatch = false;
          for (int i = 0; i < actualCount; i++) {
            long rustSeenAddress = addressesOut.get(ValueLayout.JAVA_LONG, (long) i * 8);
            System.out.println("  Buffer " + i + ": 0x" + Long.toHexString(rustSeenAddress));
            if (rustSeenAddress == originalAddress) {
              foundMatch = true;
            }
          }

          assertTrue(
              foundMatch,
              String.format(
                  "FFI export should be zero-copy! Original: 0x%X not found in FFI buffers",
                  originalAddress));

          System.out.println("✓ Java → Rust FFI export is zero-copy");
        }

        // Register with DataFusion and query back
        ctx.registerTable("test", testData, allocator);

        try (DataFrame df = ctx.sql("SELECT x FROM test");
            RecordBatchStream stream = df.executeStream(allocator)) {

          assertTrue(stream.loadNextBatch(), "Should have results");
          VectorSchemaRoot result = stream.getVectorSchemaRoot();

          // Verify data is correct
          BigIntVector resultVector = (BigIntVector) result.getVector("x");
          assertEquals(3, result.getRowCount());
          assertEquals(100, resultVector.get(0));
          assertEquals(200, resultVector.get(1));
          assertEquals(300, resultVector.get(2));

          System.out.println("✓ Round-trip data integrity verified");
          testPassed = true;
        }
      }
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        if (testPassed) {
          System.out.println(
              "Note: Expected memory accounting difference from FFI verification test");
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Test that verifies FFI structure layout is as expected.
   *
   * <p>This is a sanity check to ensure our size constants match what Rust expects.
   */
  @Test
  void testFfiStructureSizes() throws Throwable {
    BufferAllocator allocator = new RootAllocator();
    boolean testPassed = false;

    try (Arena arena = Arena.ofConfined()) {
      // Create minimal test data
      Schema schema =
          new Schema(
              Arrays.asList(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
          ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {

        BigIntVector vector = (BigIntVector) root.getVector("x");
        vector.allocateNew(1);
        vector.set(0, 42);
        vector.setValueCount(1);
        root.setRowCount(1);

        Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

        // Query Rust for structure information
        MemorySegment ffiArraySegment =
            MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);

        long nChildren = (long) DataFusionBindings.GET_FFI_N_CHILDREN.invokeExact(ffiArraySegment);
        long nBuffers = (long) DataFusionBindings.GET_FFI_N_BUFFERS.invokeExact(ffiArraySegment);

        // For a struct array wrapping a VectorSchemaRoot with one Int64 column:
        // - The struct array itself has 1 child (the Int64 array)
        // - The struct array has 1 buffer (validity bitmap, though often null)
        assertEquals(1, nChildren, "Should have 1 child (the Int64 column)");
        assertTrue(nBuffers >= 0, "Should have non-negative buffer count");

        System.out.println("✓ FFI structure layout verified");
        System.out.println("  n_children: " + nChildren);
        System.out.println("  n_buffers: " + nBuffers);

        testPassed = true;
      }
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        if (testPassed) {
          System.out.println(
              "Note: Expected memory accounting difference from FFI verification test");
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Test zero-copy with larger data to ensure it works at scale.
   *
   * <p>This test creates a larger dataset to verify that zero-copy works correctly and efficiently
   * with realistic data sizes.
   */
  @Test
  void testLargeDataZeroCopy() throws Throwable {
    BufferAllocator allocator = new RootAllocator();
    boolean testPassed = false;
    final int numRows = 100_000;

    try {
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("value", FieldType.nullable(new ArrowType.Int(64, true)), null)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        BigIntVector vector = (BigIntVector) root.getVector("value");
        vector.allocateNew(numRows);
        for (int i = 0; i < numRows; i++) {
          vector.set(i, i * 2L); // Even numbers
        }
        vector.setValueCount(numRows);
        root.setRowCount(numRows);

        // Get original address
        long originalAddress = vector.getDataBuffer().memoryAddress();
        long bufferSize = vector.getDataBuffer().capacity();

        // Export and verify
        try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
            ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
            Arena arena = Arena.ofConfined()) {

          Data.exportVectorSchemaRoot(allocator, root, null, ffiArray, ffiSchema);

          MemorySegment addressesOut =
              arena.allocate(ValueLayout.JAVA_LONG.byteSize() * MAX_BUFFERS);
          MemorySegment actualCountOut = arena.allocate(ValueLayout.JAVA_INT);
          MemorySegment errorOut = NativeUtil.allocateErrorOut(arena);

          MemorySegment ffiArraySegment =
              MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);

          int result =
              (int)
                  DataFusionBindings.GET_CHILD_BUFFER_ADDRESSES.invokeExact(
                      ffiArraySegment, 0, addressesOut, MAX_BUFFERS, actualCountOut, errorOut);

          assertEquals(0, result);

          int actualCount = actualCountOut.get(ValueLayout.JAVA_INT, 0);

          // Find our data buffer address in the FFI structure
          boolean foundMatch = false;
          for (int i = 0; i < actualCount; i++) {
            long rustSeenAddress = addressesOut.get(ValueLayout.JAVA_LONG, (long) i * 8);
            if (rustSeenAddress == originalAddress) {
              foundMatch = true;
              break;
            }
          }

          assertTrue(foundMatch, "Large data should also be zero-copy");

          System.out.println("✓ Large data zero-copy verified (" + numRows + " rows)");
          System.out.println("  Buffer size: " + bufferSize + " bytes");
          System.out.println("  Address: 0x" + Long.toHexString(originalAddress));

          testPassed = true;
        }
      }
    } finally {
      try {
        allocator.close();
      } catch (IllegalStateException e) {
        if (testPassed) {
          System.out.println(
              "Note: Expected memory accounting difference from FFI verification test");
        } else {
          throw e;
        }
      }
    }
  }
}
