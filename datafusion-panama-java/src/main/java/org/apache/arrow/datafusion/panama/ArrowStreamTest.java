package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import java.util.List;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Test class for Arrow C Data Interface streaming using Data.importArrayStream
 */
public class ArrowStreamTest {
    
    public static void testArrowStreamAPIs(MemorySegment sessionCtx, String query, BufferAllocator allocator) {
        System.out.println("🔄 Testing Arrow Stream APIs with Data.importArrayStream...");
        System.out.println("Query: " + query);

        if (sessionCtx.equals(MemorySegment.NULL)) {
            System.out.println("No session context available");
            return;
        }
        
        try {
            // Execute the SQL query and get ArrowStreamResult
            MemorySegment streamResult = DataFusionFFI.sessionContextSqlArrowStream(sessionCtx, query);
            
            if (streamResult.equals(MemorySegment.NULL)) {
                System.out.println("❌ Failed to execute query or no results");
                return;
            }
            
            System.out.println("✅ SQL query executed successfully, got stream result");
            
            // Get the ArrowArrayStream pointer from the result
            MemorySegment streamPtr = DataFusionFFI.arrowStreamGetStream(streamResult);
            
            if (streamPtr.equals(MemorySegment.NULL)) {
                System.out.println("❌ Failed to get stream pointer");
                DataFusionFFI.arrowStreamFree(streamResult);
                return;
            }
            
            System.out.println("✅ ArrowArrayStream pointer obtained: " + streamPtr.address());
            
            // Wrap the native stream pointer with ArrowArrayStream
            ArrowArrayStream arrowStream = ArrowArrayStream.wrap(streamPtr.address());
            System.out.println("✅ ArrowArrayStream wrapped from native pointer");
            
            // Import the stream using Data.importArrayStream
            try (ArrowReader reader = Data.importArrayStream(allocator, arrowStream)) {
                System.out.println("✅ ArrowReader created from importArrayStream!");
                
                // Iterate through the stream data
                int batchCount = 0;
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    System.out.println("✅ Loaded batch " + batchCount);
                    System.out.println("   Row count: " + root.getRowCount());
                    System.out.println("   Schema: " + root.getSchema());
                    
                    // Print actual data values from the query
                    printVectorSchemaRoot(root, batchCount);
                    batchCount++;
                }
                
                System.out.println("✅ Successfully processed " + batchCount + " batch(es) from stream");
                
            } catch (Exception e) {
                System.out.println("❌ Stream import failed: " + e.getMessage());
                e.printStackTrace();
            }
            
            // Clean up the stream result
            DataFusionFFI.arrowStreamFree(streamResult);
            
        } catch (Exception e) {
            System.out.println("❌ Arrow Stream test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void printVectorSchemaRoot(VectorSchemaRoot root, int batchIndex) {
        System.out.println("\n📊 Actual Stream Results (Batch " + batchIndex + "):");
        System.out.println("========================================");
        
        // Print column headers
        for (int col = 0; col < root.getFieldVectors().size(); col++) {
            System.out.print(root.getVector(col).getField().getName());
            if (col < root.getFieldVectors().size() - 1) {
                System.out.print("\t");
            }
        }
        System.out.println();
        
        // Print separator
        for (int col = 0; col < root.getFieldVectors().size(); col++) {
            System.out.print("--------");
            if (col < root.getFieldVectors().size() - 1) {
                System.out.print("\t");
            }
        }
        System.out.println();
        
        // Print data rows
        for (int row = 0; row < root.getRowCount(); row++) {
            for (int col = 0; col < root.getFieldVectors().size(); col++) {
                var vector = root.getVector(col);
                Object value = vector.getObject(row);
                System.out.print(value != null ? value.toString() : "null");
                if (col < root.getFieldVectors().size() - 1) {
                    System.out.print("\t");
                }
            }
            System.out.println();
        }
        System.out.println();
    }
    
    /**
     * Helper method to create an ArrowStreamResult from a VectorSchemaRoot.
     * This is used by ExecutionPlan implementations to return data as streams.
     * 
     * This creates a proper ArrowStreamResult that matches the Rust structure
     * and can be consumed by the DataFusion engine.
     */
    public static MemorySegment createArrowStreamFromRoot(VectorSchemaRoot root, BufferAllocator allocator) {
        try {
            System.out.println("🔄 Creating ArrowStreamResult from VectorSchemaRoot with " + root.getRowCount() + " rows");
            
            // Print the actual data for demonstration
            System.out.println("📊 Original Java data to convert:");
            printVectorSchemaRoot(root, 0);
            
            // Create a ByteArrayOutputStream to hold the Arrow stream data
            System.out.println("🔄 Serializing VectorSchemaRoot to Arrow stream format...");
            java.io.ByteArrayOutputStream byteStream = new java.io.ByteArrayOutputStream();
            
            // Use ArrowStreamWriter to serialize the VectorSchemaRoot
            try (org.apache.arrow.vector.ipc.ArrowStreamWriter writer = 
                    new org.apache.arrow.vector.ipc.ArrowStreamWriter(root, null, byteStream)) {
                
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            
            byte[] streamData = byteStream.toByteArray();
            System.out.println("✅ Serialized " + streamData.length + " bytes of Arrow stream data");
            
            // Create ArrowStreamReader from the serialized data
            java.io.ByteArrayInputStream inputStream = new java.io.ByteArrayInputStream(streamData);
            
            try (ArrowReader reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(inputStream, allocator)) {
                // Create an ArrowArrayStream and export the reader to it
                System.out.println("🔄 Exporting ArrowReader to ArrowArrayStream...");
                ArrowArrayStream arrowStream = ArrowArrayStream.allocateNew(allocator);
                Data.exportArrayStream(allocator, reader, arrowStream);
                
                System.out.println("✅ Exported to ArrowArrayStream: " + arrowStream.memoryAddress());
                
                // Wrap the ArrowArrayStream pointer in a MemorySegment for native FFI
                MemorySegment streamPtr = MemorySegment.ofAddress(arrowStream.memoryAddress());
                
                // Create the ArrowStreamResult using the native function
                System.out.println("🔄 Creating ArrowStreamResult from ArrowArrayStream...");
                MemorySegment streamResult = DataFusionFFI.createArrowStreamResult(streamPtr);
                
                if (streamResult.equals(MemorySegment.NULL)) {
                    System.err.println("❌ Failed to create ArrowStreamResult from ArrowArrayStream");
                    try {
                        arrowStream.close();
                    } catch (Exception closeEx) {
                        System.err.println("⚠️ Failed to close ArrowArrayStream: " + closeEx.getMessage());
                    }
                    return MemorySegment.NULL;
                }
                
                System.out.println("✅ Created ArrowStreamResult pointer: " + streamResult.address());
                System.out.println("✅ Successfully converted actual VectorSchemaRoot data to ArrowStreamResult");
                
                return streamResult;
            }
            
        } catch (Exception e) {
            System.err.println("❌ Failed to create Arrow stream from VectorSchemaRoot: " + e.getMessage());
            e.printStackTrace();
            return MemorySegment.NULL;
        }
    }
}