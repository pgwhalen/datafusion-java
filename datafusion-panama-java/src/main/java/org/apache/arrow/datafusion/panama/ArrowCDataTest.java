package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Test class to understand Arrow C Data Interface APIs
 */
public class ArrowCDataTest {
    
    public static void testArrowCDataAPIs(MemorySegment arrowResult, BufferAllocator allocator) {
        System.out.println("🔍 Testing Arrow C Data Interface APIs with ArrowArrayStream...");
        
        if (arrowResult.equals(MemorySegment.NULL)) {
            System.out.println("No data to test");
            return;
        }
        
        try {
            // Get schema and batch information
            MemorySegment schemaPtr = DataFusionFFI.arrowResultGetSchema(arrowResult);
            int numBatches = DataFusionFFI.arrowResultGetNumBatches(arrowResult);
            
            System.out.println("Schema pointer: " + schemaPtr.address());
            System.out.println("Number of batches: " + numBatches);
            
            if (!schemaPtr.equals(MemorySegment.NULL) && numBatches > 0) {
                System.out.println("✅ ArrowSchema pointer available");
                
                // Process each batch - import schema and data together
                for (int i = 0; i < numBatches; i++) {
                    MemorySegment batchPtr = DataFusionFFI.arrowResultGetBatch(arrowResult, i);
                    if (!batchPtr.equals(MemorySegment.NULL)) {
                        System.out.println("Processing batch " + i + "...");
                        
                        try {
                            // Create fresh schema copy for this batch to handle C Data Interface ownership
                            MemorySegment schemaCopyPtr = DataFusionFFI.arrowResultCreateSchemaCopy(arrowResult);
                            ArrowSchema batchSchema = ArrowSchema.wrap(schemaCopyPtr.address());
                            ArrowArray arrowArray = ArrowArray.wrap(batchPtr.address());
                            
                            System.out.println("✅ Fresh ArrowSchema copy and ArrowArray created for batch " + i);
                            
                            // Import the complete VectorSchemaRoot directly
                            // This consumes both schema and array in one operation
                            try (VectorSchemaRoot root = Data.importVectorSchemaRoot(
                                allocator, arrowArray, batchSchema, null)) {
                                
                                System.out.println("✅ VectorSchemaRoot imported for batch " + i + "!");
                                System.out.println("   Row count: " + root.getRowCount());
                                System.out.println("   Schema: " + root.getSchema());
                                
                                // Print actual data values from the query
                                printVectorSchemaRoot(root, i);
                            }
                            
                            // Free the schema copy (the ArrowSchema wrapper will be auto-released)
                            DataFusionFFI.freeSchemaCopy(schemaCopyPtr);
                            
                        } catch (Exception e) {
                            System.out.println("❌ Batch " + i + " import failed: " + e.getMessage());
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            System.out.println("❌ Arrow C Data test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void printVectorSchemaRoot(VectorSchemaRoot root, int batchIndex) {
        System.out.println("\n📊 printVectorSchemaRoot (Batch " + batchIndex + "):");
        System.out.println("=====================================");
        
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
}