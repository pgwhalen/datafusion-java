package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Hello World example for DataFusion Panama FFM bindings.
 * This demonstrates creating a DataFusion session context, executing SQL queries,
 * and reading the actual results via Arrow without copying data.
 */
public class DataFusionHelloWorld {
    
    public static void main(String[] args) {
        System.out.println("DataFusion Panama FFM Hello World");
        System.out.println("===================================");
        
        // Get version information
        try {
            String version = DataFusionFFI.version();
            System.out.println("Library version: " + version);
        } catch (Exception e) {
            System.err.println("Failed to get version: " + e.getMessage());
            return;
        }
        
        // Create a new DataFusion session context
        MemorySegment sessionCtx = null;
        try {
            System.out.println("Creating DataFusion session context...");
            sessionCtx = DataFusionFFI.sessionContextNew();
            
            if (sessionCtx.equals(MemorySegment.NULL)) {
                System.err.println("Failed to create session context");
                String lastError = DataFusionFFI.getLastError();
                if (lastError != null) {
                    System.err.println("Error: " + lastError);
                }
                return;
            }
            
            System.out.println("Session context created successfully!");
            
            // Create Arrow allocator for reading results
            try (BufferAllocator allocator = new RootAllocator()) {
                System.out.println("\n📋 Comparison with old batch-by-batch approach:");
                System.out.println("================================================================");
                
                MemorySegment arrowResult = DataFusionFFI.sessionContextSqlArrow(sessionCtx, "SELECT 'comparison' as test, 123 as number");
                if (!arrowResult.equals(MemorySegment.NULL)) {
                    System.out.println("🔄 Old approach (batch-by-batch):");
                    ArrowCDataTest.testArrowCDataAPIs(arrowResult, allocator);
                    DataFusionFFI.arrowResultFree(arrowResult);
                } else {
                    System.out.println(" sessionContextSqlArrow is null");
                }
                
                System.out.println("\n🔥 NEW: Testing Java TableProvider Implementation");
                System.out.println("================================================================");
                
                // Test the new TableProvider functionality
                TableProviderTest.testTableProvider(sessionCtx, allocator);
            }
            
        } catch (Exception e) {
            System.err.println("Error during execution: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up the session context
            if (sessionCtx != null && !sessionCtx.equals(MemorySegment.NULL)) {
                System.out.println("Destroying session context...");
                try {
                    DataFusionFFI.sessionContextDestroy(sessionCtx);
                    System.out.println("Session context destroyed successfully!");
                } catch (Exception e) {
                    System.err.println("Error destroying session context: " + e.getMessage());
                }
            }
        }
        
        System.out.println("DataFusion Panama FFM Hello World completed!");
    }
}