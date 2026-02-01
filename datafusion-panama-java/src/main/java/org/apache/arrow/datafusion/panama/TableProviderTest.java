package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Test class demonstrating Java TableProvider implementation.
 * 
 * This shows how to create a custom TableProvider in Java that can be
 * queried by DataFusion SQL queries.
 */
public class TableProviderTest {
    
    /**
     * ExecutionPlan implementation that produces data from the table
     */
    public static class SimpleTableExecutionPlan implements ExecutionPlan {
        private final String planName;
        private final int[] projections;
        private final String sqlFilter;
        private final long limit;
        private final List<String[]> data;
        private final String[] columnNames;
        private final String schemaJson;
        
        public SimpleTableExecutionPlan(String planName, int[] projections, String sqlFilter, long limit, 
                                      List<String[]> data, String[] columnNames) {
            this.planName = planName;
            this.projections = projections;
            this.sqlFilter = sqlFilter;
            this.limit = limit;
            this.data = data;
            this.columnNames = columnNames;
            // Simple schema - assuming name(string) and age(int32) - matching the actual Arrow schema created above
            this.schemaJson = "{\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"nullable\":false},{\"name\":\"age\",\"type\":\"int32\",\"nullable\":false}]}";
        }
        
        @Override
        public MemorySegment execute(int partition, BufferAllocator allocator) {
            System.out.println("🚀 SimpleTableExecutionPlan.execute() called for partition " + partition);
            System.out.println("   Plan: " + planName);
            
            try {
                // Create Arrow schema
                Schema schema = new Schema(List.of(
                    Field.nullable("name", new ArrowType.Utf8()),
                    Field.nullable("age", new ArrowType.Int(32, true))
                ));
                
                // Create VectorSchemaRoot
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                
                // Get vectors
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
                IntVector ageVector = (IntVector) root.getVector("age");
                
                // Apply limit
                int actualLimit = (limit == -1) ? data.size() : Math.min((int) limit, data.size());
                
                // Allocate and populate data
                root.allocateNew();
                for (int i = 0; i < actualLimit; i++) {
                    String[] row = data.get(i);
                    nameVector.set(i, row[0].getBytes());
                    ageVector.set(i, Integer.parseInt(row[1]));
                }
                root.setRowCount(actualLimit);
                
                System.out.println("✅ ExecutionPlan produced " + actualLimit + " rows");
                
                // Convert to ArrowArrayStream using ArrowStreamTest approach
                return ArrowStreamTest.createArrowStreamFromRoot(root, allocator);
                
            } catch (Exception e) {
                System.err.println("❌ Error in SimpleTableExecutionPlan.execute(): " + e.getMessage());
                e.printStackTrace();
                return MemorySegment.NULL;
            }
        }
        
        @Override
        public String getName() {
            return planName;
        }
        
        @Override
        public String getSchemaJson() {
            return schemaJson;
        }
    }
    
    /**
     * Simple in-memory TableProvider implementation for testing
     */
    public static class SimpleTableProvider implements TableProvider {
        private final String tableName;
        private final List<String[]> data;
        private final String[] columnNames;
        
        public SimpleTableProvider(String tableName, String[] columnNames, List<String[]> data) {
            this.tableName = tableName;
            this.columnNames = columnNames;
            this.data = data;
        }
        
        @Override
        public MemorySegment scan(
                MemorySegment sessionPtr,
                int[] projections,
                String sqlFilter,
                long limit,
                BufferAllocator allocator) {
            
            System.out.println("🔥 Java TableProvider.scan() called!");
            System.out.println("   Table: " + tableName);
            System.out.println("   Projections: " + (projections != null ? Arrays.toString(projections) : "ALL"));
            System.out.println("   Filter: " + (sqlFilter.isEmpty() ? "NONE" : sqlFilter));
            System.out.println("   Limit: " + (limit == -1 ? "NONE" : String.valueOf(limit)));
            
            // Apply limit if specified
            int actualLimit = (limit == -1) ? data.size() : Math.min((int) limit, data.size());
            System.out.println("✅ Creating ExecutionPlan that will generate " + actualLimit + " rows for table: " + tableName);
            
            System.out.println("📊 Sample data that ExecutionPlan will produce:");
            for (int i = 0; i < Math.min(3, actualLimit); i++) {
                String[] row = data.get(i);
                System.out.println("   Row " + i + ": name=" + row[0] + ", age=" + row[1]);
            }
            if (actualLimit > 3) {
                System.out.println("   ... and " + (actualLimit - 3) + " more rows");
            }
            
            // Create an ExecutionPlan that will produce the data when executed
            String planName = "SimpleTableScan[" + tableName + "]";
            ExecutionPlan executionPlan = new SimpleTableExecutionPlan(planName, projections, sqlFilter, limit, data, columnNames);
            
            // Create native ExecutionPlan wrapper
            MemorySegment nativeExecutionPlan = ExecutionPlanManager.createNativeExecutionPlan(executionPlan, allocator);
            
            if (nativeExecutionPlan.equals(MemorySegment.NULL)) {
                System.err.println("❌ Failed to create native ExecutionPlan");
                return MemorySegment.NULL;
            }
            
            System.out.println("✅ Created ExecutionPlan: " + planName);
            return nativeExecutionPlan;
        }
        
        @Override
        public String getTableName() {
            return tableName;
        }
    }
    
    /**
     * Test the TableProvider functionality
     */
    public static void testTableProvider(MemorySegment sessionCtx, BufferAllocator allocator) {
        System.out.println("🚀 Testing Java TableProvider Implementation");
        System.out.println("==========================================");
        
        if (sessionCtx.equals(MemorySegment.NULL)) {
            System.out.println("❌ No session context available");
            return;
        }
        
        try {
            // Create sample data
            List<String[]> sampleData = Arrays.asList(
                new String[]{"Alice", "25"},
                new String[]{"Bob", "30"},
                new String[]{"Charlie", "35"},
                new String[]{"Diana", "28"},
                new String[]{"Eve", "32"}
            );
            
            // Create our custom TableProvider
            SimpleTableProvider provider = new SimpleTableProvider(
                "people", 
                new String[]{"name", "age"}, 
                sampleData
            );
            
            System.out.println("✅ Created SimpleTableProvider with " + sampleData.size() + " rows");
            
            // Register the table provider with DataFusion
            boolean registered = TableProviderManager.registerTableProvider(
                sessionCtx, 
                provider, 
                "people",
                allocator
            );
            
            if (!registered) {
                System.out.println("❌ Failed to register TableProvider");
                return;
            }
            
            System.out.println("✅ Successfully registered 'people' table with DataFusion");
            System.out.println("");
            
            // Test querying the custom table
            System.out.println("🔍 Testing SQL queries against custom table:");
            System.out.println("--------------------------------------------");
            
            // Simple SELECT * query
            testTableQuery(sessionCtx, "SELECT * FROM people", allocator);
            
            // SELECT with projection
            testTableQuery(sessionCtx, "SELECT name FROM people", allocator);
            
            // SELECT with filter (if supported)
            testTableQuery(sessionCtx, "SELECT name, age FROM people WHERE age > 30", allocator);
            
            // SELECT with limit
            testTableQuery(sessionCtx, "SELECT * FROM people LIMIT 2", allocator);
            
            System.out.println("🎉 TableProvider test completed!");
            
        } catch (Exception e) {
            System.err.println("❌ TableProvider test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test a SQL query against the custom table
     */
    private static void testTableQuery(MemorySegment sessionCtx, String query, BufferAllocator allocator) {
        System.out.println("\n📝 Query: " + query);
        
        try {
            // Execute the query using the streaming API
            ArrowStreamTest.testArrowStreamAPIs(sessionCtx, query, allocator);
            
        } catch (Exception e) {
            System.err.println("❌ Query failed: " + e.getMessage());
        }
    }
    
    /**
     * Main method for standalone testing
     */
    public static void main(String[] args) {
        System.out.println("DataFusion Java TableProvider Test");
        System.out.println("==================================");
        
        try (BufferAllocator allocator = new RootAllocator()) {
            // Create DataFusion session context
            MemorySegment sessionCtx = DataFusionFFI.sessionContextNew();
            
            if (sessionCtx.equals(MemorySegment.NULL)) {
                System.err.println("Failed to create session context");
                return;
            }
            
            try {
                // Test TableProvider functionality
                testTableProvider(sessionCtx, allocator);
                
            } finally {
                // Clean up session context
                DataFusionFFI.sessionContextDestroy(sessionCtx);
            }
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}