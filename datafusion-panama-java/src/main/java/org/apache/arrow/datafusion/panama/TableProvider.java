package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Java interface for implementing custom DataFusion TableProviders.
 * 
 * This interface allows Java code to provide custom table implementations
 * that can be queried by DataFusion SQL. When DataFusion needs to scan
 * the table, it will call the scan() method with the appropriate parameters.
 */
public interface TableProvider {
    
    /**
     * Perform a scan on the table to create an ExecutionPlan.
     * 
     * This method is called by DataFusion when it needs to plan how to read data from the table.
     * The implementation should return an ExecutionPlan that, when executed, will produce the requested data.
     * This follows the proper DataFusion architecture: TableProvider.scan() returns ExecutionPlan,
     * then ExecutionPlan.execute() returns the actual data stream.
     * 
     * @param sessionPtr Pointer to the DataFusion session context (currently unused)
     * @param projections Array of column indices to project, or null for all columns
     * @param sqlFilter SQL-like filter expression as a string, or empty for no filter
     * @param limit Maximum number of rows to return, or -1 for no limit
     * @param allocator Arrow memory allocator for creating result data
     * @return MemorySegment pointer to JavaExecutionPlan, or null on failure
     */
    MemorySegment scan(
        MemorySegment sessionPtr,
        int[] projections,
        String sqlFilter,
        long limit,
        BufferAllocator allocator
    );
    
    /**
     * Get the name of this table provider for debugging/logging purposes.
     * @return table name
     */
    default String getTableName() {
        return "JavaTableProvider";
    }
}