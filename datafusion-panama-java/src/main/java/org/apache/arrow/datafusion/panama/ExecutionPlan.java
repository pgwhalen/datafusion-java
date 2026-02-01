package org.apache.arrow.datafusion.panama;

import java.lang.foreign.MemorySegment;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Java interface for DataFusion ExecutionPlan.
 * 
 * This interface represents a DataFusion ExecutionPlan that can be executed 
 * to produce Arrow data streams. ExecutionPlans are returned by TableProvider
 * scan operations and can be executed to get the actual data.
 */
public interface ExecutionPlan {
    
    /**
     * Execute this execution plan for a specific partition.
     * 
     * @param partition The partition number to execute (0-based)
     * @param allocator Arrow memory allocator for creating result data
     * @return MemorySegment pointing to ArrowStreamResult, or NULL on failure
     */
    MemorySegment execute(int partition, BufferAllocator allocator);
    
    /**
     * Get the name of this execution plan.
     * 
     * @return The name of the execution plan
     */
    String getName();
    
    /**
     * Get the schema of the data this execution plan will produce.
     * 
     * @return JSON representation of the Arrow schema
     */
    String getSchemaJson();
}