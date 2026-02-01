package org.apache.arrow.datafusion.panama;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Manages Java ExecutionPlan implementations and their native callbacks.
 * 
 * This class handles the FFI bridge between Java ExecutionPlan implementations
 * and the native Rust code, managing callbacks and memory.
 */
public class ExecutionPlanManager {
    
    // Store Java ExecutionPlan instances by ID to prevent GC
    private static final ConcurrentHashMap<Long, ExecutionPlanWrapper> EXECUTION_PLANS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_ID = new AtomicLong(1);
    
    // Global arena to keep callback memory alive - this is the fix for the crash!
    private static final Arena GLOBAL_ARENA = Arena.ofShared();
    
    // Native callback function handle
    private static final MethodHandle EXECUTE_CALLBACK;
    
    static {
        try {
            // Create the native callback method handle
            EXECUTE_CALLBACK = MethodHandles.lookup().findStatic(
                ExecutionPlanManager.class,
                "nativeExecuteCallback",
                MethodType.methodType(MemorySegment.class, 
                    MemorySegment.class,  // user_data
                    int.class             // partition
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ExecutionPlanManager", e);
        }
    }
    
    /**
     * Wrapper to hold ExecutionPlan with its allocator and ID
     */
    private static class ExecutionPlanWrapper {
        final ExecutionPlan plan;
        final BufferAllocator allocator;
        final long id;
        
        ExecutionPlanWrapper(ExecutionPlan plan, BufferAllocator allocator, long id) {
            this.plan = plan;
            this.allocator = allocator;
            this.id = id;
        }
    }
    
    /**
     * Create a native ExecutionPlan wrapper for a Java ExecutionPlan.
     * 
     * @param plan Java ExecutionPlan implementation
     * @param allocator Arrow memory allocator
     * @return MemorySegment pointing to native JavaExecutionPlan, or NULL on failure
     */
    public static MemorySegment createNativeExecutionPlan(
            ExecutionPlan plan, 
            BufferAllocator allocator) {
        
        if (plan == null || allocator == null) {
            return MemorySegment.NULL;
        }
        
        // Generate unique ID for this execution plan
        long planId = NEXT_ID.getAndIncrement();
        
        // Store the plan to prevent GC
        EXECUTION_PLANS.put(planId, new ExecutionPlanWrapper(plan, allocator, planId));
        
        // Create user_data pointer containing the plan ID - use global arena to keep alive
        MemorySegment userDataSegment = GLOBAL_ARENA.allocate(ValueLayout.JAVA_LONG);
        userDataSegment.set(ValueLayout.JAVA_LONG, 0, planId);
        
        // Create callback function descriptor
        FunctionDescriptor callbackDesc = FunctionDescriptor.of(
            ValueLayout.ADDRESS,  // return MemorySegment
            ValueLayout.ADDRESS,  // user_data
            ValueLayout.JAVA_INT  // partition
        );
        
        // Create upcall stub for the callback - use global arena to keep alive!
        MemorySegment callback = Linker.nativeLinker().upcallStub(
            EXECUTE_CALLBACK,
            callbackDesc,
            GLOBAL_ARENA  // This is the key fix - don't auto-close the arena!
        );
        
        // Create plan name and schema JSON - use global arena
        String planName = plan.getName();
        String schemaJson = plan.getSchemaJson();
        MemorySegment planNameSegment = GLOBAL_ARENA.allocateUtf8String(planName);
        MemorySegment schemaSegment = GLOBAL_ARENA.allocateUtf8String(schemaJson);
        
        try {
            
            // Create the native execution plan
            MemorySegment nativePlan = DataFusionFFI.createJavaExecutionPlan(
                userDataSegment, callback, planNameSegment, schemaSegment
            );
            
            if (nativePlan == null || nativePlan.equals(MemorySegment.NULL)) {
                EXECUTION_PLANS.remove(planId);
                return MemorySegment.NULL;
            }
            
            return nativePlan;
            
        } catch (Exception e) {
            System.err.println("Failed to create native execution plan: " + e.getMessage());
            e.printStackTrace();
            return MemorySegment.NULL;
        }
    }
    
    /**
     * Native callback function called from Rust when execute() is invoked.
     * This method bridges the native call to the Java ExecutionPlan.
     */
    public static MemorySegment nativeExecuteCallback(
            MemorySegment userData,
            int partition) {
        
        try {
            // Fix the userData segment size issue - reinterpret with correct size
            MemorySegment fixedUserData = userData.reinterpret(ValueLayout.JAVA_LONG.byteSize());
            
            // Extract plan ID from user_data
            long planId = fixedUserData.get(ValueLayout.JAVA_LONG, 0);
            
            // Get the plan wrapper
            ExecutionPlanWrapper wrapper = EXECUTION_PLANS.get(planId);
            if (wrapper == null) {
                System.err.println("ExecutionPlan not found for ID: " + planId);
                return MemorySegment.NULL;
            }
            
            // Call the Java ExecutionPlan execute method
            MemorySegment result = wrapper.plan.execute(partition, wrapper.allocator);
            
            return result != null ? result : MemorySegment.NULL;
            
        } catch (Exception e) {
            System.err.println("Error in native execute callback: " + e.getMessage());
            e.printStackTrace();
            return MemorySegment.NULL;
        }
    }
    
    /**
     * Clean up resources for an execution plan
     */
    public static void cleanup(long planId) {
        EXECUTION_PLANS.remove(planId);
    }
}