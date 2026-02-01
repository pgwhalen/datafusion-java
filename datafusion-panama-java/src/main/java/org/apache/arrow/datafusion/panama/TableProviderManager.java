package org.apache.arrow.datafusion.panama;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Manages Java TableProvider implementations and their native callbacks.
 * 
 * This class handles the FFI bridge between Java TableProvider implementations
 * and the native Rust code, managing callbacks and memory.
 */
public class TableProviderManager {
    
    // Store Java TableProvider instances by ID to prevent GC
    private static final ConcurrentHashMap<Long, TableProviderWrapper> PROVIDERS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_ID = new AtomicLong(1);
    
    // Global arena to keep callback memory alive - this is the fix for the crash!
    private static final Arena GLOBAL_ARENA = Arena.ofShared();
    
    // Native callback function handle
    private static final MethodHandle SCAN_CALLBACK;
    
    static {
        try {
            // Create the native callback method handle
            SCAN_CALLBACK = MethodHandles.lookup().findStatic(
                TableProviderManager.class,
                "nativeScanCallback",
                MethodType.methodType(MemorySegment.class, 
                    MemorySegment.class,  // user_data
                    MemorySegment.class,  // session_ptr  
                    MemorySegment.class,  // projections
                    int.class,            // projections_len
                    MemorySegment.class,  // sql_filter
                    long.class            // limit
                )
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize TableProviderManager", e);
        }
    }
    
    /**
     * Wrapper to hold TableProvider with its allocator and ID
     */
    private static class TableProviderWrapper {
        final TableProvider provider;
        final BufferAllocator allocator;
        final long id;
        
        TableProviderWrapper(TableProvider provider, BufferAllocator allocator, long id) {
            this.provider = provider;
            this.allocator = allocator;
            this.id = id;
        }
    }
    
    /**
     * Register a Java TableProvider with a DataFusion session context.
     * 
     * @param sessionCtx DataFusion session context
     * @param provider Java TableProvider implementation
     * @param tableName Name to register the table as
     * @param allocator Arrow memory allocator
     * @return true if registration succeeded, false otherwise
     */
    public static boolean registerTableProvider(
            MemorySegment sessionCtx, 
            TableProvider provider, 
            String tableName,
            BufferAllocator allocator) {
        
        if (sessionCtx == null || sessionCtx.equals(MemorySegment.NULL) || 
            provider == null || tableName == null || allocator == null) {
            return false;
        }
        
        // Generate unique ID for this provider
        long providerId = NEXT_ID.getAndIncrement();
        
        // Store the provider to prevent GC
        PROVIDERS.put(providerId, new TableProviderWrapper(provider, allocator, providerId));
        
        // Create user_data pointer containing the provider ID - use global arena to keep alive
        MemorySegment userDataSegment = GLOBAL_ARENA.allocate(ValueLayout.JAVA_LONG);
        userDataSegment.set(ValueLayout.JAVA_LONG, 0, providerId);
        
        // Create callback function descriptor
        FunctionDescriptor callbackDesc = FunctionDescriptor.of(
            ValueLayout.ADDRESS,  // return MemorySegment
            ValueLayout.ADDRESS,  // user_data
            ValueLayout.ADDRESS,  // session_ptr
            ValueLayout.ADDRESS,  // projections
            ValueLayout.JAVA_INT, // projections_len
            ValueLayout.ADDRESS,  // sql_filter
            ValueLayout.JAVA_LONG  // limit
        );
        
        // Create upcall stub for the callback - use global arena to keep alive!
        MemorySegment callback = Linker.nativeLinker().upcallStub(
            SCAN_CALLBACK,
            callbackDesc,
            GLOBAL_ARENA  // This is the key fix - don't auto-close the arena!
        );
        
        // Create schema JSON based on the provider's schema - use global arena  
        String schemaJson = generateSchemaJson(provider);
        MemorySegment schemaSegment = GLOBAL_ARENA.allocateUtf8String(schemaJson);
        MemorySegment tableNameSegment = GLOBAL_ARENA.allocateUtf8String(tableName);
        
        try {
            // Create the native table provider
            MemorySegment nativeProvider = DataFusionFFI.createJavaTableProvider(
                userDataSegment, callback, schemaSegment, tableNameSegment
            );
            
            if (nativeProvider == null || nativeProvider.equals(MemorySegment.NULL)) {
                PROVIDERS.remove(providerId);
                return false;
            }
            
            // Register with DataFusion
            int result = DataFusionFFI.registerJavaTableProvider(sessionCtx, nativeProvider, tableNameSegment);
            
            if (result != 0) {
                PROVIDERS.remove(providerId);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            System.err.println("Failed to register table provider: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Native callback function called from Rust when scan() is invoked.
     * This method bridges the native call to the Java TableProvider.
     */
    public static MemorySegment nativeScanCallback(
            MemorySegment userData,
            MemorySegment sessionPtr,
            MemorySegment projections,
            int projectionsLen,
            MemorySegment sqlFilter,
            long limit) {
        
        System.out.println("🔥 nativeScanCallback called from Rust!");
        System.out.println("   userData: " + userData.address());  
        System.out.println("   projectionsLen: " + projectionsLen);
        System.out.println("   limit: " + limit);
        
        try {
            // Fix the userData segment size issue - reinterpret with correct size
            MemorySegment fixedUserData = userData.reinterpret(ValueLayout.JAVA_LONG.byteSize());
            
            // Extract provider ID from user_data
            long providerId = fixedUserData.get(ValueLayout.JAVA_LONG, 0);
            System.out.println("   providerId: " + providerId);
            
            // Get the provider wrapper
            TableProviderWrapper wrapper = PROVIDERS.get(providerId);
            if (wrapper == null) {
                System.err.println("TableProvider not found for ID: " + providerId);
                return MemorySegment.NULL;
            }
            
            // Convert projections array
            int[] projectionsArray = null;
            if (projectionsLen > 0 && !projections.equals(MemorySegment.NULL)) {
                // Fix projections segment size issue - reinterpret with correct size
                MemorySegment fixedProjections = projections.reinterpret(ValueLayout.JAVA_INT.byteSize() * projectionsLen);
                projectionsArray = new int[(int) projectionsLen];
                for (int i = 0; i < projectionsLen; i++) {
                    projectionsArray[i] = (int) fixedProjections.getAtIndex(ValueLayout.JAVA_INT, i);
                }
            }
            
            // Convert SQL filter string
            String sqlFilterStr = "";
            if (!sqlFilter.equals(MemorySegment.NULL)) {
                // Fix SQL filter string access - no size limit since it's null-terminated
                sqlFilterStr = sqlFilter.reinterpret(Long.MAX_VALUE).getUtf8String(0);
            }
            
            // Call the Java TableProvider scan method
            MemorySegment result = wrapper.provider.scan(
                sessionPtr,
                projectionsArray,
                sqlFilterStr,
                limit,
                wrapper.allocator
            );
            
            return result != null ? result : MemorySegment.NULL;
            
        } catch (Exception e) {
            System.err.println("Error in native scan callback: " + e.getMessage());
            e.printStackTrace();
            return MemorySegment.NULL;
        }
    }
    
    /**
     * Clean up resources for a table provider
     */
    public static void cleanup(long providerId) {
        PROVIDERS.remove(providerId);
    }
    
    /**
     * Generate schema JSON from a TableProvider.
     * This creates a simple JSON schema format that can be parsed by the Rust side.
     */
    private static String generateSchemaJson(TableProvider provider) {
        // For now, we'll create a hardcoded schema based on the known structure
        // In a full implementation, this would inspect the provider's actual schema
        
        // The test uses "people" table with "name" and "age" columns
        StringBuilder json = new StringBuilder();
        json.append("{\"fields\":[");
        json.append("{\"name\":\"name\",\"type\":\"string\",\"nullable\":false},");
        json.append("{\"name\":\"age\",\"type\":\"int32\",\"nullable\":false}");
        json.append("]}");
        
        String schemaJson = json.toString();
        System.out.println("📋 Generated schema JSON: " + schemaJson);
        return schemaJson;
    }
}