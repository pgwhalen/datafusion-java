package org.apache.arrow.datafusion.panama;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Java Panama FFM bindings for DataFusion C FFI library.
 * This class provides a Java interface to the DataFusion native library
 * using the new Foreign Function & Memory API.
 */
public class DataFusionFFI {
    
    private static final Linker linker = Linker.nativeLinker();
    private static final SymbolLookup lookup;
    
    // Method handles for the native functions
    private static final MethodHandle datafusion_session_context_new;
    private static final MethodHandle datafusion_session_context_destroy;
    private static final MethodHandle datafusion_session_context_sql_arrow;
    private static final MethodHandle datafusion_session_context_sql_arrow_stream;
    private static final MethodHandle datafusion_arrow_result_get_schema;
    private static final MethodHandle datafusion_arrow_result_create_schema_copy;
    private static final MethodHandle datafusion_free_schema_copy;
    private static final MethodHandle datafusion_arrow_result_get_num_batches;
    private static final MethodHandle datafusion_arrow_result_get_batch;
    private static final MethodHandle datafusion_arrow_result_free;
    private static final MethodHandle datafusion_arrow_stream_get_stream;
    private static final MethodHandle datafusion_arrow_stream_free;
    private static final MethodHandle datafusion_create_java_table_provider;
    private static final MethodHandle datafusion_register_java_table_provider;
    private static final MethodHandle datafusion_free_java_table_provider;
    private static final MethodHandle datafusion_create_java_execution_plan;
    private static final MethodHandle datafusion_free_java_execution_plan;
    private static final MethodHandle datafusion_create_arrow_stream_result;
    private static final MethodHandle datafusion_free_arrow_stream_result;
    private static final MethodHandle datafusion_get_last_error;
    private static final MethodHandle datafusion_free_error_string;
    private static final MethodHandle datafusion_version;
    
    static {
        // Load the native library
        System.loadLibrary("datafusion_panama_ffi");
        lookup = SymbolLookup.loaderLookup();
        
        try {
            // Initialize method handles
            var newSymbol = lookup.find("datafusion_session_context_new");
            if (newSymbol.isEmpty()) {
                throw new RuntimeException("Symbol 'datafusion_session_context_new' not found");
            }
            datafusion_session_context_new = linker.downcallHandle(
                newSymbol.get(),
                FunctionDescriptor.of(ValueLayout.ADDRESS)
            );
            
            datafusion_session_context_destroy = linker.downcallHandle(
                lookup.find("datafusion_session_context_destroy").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_session_context_sql_arrow = linker.downcallHandle(
                lookup.find("datafusion_session_context_sql_arrow").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_session_context_sql_arrow_stream = linker.downcallHandle(
                lookup.find("datafusion_session_context_sql_arrow_stream").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_result_get_schema = linker.downcallHandle(
                lookup.find("datafusion_arrow_result_get_schema").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_result_create_schema_copy = linker.downcallHandle(
                lookup.find("datafusion_arrow_result_create_schema_copy").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_free_schema_copy = linker.downcallHandle(
                lookup.find("datafusion_free_schema_copy").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_result_get_num_batches = linker.downcallHandle(
                lookup.find("datafusion_arrow_result_get_num_batches").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_result_get_batch = linker.downcallHandle(
                lookup.find("datafusion_arrow_result_get_batch").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)
            );
            
            datafusion_arrow_result_free = linker.downcallHandle(
                lookup.find("datafusion_arrow_result_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_stream_get_stream = linker.downcallHandle(
                lookup.find("datafusion_arrow_stream_get_stream").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_arrow_stream_free = linker.downcallHandle(
                lookup.find("datafusion_arrow_stream_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_create_java_table_provider = linker.downcallHandle(
                lookup.find("datafusion_create_java_table_provider").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_register_java_table_provider = linker.downcallHandle(
                lookup.find("datafusion_register_java_table_provider").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_free_java_table_provider = linker.downcallHandle(
                lookup.find("datafusion_free_java_table_provider").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_create_java_execution_plan = linker.downcallHandle(
                lookup.find("datafusion_create_java_execution_plan").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_free_java_execution_plan = linker.downcallHandle(
                lookup.find("datafusion_free_java_execution_plan").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_create_arrow_stream_result = linker.downcallHandle(
                lookup.find("datafusion_create_arrow_stream_result").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
            );
            
            datafusion_free_arrow_stream_result = linker.downcallHandle(
                lookup.find("datafusion_free_arrow_stream_result").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            datafusion_get_last_error = linker.downcallHandle(
                lookup.find("datafusion_get_last_error").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS)
            );
            
            datafusion_free_error_string = linker.downcallHandle(
                lookup.find("datafusion_free_error_string").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            
            var versionSymbol = lookup.find("datafusion_version");
            if (versionSymbol.isEmpty()) {
                throw new RuntimeException("Symbol 'datafusion_version' not found");
            }
            datafusion_version = linker.downcallHandle(
                versionSymbol.get(),
                FunctionDescriptor.of(ValueLayout.ADDRESS)
            );
            
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }
    
    /**
     * Create a new DataFusion session context
     * @return Memory address of the session context, or MemorySegment.NULL on failure
     */
    public static MemorySegment sessionContextNew() {
        try {
            return (MemorySegment) datafusion_session_context_new.invoke();
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create session context", t);
        }
    }
    
    /**
     * Destroy a DataFusion session context
     * @param ctx Memory address of the session context
     */
    public static void sessionContextDestroy(MemorySegment ctx) {
        try {
            datafusion_session_context_destroy.invoke(ctx);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy session context", t);
        }
    }
    
    /**
     * Execute a SQL query and return Arrow data result (batch-based)
     * @param ctx Memory address of the session context
     * @param sql SQL query string
     * @return Memory address of ArrowDataResult, or MemorySegment.NULL on failure
     */
    public static MemorySegment sessionContextSqlArrow(MemorySegment ctx, String sql) {
        System.out.println("🔄 [JAVA FFI] sessionContextSqlArrow called (batch-based)");
        System.out.println("   Context: " + (ctx.equals(MemorySegment.NULL) ? "NULL" : ctx.address()));
        System.out.println("   SQL: '" + sql + "'");
        
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqlSegment = arena.allocateUtf8String(sql);
            System.out.println("✅ [JAVA FFI] SQL string allocated at: " + sqlSegment.address());
            
            System.out.println("🔄 [JAVA FFI] Calling native datafusion_session_context_sql_arrow...");
            MemorySegment result = (MemorySegment) datafusion_session_context_sql_arrow.invoke(ctx, sqlSegment);
            
            if (result.equals(MemorySegment.NULL)) {
                System.out.println("❌ [JAVA FFI] Native function returned NULL pointer");
            } else {
                System.out.println("✅ [JAVA FFI] Native function returned pointer: " + result.address());
            }
            
            return result;
        } catch (Throwable t) {
            System.out.println("❌ [JAVA FFI] Exception in sessionContextSqlArrow: " + t.getMessage());
            t.printStackTrace();
            throw new RuntimeException("Failed to execute SQL with Arrow data result", t);
        }
    }

    /**
     * Execute a SQL query and return Arrow stream
     * @param ctx Memory address of the session context
     * @param sql SQL query string
     * @return Memory address of FFI_ArrowArrayStream, or MemorySegment.NULL on failure
     */
    public static MemorySegment sessionContextSqlArrowStream(MemorySegment ctx, String sql) {
        System.out.println("🔄 [JAVA FFI] sessionContextSqlArrowStream called (streaming)");
        System.out.println("   Context: " + (ctx.equals(MemorySegment.NULL) ? "NULL" : ctx.address()));
        System.out.println("   SQL: '" + sql + "'");
        
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment sqlSegment = arena.allocateUtf8String(sql);
            System.out.println("✅ [JAVA FFI] SQL string allocated at: " + sqlSegment.address());
            
            System.out.println("🔄 [JAVA FFI] Calling native datafusion_session_context_sql_arrow_stream...");
            MemorySegment result = (MemorySegment) datafusion_session_context_sql_arrow_stream.invoke(ctx, sqlSegment);
            
            if (result.equals(MemorySegment.NULL)) {
                System.out.println("❌ [JAVA FFI] Native function returned NULL pointer");
            } else {
                System.out.println("✅ [JAVA FFI] Native function returned pointer: " + result.address());
            }
            
            return result;
        } catch (Throwable t) {
            System.out.println("❌ [JAVA FFI] Exception in sessionContextSqlArrowStream: " + t.getMessage());
            t.printStackTrace();
            throw new RuntimeException("Failed to execute SQL with Arrow stream", t);
        }
    }
    
    /**
     * Get schema pointer from ArrowDataResult
     * @param result Memory address of ArrowDataResult
     * @return Memory address of Arrow C schema
     */
    public static MemorySegment arrowResultGetSchema(MemorySegment result) {
        try {
            return (MemorySegment) datafusion_arrow_result_get_schema.invoke(result);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get schema", t);
        }
    }

    /**
     * Create a fresh copy of the schema for importing (handles C Data Interface ownership)
     * @param result Memory address of ArrowDataResult
     * @return Memory address of a new Arrow C schema copy
     */
    public static MemorySegment arrowResultCreateSchemaCopy(MemorySegment result) {
        try {
            return (MemorySegment) datafusion_arrow_result_create_schema_copy.invoke(result);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create schema copy", t);
        }
    }

    /**
     * Free a schema copy created by arrowResultCreateSchemaCopy
     * @param schemaCopy Memory address of the schema copy to free
     */
    public static void freeSchemaCopy(MemorySegment schemaCopy) {
        try {
            datafusion_free_schema_copy.invoke(schemaCopy);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free schema copy", t);
        }
    }
    
    /**
     * Get number of batches from ArrowDataResult
     * @param result Memory address of ArrowDataResult
     * @return Number of batches
     */
    public static int arrowResultGetNumBatches(MemorySegment result) {
        try {
            return (int) datafusion_arrow_result_get_num_batches.invoke(result);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get number of batches", t);
        }
    }
    
    /**
     * Get a specific batch from ArrowDataResult
     * @param result Memory address of ArrowDataResult
     * @param batchIndex Index of the batch to get
     * @return Memory address of Arrow C array
     */
    public static MemorySegment arrowResultGetBatch(MemorySegment result, int batchIndex) {
        try {
            return (MemorySegment) datafusion_arrow_result_get_batch.invoke(result, batchIndex);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get batch", t);
        }
    }
    
    /**
     * Get ArrowArrayStream pointer from ArrowStreamResult
     * @param streamResult Memory address of ArrowStreamResult
     * @return Memory address of ArrowArrayStream
     */
    public static MemorySegment arrowStreamGetStream(MemorySegment streamResult) {
        try {
            return (MemorySegment) datafusion_arrow_stream_get_stream.invoke(streamResult);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get stream", t);
        }
    }

    /**
     * Free ArrowStreamResult
     * @param streamResult Memory address of ArrowStreamResult
     */
    public static void arrowStreamFree(MemorySegment streamResult) {
        try {
            datafusion_arrow_stream_free.invoke(streamResult);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free Arrow stream", t);
        }
    }

    /**
     * Create a Java TableProvider 
     * @param userData User data pointer containing provider ID
     * @param callback Native callback function for scan operations
     * @param schemaJson JSON schema definition
     * @param tableName Name of the table
     * @return Memory address of native TableProvider, or MemorySegment.NULL on failure
     */
    public static MemorySegment createJavaTableProvider(
            MemorySegment userData, 
            MemorySegment callback, 
            MemorySegment schemaJson, 
            MemorySegment tableName) {
        try {
            return (MemorySegment) datafusion_create_java_table_provider.invoke(
                userData, callback, schemaJson, tableName);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create Java table provider", t);
        }
    }

    /**
     * Register a Java TableProvider with DataFusion session context
     * @param sessionCtx Memory address of session context
     * @param provider Memory address of native TableProvider
     * @param tableName Memory address of table name string
     * @return 0 on success, -1 on failure
     */
    public static int registerJavaTableProvider(
            MemorySegment sessionCtx, 
            MemorySegment provider, 
            MemorySegment tableName) {
        try {
            return (int) datafusion_register_java_table_provider.invoke(sessionCtx, provider, tableName);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to register Java table provider", t);
        }
    }

    /**
     * Free a Java TableProvider
     * @param provider Memory address of TableProvider to free
     */
    public static void freeJavaTableProvider(MemorySegment provider) {
        try {
            datafusion_free_java_table_provider.invoke(provider);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free Java table provider", t);
        }
    }

    /**
     * Free ArrowDataResult
     * @param result Memory address of ArrowDataResult
     */
    public static void arrowResultFree(MemorySegment result) {
        try {
            datafusion_arrow_result_free.invoke(result);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free Arrow result", t);
        }
    }
    
    /**
     * Get the last error message
     * @return Error message string, or null if no error
     */
    public static String getLastError() {
        try {
            MemorySegment errorPtr = (MemorySegment) datafusion_get_last_error.invoke();
            if (errorPtr.equals(MemorySegment.NULL)) {
                return null;
            }
            return errorPtr.getUtf8String(0);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get last error", t);
        }
    }
    
    /**
     * Free an error message string
     * @param errorMsg Memory address of the error message
     */
    public static void freeErrorString(MemorySegment errorMsg) {
        try {
            datafusion_free_error_string.invoke(errorMsg);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free error string", t);
        }
    }
    
    /**
     * Create a Java ExecutionPlan 
     * @param userData User data pointer containing plan ID
     * @param callback Native callback function for execute operations
     * @param planName Name of the execution plan
     * @param schemaJson JSON schema definition
     * @return Memory address of native ExecutionPlan, or MemorySegment.NULL on failure
     */
    public static MemorySegment createJavaExecutionPlan(
            MemorySegment userData, 
            MemorySegment callback, 
            MemorySegment planName, 
            MemorySegment schemaJson) {
        try {
            return (MemorySegment) datafusion_create_java_execution_plan.invoke(
                userData, callback, planName, schemaJson);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create Java execution plan", t);
        }
    }

    /**
     * Free a Java ExecutionPlan
     * @param plan Memory address of ExecutionPlan to free
     */
    public static void freeJavaExecutionPlan(MemorySegment plan) {
        try {
            datafusion_free_java_execution_plan.invoke(plan);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free Java execution plan", t);
        }
    }

    /**
     * Create an ArrowStreamResult from an ArrowArrayStream pointer
     * @param streamPtr Memory address of the ArrowArrayStream
     * @return Memory address of ArrowStreamResult, or MemorySegment.NULL on failure
     */
    public static MemorySegment createArrowStreamResult(MemorySegment streamPtr) {
        try {
            return (MemorySegment) datafusion_create_arrow_stream_result.invoke(streamPtr);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create ArrowStreamResult", t);
        }
    }

    /**
     * Free an ArrowStreamResult
     * @param streamResult Memory address of ArrowStreamResult to free
     */
    public static void freeArrowStreamResult(MemorySegment streamResult) {
        try {
            datafusion_free_arrow_stream_result.invoke(streamResult);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to free ArrowStreamResult", t);
        }
    }

    /**
     * Get version information
     * @return Version string
     */
    public static String version() {
        try {
            MemorySegment versionPtr = (MemorySegment) datafusion_version.invoke();
            if (versionPtr.equals(MemorySegment.NULL)) {
                return "NULL pointer returned";
            }
            // For C strings, we need to create a bounded segment by finding the string length
            // We'll use a reasonable maximum length for now
            MemorySegment boundedPtr = versionPtr.reinterpret(1000); // 1000 bytes should be enough for version string
            return boundedPtr.getUtf8String(0);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get version", t);
        }
    }
}