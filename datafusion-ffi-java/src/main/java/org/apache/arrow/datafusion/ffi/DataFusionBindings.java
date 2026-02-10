package org.apache.arrow.datafusion.ffi;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

/**
 * FFM method handles for all native DataFusion functions.
 *
 * <p>This class provides low-level bindings to the native C-FFI library. Higher-level wrappers like
 * {@link FfiSessionContext} should be used instead for most purposes.
 */
public final class DataFusionBindings {
  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup LOOKUP = NativeLoader.get();

  // Runtime functions
  public static final MethodHandle RUNTIME_CREATE =
      downcall("datafusion_runtime_create", FunctionDescriptor.of(ValueLayout.ADDRESS));

  public static final MethodHandle RUNTIME_DESTROY =
      downcall("datafusion_runtime_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  // Context functions
  public static final MethodHandle CONTEXT_CREATE =
      downcall("datafusion_context_create", FunctionDescriptor.of(ValueLayout.ADDRESS));

  public static final MethodHandle CONTEXT_CREATE_WITH_CONFIG =
      downcall(
          "datafusion_context_create_with_config",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // keys
              ValueLayout.ADDRESS, // values
              ValueLayout.JAVA_LONG, // len
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle CONTEXT_DESTROY =
      downcall("datafusion_context_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle CONTEXT_REGISTER_RECORD_BATCH =
      downcall(
          "datafusion_context_register_record_batch",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // ctx
              ValueLayout.ADDRESS, // name
              ValueLayout.ADDRESS, // schema
              ValueLayout.ADDRESS, // array
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle CONTEXT_SQL =
      downcall(
          "datafusion_context_sql",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // rt
              ValueLayout.ADDRESS, // ctx
              ValueLayout.ADDRESS, // sql
              ValueLayout.ADDRESS // error_out
              ));

  // DataFrame functions
  public static final MethodHandle DATAFRAME_DESTROY =
      downcall("datafusion_dataframe_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle DATAFRAME_EXECUTE_STREAM =
      downcall(
          "datafusion_dataframe_execute_stream",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // rt
              ValueLayout.ADDRESS, // df
              ValueLayout.ADDRESS // error_out
              ));

  // Stream functions
  public static final MethodHandle STREAM_DESTROY =
      downcall("datafusion_stream_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle STREAM_SCHEMA =
      downcall(
          "datafusion_stream_schema",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // stream
              ValueLayout.ADDRESS, // schema_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle STREAM_NEXT =
      downcall(
          "datafusion_stream_next",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // rt
              ValueLayout.ADDRESS, // stream
              ValueLayout.ADDRESS, // array_out
              ValueLayout.ADDRESS, // schema_out
              ValueLayout.ADDRESS // error_out
              ));

  // Memory functions
  public static final MethodHandle FREE_STRING =
      downcall("datafusion_free_string", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle FREE_STRING_ARRAY =
      downcall(
          "datafusion_free_string_array",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS, // strings
              ValueLayout.JAVA_LONG // len
              ));

  // Catalog registration functions
  public static final MethodHandle CONTEXT_REGISTER_CATALOG =
      downcall(
          "datafusion_context_register_catalog",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // ctx
              ValueLayout.ADDRESS, // name
              ValueLayout.ADDRESS, // callbacks
              ValueLayout.ADDRESS // error_out
              ));

  // Callback allocation functions
  public static final MethodHandle ALLOC_CATALOG_PROVIDER_CALLBACKS =
      downcall(
          "datafusion_alloc_catalog_provider_callbacks",
          FunctionDescriptor.of(ValueLayout.ADDRESS));

  public static final MethodHandle ALLOC_SCHEMA_PROVIDER_CALLBACKS =
      downcall(
          "datafusion_alloc_schema_provider_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  public static final MethodHandle ALLOC_TABLE_PROVIDER_CALLBACKS =
      downcall(
          "datafusion_alloc_table_provider_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  // FFI_RecordBatchStream layout helpers (Java constructs the stream struct directly)
  public static final MethodHandle POLL_RESULT_SIZE =
      downcall("datafusion_poll_result_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle POLL_RESULT_ALIGN =
      downcall("datafusion_poll_result_align", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle WRAPPED_SCHEMA_SIZE =
      downcall("datafusion_wrapped_schema_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle FFI_RECORD_BATCH_STREAM_SIZE =
      downcall(
          "datafusion_ffi_record_batch_stream_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle RSTRING_SIZE =
      downcall("datafusion_rstring_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle CREATE_RSTRING =
      downcall(
          "datafusion_create_rstring",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS, // ptr (UTF-8 bytes)
              ValueLayout.JAVA_LONG, // len
              ValueLayout.ADDRESS // out (RString buffer)
              ));

  // FFI_ExecutionPlan layout helpers (Java constructs the plan struct directly)
  public static final MethodHandle FFI_EXECUTION_PLAN_SIZE =
      downcall("datafusion_ffi_execution_plan_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle FFI_PLAN_PROPERTIES_SIZE =
      downcall("datafusion_ffi_plan_properties_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle RVEC_PLAN_SIZE =
      downcall("datafusion_rvec_plan_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle FFI_RESULT_STREAM_SIZE =
      downcall("datafusion_ffi_result_stream_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle FFI_TASK_CONTEXT_SIZE =
      downcall("datafusion_ffi_task_context_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle CREATE_FFI_PLAN_PROPERTIES =
      downcall(
          "datafusion_create_ffi_plan_properties",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT, // partitioning_count
              ValueLayout.JAVA_INT, // emission_type
              ValueLayout.JAVA_INT, // boundedness
              ValueLayout.ADDRESS, // schema_ptr (FFI_ArrowSchema*)
              ValueLayout.ADDRESS // out (FFI_PlanProperties buffer)
              ));

  public static final MethodHandle CREATE_EMPTY_RVEC_PLAN =
      downcall(
          "datafusion_create_empty_rvec_plan",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS // out (RVec<FFI_ExecutionPlan> buffer)
              ));

  // File format callback allocation
  public static final MethodHandle ALLOC_FILE_FORMAT_CALLBACKS =
      downcall(
          "datafusion_alloc_file_format_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  // File source callback allocation
  public static final MethodHandle ALLOC_FILE_SOURCE_CALLBACKS =
      downcall(
          "datafusion_alloc_file_source_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  // File opener callback allocation
  public static final MethodHandle ALLOC_FILE_OPENER_CALLBACKS =
      downcall(
          "datafusion_alloc_file_opener_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  // Session state functions
  public static final MethodHandle CONTEXT_STATE =
      downcall(
          "datafusion_context_state",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // ctx
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle SESSION_STATE_DESTROY =
      downcall("datafusion_session_state_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle SESSION_STATE_CREATE_LOGICAL_PLAN =
      downcall(
          "datafusion_session_state_create_logical_plan",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // state_with_rt
              ValueLayout.ADDRESS, // sql
              ValueLayout.ADDRESS // error_out
              ));

  // Logical plan functions
  public static final MethodHandle LOGICAL_PLAN_DESTROY =
      downcall("datafusion_logical_plan_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  // Listing table registration (all-in-one)
  public static final MethodHandle CONTEXT_REGISTER_LISTING_TABLE =
      downcall(
          "datafusion_context_register_listing_table",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // ctx
              ValueLayout.ADDRESS, // rt
              ValueLayout.ADDRESS, // name
              ValueLayout.ADDRESS, // urls (pointer to array of C string pointers)
              ValueLayout.JAVA_LONG, // urls_len
              ValueLayout.ADDRESS, // file_extension
              ValueLayout.ADDRESS, // schema (FFI_ArrowSchema*)
              ValueLayout.ADDRESS, // format_callbacks
              ValueLayout.JAVA_INT, // collect_stat (i32 in Rust, 0=false, 1=true)
              ValueLayout.JAVA_LONG, // target_partitions
              ValueLayout.ADDRESS // error_out
              ));

  private DataFusionBindings() {}

  /** Get the native linker for creating upcall stubs. */
  public static Linker getLinker() {
    return LINKER;
  }

  private static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
    MemorySegment symbol =
        LOOKUP.find(name).orElseThrow(() -> new RuntimeException("Symbol not found: " + name));
    return LINKER.downcallHandle(symbol, descriptor);
  }
}
