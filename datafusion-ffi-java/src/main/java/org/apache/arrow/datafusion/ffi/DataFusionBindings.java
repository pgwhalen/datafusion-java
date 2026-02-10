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

  public static final MethodHandle ALLOC_EXECUTION_PLAN_CALLBACKS =
      downcall(
          "datafusion_alloc_execution_plan_callbacks", FunctionDescriptor.of(ValueLayout.ADDRESS));

  public static final MethodHandle ALLOC_RECORD_BATCH_READER_CALLBACKS =
      downcall(
          "datafusion_alloc_record_batch_reader_callbacks",
          FunctionDescriptor.of(ValueLayout.ADDRESS));

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

  // Physical expression functions
  public static final MethodHandle PHYSICAL_EXPR_DESTROY =
      downcall("datafusion_physical_expr_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  // Session functions
  public static final MethodHandle SESSION_CREATE_PHYSICAL_EXPR =
      downcall(
          "datafusion_session_create_physical_expr",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // session
              ValueLayout.ADDRESS, // filter_ptrs
              ValueLayout.JAVA_LONG, // filter_count
              ValueLayout.ADDRESS, // schema_ffi
              ValueLayout.ADDRESS // error_out
              ));

  // Literal guarantee functions
  public static final MethodHandle LITERAL_GUARANTEE_ANALYZE =
      downcall(
          "datafusion_literal_guarantee_analyze",
          FunctionDescriptor.of(
              ValueLayout.ADDRESS,
              ValueLayout.ADDRESS, // expr
              ValueLayout.ADDRESS, // count_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GUARANTEE_GET_INFO =
      downcall(
          "datafusion_guarantee_get_info",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // handle
              ValueLayout.JAVA_LONG, // idx
              ValueLayout.ADDRESS, // name_out
              ValueLayout.ADDRESS, // name_len_out
              ValueLayout.ADDRESS, // rel_type_out
              ValueLayout.ADDRESS, // rel_strs_out
              ValueLayout.ADDRESS, // guarantee_type_out
              ValueLayout.ADDRESS, // literal_count_out
              ValueLayout.ADDRESS, // spans_count_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GUARANTEE_GET_SPAN =
      downcall(
          "datafusion_guarantee_get_span",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // handle
              ValueLayout.JAVA_LONG, // g_idx
              ValueLayout.JAVA_LONG, // s_idx
              ValueLayout.ADDRESS, // start_line_out
              ValueLayout.ADDRESS, // start_col_out
              ValueLayout.ADDRESS, // end_line_out
              ValueLayout.ADDRESS, // end_col_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GUARANTEE_GET_LITERAL =
      downcall(
          "datafusion_guarantee_get_literal",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // handle
              ValueLayout.JAVA_LONG, // g_idx
              ValueLayout.JAVA_LONG, // l_idx
              ValueLayout.ADDRESS, // scalar_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GUARANTEES_DESTROY =
      downcall("datafusion_guarantees_destroy", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  public static final MethodHandle FFI_SCALAR_VALUE_SIZE =
      downcall("datafusion_ffi_scalar_value_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  public static final MethodHandle TEST_SCALAR_VALUE =
      downcall(
          "datafusion_test_scalar_value",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT, // type_tag
              ValueLayout.ADDRESS, // scalar_out
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
