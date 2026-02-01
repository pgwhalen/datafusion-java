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

  // Zero-copy verification functions (for testing)
  public static final MethodHandle GET_FFI_BUFFER_ADDRESSES =
      downcall(
          "datafusion_get_ffi_buffer_addresses",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // array
              ValueLayout.ADDRESS, // addresses_out
              ValueLayout.JAVA_INT, // max_addresses
              ValueLayout.ADDRESS, // actual_count_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GET_CHILD_BUFFER_ADDRESSES =
      downcall(
          "datafusion_get_child_buffer_addresses",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // array
              ValueLayout.JAVA_INT, // child_index
              ValueLayout.ADDRESS, // addresses_out
              ValueLayout.JAVA_INT, // max_addresses
              ValueLayout.ADDRESS, // actual_count_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle IMPORT_AND_GET_BUFFER_ADDRESSES =
      downcall(
          "datafusion_import_and_get_buffer_addresses",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // schema
              ValueLayout.ADDRESS, // array
              ValueLayout.JAVA_INT, // child_index
              ValueLayout.ADDRESS, // addresses_out
              ValueLayout.JAVA_INT, // max_addresses
              ValueLayout.ADDRESS, // actual_count_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle CREATE_TEST_DATA_WITH_ADDRESSES =
      downcall(
          "datafusion_create_test_data_with_addresses",
          FunctionDescriptor.of(
              ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS, // array_out
              ValueLayout.ADDRESS, // schema_out
              ValueLayout.ADDRESS, // data_buffer_address_out
              ValueLayout.ADDRESS // error_out
              ));

  public static final MethodHandle GET_FFI_N_CHILDREN =
      downcall(
          "datafusion_get_ffi_n_children",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

  public static final MethodHandle GET_FFI_N_BUFFERS =
      downcall(
          "datafusion_get_ffi_n_buffers",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

  private DataFusionBindings() {}

  private static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
    MemorySegment symbol =
        LOOKUP.find(name).orElseThrow(() -> new RuntimeException("Symbol not found: " + name));
    return LINKER.downcallHandle(symbol, descriptor);
  }
}
