package org.apache.arrow.datafusion;

import java.lang.foreign.*;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Internal FFI bridge for ScalarUdf.
 *
 * <p>This class constructs an {@code FFI_ScalarUDF} struct directly in Java arena memory. The
 * struct contains function pointers that Rust invokes via {@code ForeignScalarUDF}.
 *
 * <p>Layout of FFI_ScalarUDF (136 bytes, align 8):
 *
 * <pre>
 * offset   0: name                   (RString, 32 bytes)
 * offset  32: aliases                (RVec&lt;RString&gt;, 32 bytes)
 * offset  64: volatility             (FFI_Volatility i32, 4 bytes + 4 pad)
 * offset  72: return_field_from_args (ADDRESS)
 * offset  80: invoke_with_args       (ADDRESS)
 * offset  88: short_circuits         (bool, 1 byte + 7 pad)
 * offset  96: coerce_types           (ADDRESS)
 * offset 104: clone                  (ADDRESS)
 * offset 112: release                (ADDRESS)
 * offset 120: private_data           (ADDRESS)
 * offset 128: library_marker_id      (ADDRESS)
 * </pre>
 */
final class ScalarUdfHandle implements TraitHandle {

  // ======== Struct layout ========

  // RString and RVec<RString> are 32 bytes each — model as 4 longs
  // FFI_Volatility is repr(C) enum with i32 discriminant — 4 bytes + 4 pad before next ptr
  // short_circuits is bool (1 byte) + 7 pad
  static final StructLayout FFI_SCALAR_UDF_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("name"),
          MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG).withName("aliases"),
          ValueLayout.JAVA_INT.withName("volatility"),
          MemoryLayout.paddingLayout(4),
          ValueLayout.ADDRESS.withName("return_field_from_args"),
          ValueLayout.ADDRESS.withName("invoke_with_args"),
          ValueLayout.JAVA_LONG.withName("short_circuits"),
          ValueLayout.ADDRESS.withName("coerce_types"),
          ValueLayout.ADDRESS.withName("clone"),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("library_marker_id"));

  private static final long NAME_OFFSET =
      FFI_SCALAR_UDF_LAYOUT.byteOffset(PathElement.groupElement("name"));
  private static final long ALIASES_OFFSET =
      FFI_SCALAR_UDF_LAYOUT.byteOffset(PathElement.groupElement("aliases"));

  private static final VarHandle VH_VOLATILITY =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("volatility"));
  private static final VarHandle VH_RETURN_FIELD_FROM_ARGS =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("return_field_from_args"));
  private static final VarHandle VH_INVOKE_WITH_ARGS =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("invoke_with_args"));
  private static final VarHandle VH_SHORT_CIRCUITS =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("short_circuits"));
  private static final VarHandle VH_COERCE_TYPES =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("coerce_types"));
  private static final VarHandle VH_CLONE =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("clone"));
  private static final VarHandle VH_RELEASE =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("release"));
  private static final VarHandle VH_PRIVATE_DATA =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("private_data"));
  private static final VarHandle VH_LIBRARY_MARKER_ID =
      FFI_SCALAR_UDF_LAYOUT.varHandle(PathElement.groupElement("library_marker_id"));

  // ======== Sizes ========

  private static final long ARROW_SCHEMA_SIZE = 72; // sizeof(FFI_ArrowSchema) = WrappedSchema
  private static final long ARROW_ARRAY_SIZE = 80; // sizeof(FFI_ArrowArray)
  private static final long WRAPPED_ARRAY_SIZE = ARROW_ARRAY_SIZE + ARROW_SCHEMA_SIZE; // 152

  // ======== Return type layouts ========

  // FFIResult<WrappedSchema> = RResult<WrappedSchema, RString>: disc(8) + max(72,32) = 80 bytes
  private static final StructLayout FFI_RESULT_WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(10, ValueLayout.JAVA_LONG).withName("ffi_result"));

  private static final long RESULT_SCHEMA_DISC_OFFSET = 0;
  private static final long RESULT_SCHEMA_PAYLOAD_OFFSET = 8;

  // FFIResult<WrappedArray> = RResult<WrappedArray, RString>: disc(8) + max(152,32) = 160 bytes
  private static final StructLayout FFI_RESULT_WRAPPED_ARRAY_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(20, ValueLayout.JAVA_LONG).withName("ffi_result"));

  private static final long RESULT_ARRAY_DISC_OFFSET = 0;
  private static final long RESULT_ARRAY_PAYLOAD_OFFSET = 8;

  // FFIResult<RVec<WrappedSchema>> = RResult<RVec<WrappedSchema>, RString>: disc(8) + max(32,32)
  // = 40
  private static final StructLayout FFI_RESULT_RVEC_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(5, ValueLayout.JAVA_LONG).withName("ffi_result"));

  private static final long RESULT_RVEC_DISC_OFFSET = 0;
  private static final long RESULT_RVEC_PAYLOAD_OFFSET = 8;

  // FFI_ReturnFieldArgs: { arg_fields: RVec<WrappedSchema>(32), scalar_arguments:
  // RVec<ROption<RVec<u8>>>(32) } = 64 bytes
  private static final StructLayout FFI_RETURN_FIELD_ARGS_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(8, ValueLayout.JAVA_LONG).withName("fields"));

  // RVec layout: { buf: *mut T, length: usize, capacity: usize, vtable: *const } = 32 bytes
  private static final StructLayout RVEC_WRAPPED_ARRAY_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG));

  private static final StructLayout RVEC_WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(MemoryLayout.sequenceLayout(4, ValueLayout.JAVA_LONG));

  // WrappedSchema passed by value in invoke_with_args (return_field param)
  private static final StructLayout WRAPPED_SCHEMA_LAYOUT =
      MemoryLayout.structLayout(
          MemoryLayout.sequenceLayout(ARROW_SCHEMA_SIZE / 8, ValueLayout.JAVA_LONG));

  // ======== Rust symbol lookups ========

  private static final MemorySegment CLONE_FN =
      NativeLoader.get().find("datafusion_scalar_udf_clone").orElseThrow();

  private static final MemorySegment RELEASE_FN =
      NativeLoader.get().find("datafusion_scalar_udf_release").orElseThrow();

  // ======== Downcall handles for Rust helpers ========

  private static final MethodHandle CREATE_EMPTY_RVEC_RSTRING =
      NativeUtil.downcall(
          "datafusion_create_empty_rvec_rstring", FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

  private static final MethodHandle CREATE_RVEC_WRAPPED_SCHEMA =
      NativeUtil.downcall(
          "datafusion_create_rvec_wrapped_schema",
          FunctionDescriptor.ofVoid(
              ValueLayout.ADDRESS.withName("schemas"),
              ValueLayout.JAVA_LONG.withName("count"),
              ValueLayout.ADDRESS.withName("out")));

  // ======== Size validation downcalls ========

  private static final MethodHandle FFI_SCALAR_UDF_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_scalar_udf_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RETURN_FIELD_ARGS_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_return_field_args_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_WRAPPED_SCHEMA_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_result_wrapped_schema_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_WRAPPED_ARRAY_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_result_wrapped_array_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_WRAPPED_ARRAY_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_wrapped_array_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RESULT_RVEC_WRAPPED_SCHEMA_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_result_rvec_wrapped_schema_size",
          FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RVEC_RSTRING_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_rvec_rstring_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_VOLATILITY_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_volatility_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  private static final MethodHandle FFI_RVEC_WRAPPED_SCHEMA_SIZE_MH =
      NativeUtil.downcall(
          "datafusion_ffi_rvec_wrapped_schema_size", FunctionDescriptor.of(ValueLayout.JAVA_LONG));

  // ======== Callback FunctionDescriptors ========

  // return_field_from_args: (&Self, FFI_ReturnFieldArgs) -> FFIResult<WrappedSchema>
  private static final FunctionDescriptor RETURN_FIELD_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_WRAPPED_SCHEMA_LAYOUT, ValueLayout.ADDRESS, FFI_RETURN_FIELD_ARGS_LAYOUT);

  // invoke_with_args: (&Self, RVec<WrappedArray>, RVec<WrappedSchema>, usize, WrappedSchema) ->
  // FFIResult<WrappedArray>
  private static final FunctionDescriptor INVOKE_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_WRAPPED_ARRAY_LAYOUT,
          ValueLayout.ADDRESS,
          RVEC_WRAPPED_ARRAY_LAYOUT,
          RVEC_WRAPPED_SCHEMA_LAYOUT,
          ValueLayout.JAVA_LONG,
          WRAPPED_SCHEMA_LAYOUT);

  // coerce_types: (&Self, RVec<WrappedSchema>) -> FFIResult<RVec<WrappedSchema>>
  private static final FunctionDescriptor COERCE_TYPES_DESC =
      FunctionDescriptor.of(
          FFI_RESULT_RVEC_SCHEMA_LAYOUT, ValueLayout.ADDRESS, RVEC_WRAPPED_SCHEMA_LAYOUT);

  // release: (&mut Self) -> void
  private static final FunctionDescriptor RELEASE_DESC =
      FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

  // ======== Static MethodHandles ========

  private static final MethodHandle RETURN_FIELD_MH = initReturnFieldMethodHandle();
  private static final MethodHandle INVOKE_MH = initInvokeMethodHandle();
  private static final MethodHandle COERCE_TYPES_MH = initCoerceTypesMethodHandle();
  private static final MethodHandle RELEASE_MH = initReleaseMethodHandle();

  private static MethodHandle initReturnFieldMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ScalarUdfHandle.class,
              "returnFieldFromArgs",
              MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initInvokeMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ScalarUdfHandle.class,
              "invokeWithArgs",
              MethodType.methodType(
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  MemorySegment.class,
                  long.class,
                  MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initCoerceTypesMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ScalarUdfHandle.class,
              "coerceTypes",
              MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static MethodHandle initReleaseMethodHandle() {
    try {
      return MethodHandles.lookup()
          .findVirtual(
              ScalarUdfHandle.class,
              "release",
              MethodType.methodType(void.class, MemorySegment.class));
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  // ======== Size validation ========

  static void validateSizes() {
    NativeUtil.validateSize(
        FFI_SCALAR_UDF_LAYOUT.byteSize(), FFI_SCALAR_UDF_SIZE_MH, "FFI_ScalarUDF");
    NativeUtil.validateSize(
        FFI_RETURN_FIELD_ARGS_LAYOUT.byteSize(),
        FFI_RETURN_FIELD_ARGS_SIZE_MH,
        "FFI_ReturnFieldArgs");
    NativeUtil.validateSize(
        FFI_RESULT_WRAPPED_SCHEMA_LAYOUT.byteSize(),
        FFI_RESULT_WRAPPED_SCHEMA_SIZE_MH,
        "FFIResult<WrappedSchema>");
    NativeUtil.validateSize(
        FFI_RESULT_WRAPPED_ARRAY_LAYOUT.byteSize(),
        FFI_RESULT_WRAPPED_ARRAY_SIZE_MH,
        "FFIResult<WrappedArray>");
    NativeUtil.validateSize(WRAPPED_ARRAY_SIZE, FFI_WRAPPED_ARRAY_SIZE_MH, "WrappedArray");
    NativeUtil.validateSize(
        FFI_RESULT_RVEC_SCHEMA_LAYOUT.byteSize(),
        FFI_RESULT_RVEC_WRAPPED_SCHEMA_SIZE_MH,
        "FFIResult<RVec<WrappedSchema>>");
    NativeUtil.validateSize(32, FFI_RVEC_RSTRING_SIZE_MH, "RVec<RString>");
    NativeUtil.validateSize(4, FFI_VOLATILITY_SIZE_MH, "FFI_Volatility");
    NativeUtil.validateSize(32, FFI_RVEC_WRAPPED_SCHEMA_SIZE_MH, "RVec<WrappedSchema>");
  }

  // ======== Instance fields ========

  private final Arena arena;
  private final ScalarUdf udf;
  private final BufferAllocator allocator;
  private final boolean fullStackTrace;
  private final MemorySegment ffiUdf;

  // Keep references to upcall stubs to prevent GC
  private final UpcallStub returnFieldStub;
  private final UpcallStub invokeStub;
  private final UpcallStub coerceTypesStub;
  private final UpcallStub releaseStub;

  ScalarUdfHandle(ScalarUdf udf, BufferAllocator allocator, Arena arena, boolean fullStackTrace) {
    this.arena = arena;
    this.udf = udf;
    this.allocator = allocator;
    this.fullStackTrace = fullStackTrace;

    try {
      // Create upcall stubs
      this.returnFieldStub =
          UpcallStub.create(RETURN_FIELD_MH.bindTo(this), RETURN_FIELD_DESC, arena);
      this.invokeStub = UpcallStub.create(INVOKE_MH.bindTo(this), INVOKE_DESC, arena);
      this.coerceTypesStub =
          UpcallStub.create(COERCE_TYPES_MH.bindTo(this), COERCE_TYPES_DESC, arena);
      this.releaseStub = UpcallStub.create(RELEASE_MH.bindTo(this), RELEASE_DESC, arena);

      // Allocate FFI_ScalarUDF struct
      this.ffiUdf = arena.allocate(FFI_SCALAR_UDF_LAYOUT);

      // Write name (RString) via Rust helper
      NativeUtil.writeRString(udf.name(), ffiUdf, NAME_OFFSET, arena);

      // Write empty aliases (RVec<RString>) via Rust helper
      CREATE_EMPTY_RVEC_RSTRING.invokeExact(ffiUdf.asSlice(ALIASES_OFFSET, 32));

      // Write volatility (FFI_Volatility: repr(C) enum, i32 discriminant)
      int volatilityDisc =
          switch (udf.volatility()) {
            case IMMUTABLE -> 0;
            case STABLE -> 1;
            case VOLATILE -> 2;
          };
      VH_VOLATILITY.set(ffiUdf, 0L, volatilityDisc);

      // Set callback function pointers
      VH_RETURN_FIELD_FROM_ARGS.set(ffiUdf, 0L, returnFieldStub.segment());
      VH_INVOKE_WITH_ARGS.set(ffiUdf, 0L, invokeStub.segment());

      // short_circuits = false (1 byte bool at offset 88, modeled as JAVA_LONG)
      VH_SHORT_CIRCUITS.set(ffiUdf, 0L, 0L);

      VH_COERCE_TYPES.set(ffiUdf, 0L, coerceTypesStub.segment());
      VH_CLONE.set(ffiUdf, 0L, CLONE_FN);
      VH_RELEASE.set(ffiUdf, 0L, RELEASE_FN);
      VH_PRIVATE_DATA.set(ffiUdf, 0L, MemorySegment.NULL);
      VH_LIBRARY_MARKER_ID.set(ffiUdf, 0L, NativeUtil.JAVA_MARKER_ID_FN);

    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new DataFusionException("Failed to create ScalarUdfHandle", e);
    }
  }

  @Override
  public MemorySegment getTraitStruct() {
    return ffiUdf;
  }

  // ======== Callbacks ========

  /**
   * Callback: return_field_from_args. Receives FFI_ReturnFieldArgs (arg_fields RVec + ignored
   * scalar_arguments RVec). Returns FFIResult&lt;WrappedSchema&gt; (80 bytes).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment returnFieldFromArgs(MemorySegment selfPtr, MemorySegment args) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_WRAPPED_SCHEMA_LAYOUT);
    try {
      // Parse arg_fields from FFI_ReturnFieldArgs (first RVec at offset 0)
      MemorySegment argsReinterpreted = args.reinterpret(FFI_RETURN_FIELD_ARGS_LAYOUT.byteSize());
      List<Field> argFields = readWrappedSchemaRVec(argsReinterpreted, 0);

      // Call Java UDF
      Field resultField = udf.returnField(argFields);

      // Export result Field as FFI_ArrowSchema into the result payload
      writeFieldAsWrappedSchema(resultField, buffer, RESULT_SCHEMA_PAYLOAD_OFFSET);

      // Set discriminant = ROk (0)
      buffer.set(ValueLayout.JAVA_LONG, RESULT_SCHEMA_DISC_OFFSET, 0L);
      return buffer;
    } catch (Throwable e) {
      buffer.fill((byte) 0);
      buffer.set(ValueLayout.JAVA_LONG, RESULT_SCHEMA_DISC_OFFSET, 1L); // RErr
      NativeUtil.writeRString(
          Errors.getErrorMessage(e, fullStackTrace), buffer, RESULT_SCHEMA_PAYLOAD_OFFSET, arena);
      return buffer;
    }
  }

  /**
   * Callback: invoke_with_args. Receives args (RVec&lt;WrappedArray&gt;), arg_fields
   * (RVec&lt;WrappedSchema&gt;), num_rows (usize), return_field (WrappedSchema). Returns
   * FFIResult&lt;WrappedArray&gt; (160 bytes).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment invokeWithArgs(
      MemorySegment selfPtr,
      MemorySegment argsRVec,
      MemorySegment argFieldsRVec,
      long numRows,
      MemorySegment returnFieldSchema) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_WRAPPED_ARRAY_LAYOUT);
    List<FieldVector> argVectors = new ArrayList<>();
    try {
      // Parse args RVec<WrappedArray>
      MemorySegment argsReinterpreted = argsRVec.reinterpret(32);
      MemorySegment argsBuf = argsReinterpreted.get(ValueLayout.ADDRESS, 0);
      long argsLen = argsReinterpreted.get(ValueLayout.JAVA_LONG, 8);

      // Parse arg_fields RVec<WrappedSchema>
      MemorySegment fieldsReinterpreted = argFieldsRVec.reinterpret(32);
      List<Field> argFields = readWrappedSchemaRVecFromReinterpreted(fieldsReinterpreted);

      // Import each WrappedArray as a FieldVector
      if (argsLen > 0 && !argsBuf.equals(MemorySegment.NULL)) {
        MemorySegment argsData = argsBuf.reinterpret(argsLen * WRAPPED_ARRAY_SIZE);
        for (int i = 0; i < argsLen; i++) {
          long elementOffset = (long) i * WRAPPED_ARRAY_SIZE;
          // WrappedArray = { array: FFI_ArrowArray (80 bytes), schema: WrappedSchema (72 bytes) }
          MemorySegment arraySegment = argsData.asSlice(elementOffset, ARROW_ARRAY_SIZE);
          MemorySegment schemaSegment =
              argsData.asSlice(elementOffset + ARROW_ARRAY_SIZE, ARROW_SCHEMA_SIZE);

          try (ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
              ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
            // Copy FFI data into Arrow Java's structs
            MemorySegment arrayDest =
                MemorySegment.ofAddress(ffiArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
            arrayDest.copyFrom(arraySegment);

            MemorySegment schemaDest =
                MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
            schemaDest.copyFrom(schemaSegment);

            // Clear release in source — dest now owns it
            arraySegment.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
            schemaSegment.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);

            FieldVector vector = Data.importVector(allocator, ffiArray, ffiSchema, null);
            argVectors.add(vector);
          }
        }
      }

      // Import return field
      Field returnField;
      try (ArrowSchema returnSchema = ArrowSchema.allocateNew(allocator)) {
        MemorySegment returnDest =
            MemorySegment.ofAddress(returnSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
        MemorySegment returnSrc = returnFieldSchema.reinterpret(ARROW_SCHEMA_SIZE);
        returnDest.copyFrom(returnSrc);
        returnSrc.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
        returnField = Data.importField(allocator, returnSchema, null);
      }

      // Call Java UDF
      FieldVector result = udf.invoke(argVectors, argFields, (int) numRows, returnField, allocator);

      try {
        // Export result as WrappedArray into the result payload
        try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
            ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {

          Data.exportVector(allocator, result, null, outArray, outSchema);

          MemorySegment arraySrc =
              MemorySegment.ofAddress(outArray.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
          MemorySegment schemaSrc =
              MemorySegment.ofAddress(outSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);

          // Write WrappedArray = { FFI_ArrowArray, FFI_ArrowSchema } at payload offset
          long payloadOffset = RESULT_ARRAY_PAYLOAD_OFFSET;
          buffer.asSlice(payloadOffset, ARROW_ARRAY_SIZE).copyFrom(arraySrc);
          buffer.asSlice(payloadOffset + ARROW_ARRAY_SIZE, ARROW_SCHEMA_SIZE).copyFrom(schemaSrc);

          // Clear release in source — dest owns it
          arraySrc.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
          schemaSrc.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
        }
      } finally {
        result.close();
      }

      // Set discriminant = ROk (0)
      buffer.set(ValueLayout.JAVA_LONG, RESULT_ARRAY_DISC_OFFSET, 0L);
      return buffer;
    } catch (Throwable e) {
      buffer.fill((byte) 0);
      buffer.set(ValueLayout.JAVA_LONG, RESULT_ARRAY_DISC_OFFSET, 1L); // RErr
      NativeUtil.writeRString(
          Errors.getErrorMessage(e, fullStackTrace), buffer, RESULT_ARRAY_PAYLOAD_OFFSET, arena);
      return buffer;
    } finally {
      // Always close imported arg vectors to prevent memory leaks
      for (FieldVector v : argVectors) {
        v.close();
      }
    }
  }

  /**
   * Callback: coerce_types. Receives arg_types (RVec&lt;WrappedSchema&gt;). Returns
   * FFIResult&lt;RVec&lt;WrappedSchema&gt;&gt; (40 bytes).
   */
  @SuppressWarnings("unused") // Called via upcall stub
  MemorySegment coerceTypes(MemorySegment selfPtr, MemorySegment argTypesRVec) {
    MemorySegment buffer = arena.allocate(FFI_RESULT_RVEC_SCHEMA_LAYOUT);
    try {
      // Parse arg_types RVec<WrappedSchema>
      MemorySegment argsReinterpreted = argTypesRVec.reinterpret(32);
      List<Field> argFields = readWrappedSchemaRVecFromReinterpreted(argsReinterpreted);

      // Call Java UDF
      List<Field> coerced = udf.coerceTypes(argFields);

      // Export each coerced field as FFI_ArrowSchema
      MemorySegment schemasArray = arena.allocate(ARROW_SCHEMA_SIZE * coerced.size(), 8);
      for (int i = 0; i < coerced.size(); i++) {
        writeFieldAsWrappedSchema(coerced.get(i), schemasArray, (long) i * ARROW_SCHEMA_SIZE);
      }

      // Use Rust helper to build RVec<WrappedSchema>
      MemorySegment rvecOut = buffer.asSlice(RESULT_RVEC_PAYLOAD_OFFSET, 32);
      CREATE_RVEC_WRAPPED_SCHEMA.invokeExact(schemasArray, (long) coerced.size(), rvecOut);

      // Set discriminant = ROk (0)
      buffer.set(ValueLayout.JAVA_LONG, RESULT_RVEC_DISC_OFFSET, 0L);
      return buffer;
    } catch (Throwable e) {
      buffer.fill((byte) 0);
      buffer.set(ValueLayout.JAVA_LONG, RESULT_RVEC_DISC_OFFSET, 1L); // RErr
      NativeUtil.writeRString(
          Errors.getErrorMessage(e, fullStackTrace), buffer, RESULT_RVEC_PAYLOAD_OFFSET, arena);
      return buffer;
    }
  }

  /** Callback: Release the UDF. Called by Rust when done. */
  @SuppressWarnings("unused") // Called via upcall stub
  void release(MemorySegment selfPtr) {
    // Cleanup happens when arena is closed
  }

  @Override
  public void close() {
    // No-op: FFI struct is in Java arena memory, freed when arena closes
  }

  // ======== Helper methods ========

  /**
   * Read an RVec&lt;WrappedSchema&gt; starting at a given offset in a segment, importing each as a
   * Field.
   */
  private List<Field> readWrappedSchemaRVec(MemorySegment segment, long rvecOffset) {
    MemorySegment buf = segment.get(ValueLayout.ADDRESS, rvecOffset);
    long len = segment.get(ValueLayout.JAVA_LONG, rvecOffset + 8);

    List<Field> fields = new ArrayList<>((int) len);
    if (len > 0 && !buf.equals(MemorySegment.NULL)) {
      MemorySegment data = buf.reinterpret(len * ARROW_SCHEMA_SIZE);
      for (int i = 0; i < len; i++) {
        MemorySegment schemaSegment = data.asSlice((long) i * ARROW_SCHEMA_SIZE, ARROW_SCHEMA_SIZE);
        fields.add(importFieldFromSchemaSegment(schemaSegment));
      }
    }
    return fields;
  }

  /** Read an RVec&lt;WrappedSchema&gt; from a pre-reinterpreted 32-byte segment. */
  private List<Field> readWrappedSchemaRVecFromReinterpreted(MemorySegment rvec) {
    MemorySegment buf = rvec.get(ValueLayout.ADDRESS, 0);
    long len = rvec.get(ValueLayout.JAVA_LONG, 8);

    List<Field> fields = new ArrayList<>((int) len);
    if (len > 0 && !buf.equals(MemorySegment.NULL)) {
      MemorySegment data = buf.reinterpret(len * ARROW_SCHEMA_SIZE);
      for (int i = 0; i < len; i++) {
        MemorySegment schemaSegment = data.asSlice((long) i * ARROW_SCHEMA_SIZE, ARROW_SCHEMA_SIZE);
        fields.add(importFieldFromSchemaSegment(schemaSegment));
      }
    }
    return fields;
  }

  /** Import a Field from FFI_ArrowSchema bytes. */
  private Field importFieldFromSchemaSegment(MemorySegment schemaSegment) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      MemorySegment dest =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      dest.copyFrom(schemaSegment);
      // Clear release in source — dest now owns it
      schemaSegment.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
      return Data.importField(allocator, ffiSchema, null);
    }
  }

  /** Export a Field as FFI_ArrowSchema bytes into a buffer at a given offset. */
  private void writeFieldAsWrappedSchema(Field field, MemorySegment buffer, long offset) {
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportField(allocator, field, null, ffiSchema);
      MemorySegment src =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      buffer.asSlice(offset, ARROW_SCHEMA_SIZE).copyFrom(src);
      // Clear release in source — dest now owns it
      src.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }
  }
}
