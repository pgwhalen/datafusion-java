package org.apache.arrow.datafusion;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

/**
 * Wraps a MemorySegment representing an upcall stub.
 *
 * <p>An upcall stub is a native function pointer that, when called from native code, invokes a Java
 * method. This record provides a type-safe wrapper that clarifies the segment's purpose and
 * provides convenient factory methods.
 *
 * @param segment the memory segment containing the upcall stub
 */
record UpcallStub(MemorySegment segment) {

  /**
   * Creates an upcall stub for a method handle bound to a specific instance.
   *
   * <p>This is the standard pattern for creating upcall stubs in Handle classes:
   *
   * <pre>{@code
   * private static final MethodHandle CALLBACK_MH = ...;  // static, looked up once
   * private static final FunctionDescriptor CALLBACK_DESC = ...;  // static
   *
   * // In constructor:
   * this.callbackStub = UpcallStub.create(CALLBACK_MH.bindTo(this), CALLBACK_DESC, arena);
   * }</pre>
   *
   * @param boundHandle the method handle bound to a specific receiver instance
   * @param descriptor the function descriptor for the native signature
   * @param arena the arena that manages the stub's lifetime
   * @return a new UpcallStub wrapping the created native function pointer
   */
  static UpcallStub create(MethodHandle boundHandle, FunctionDescriptor descriptor, Arena arena) {
    Linker linker = DataFusionBindings.getLinker();
    return new UpcallStub(linker.upcallStub(boundHandle, descriptor, arena));
  }
}
