package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;

/**
 * Common interface for all Handle classes that bridge a Java interface to a DataFusion Rust trait.
 *
 * <p>Each Handle wraps a Java interface that maps 1:1 to a DataFusion Rust trait and creates upcall
 * stubs so Rust can call back into the Java implementation.
 */
interface TraitHandle extends AutoCloseable {

  /** Get the trait struct pointer to pass to Rust. */
  MemorySegment getTraitStruct();

  /** Write the trait struct pointer into a native output pointer. */
  default void setToPointer(MemorySegment out) {
    new PointerOut(out).set(getTraitStruct());
  }
}
