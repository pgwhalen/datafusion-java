package org.apache.arrow.datafusion;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Arrow C Data Interface FFI struct transfer — sizes, layout offsets, and copy + clear-release
 * helpers for moving {@code FFI_ArrowSchema} / {@code FFI_ArrowArray} across the Java/Rust
 * boundary. Used by {@code Df*Adapter} classes; scope is intentionally narrow to the Arrow C Data
 * Interface spec so the offset/release-callback knowledge stays in one place.
 *
 * <p>Each "import from segment" helper does <em>copy + clear-release</em>: it copies a Rust- (or
 * peer-) allocated FFI struct into a Java-allocated buffer, then nulls out the release callback in
 * the source so the original side's {@code Drop} is a no-op (the destination now owns the
 * release).
 *
 * <p><b>Offset cheat sheet</b>: the release callback for an {@code FFI_ArrowSchema} sits at offset
 * 56; for an {@code FFI_ArrowArray} it sits at offset 64. (The other field at offset 64 of an
 * ArrowSchema is {@code private_data}; clearing it in the export-from-Java direction is also safe
 * because {@code ArrowSchema.close()} does not invoke the release callback.)
 *
 * <p>This is distinct from {@link NativeUtil} (generic FFM-level helpers, downcall handles, raw
 * addr+len reads) and {@code BridgeUtil} (Diplomat opaque conversion + error translation on the
 * downcall side).
 */
final class ArrowFfiUtil {
  /** sizeof(struct ArrowSchema) — 72 bytes. */
  static final long ARROW_SCHEMA_SIZE = 72;

  /** sizeof(struct ArrowArray) — 80 bytes. */
  static final long ARROW_ARRAY_SIZE = 80;

  /** Combined size of an (FFI_ArrowArray, FFI_ArrowSchema) pair as laid out by Rust. */
  static final long WRAPPED_ARRAY_SIZE = ARROW_ARRAY_SIZE + ARROW_SCHEMA_SIZE;

  private static final long ARROW_SCHEMA_RELEASE_OFFSET = 56;
  private static final long ARROW_ARRAY_RELEASE_OFFSET = 64;

  private ArrowFfiUtil() {}

  /**
   * Copy an FFI_ArrowSchema from a foreign-allocated source segment into a Java-allocated {@link
   * ArrowSchema}, and clear the release callback in the source so the source's owner does not
   * attempt to free the contents (the destination now owns them).
   */
  static ArrowSchema importSchemaFromSegment(BufferAllocator allocator, MemorySegment srcSegment) {
    ArrowSchema dest = ArrowSchema.allocateNew(allocator);
    try {
      MemorySegment destSegment =
          MemorySegment.ofAddress(dest.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      destSegment.copyFrom(srcSegment);
      srcSegment.set(ValueLayout.ADDRESS, ARROW_SCHEMA_RELEASE_OFFSET, MemorySegment.NULL);
      return dest;
    } catch (RuntimeException | Error e) {
      dest.close();
      throw e;
    }
  }

  /**
   * Copy an FFI_ArrowArray from a foreign-allocated source segment into a Java-allocated {@link
   * ArrowArray}, and clear the release callback in the source so the source's owner does not
   * attempt to free the contents.
   */
  static ArrowArray importArrayFromSegment(BufferAllocator allocator, MemorySegment srcSegment) {
    ArrowArray dest = ArrowArray.allocateNew(allocator);
    try {
      MemorySegment destSegment =
          MemorySegment.ofAddress(dest.memoryAddress()).reinterpret(ARROW_ARRAY_SIZE);
      destSegment.copyFrom(srcSegment);
      srcSegment.set(ValueLayout.ADDRESS, ARROW_ARRAY_RELEASE_OFFSET, MemorySegment.NULL);
      return dest;
    } catch (RuntimeException | Error e) {
      dest.close();
      throw e;
    }
  }

  /**
   * Copy an FFI_ArrowSchema at {@code srcAddr} into a Java-allocated {@link ArrowSchema}, clearing
   * the release callback in the source.
   */
  static ArrowSchema importSchemaFromAddress(BufferAllocator allocator, long srcAddr) {
    MemorySegment src = MemorySegment.ofAddress(srcAddr).reinterpret(ARROW_SCHEMA_SIZE);
    return importSchemaFromSegment(allocator, src);
  }

  /**
   * Copy a Java-allocated FFI_ArrowSchema's contents to the given foreign-allocated address. Clears
   * {@code private_data} (offset 64) in the Java source — {@code ArrowSchema.close()} does not
   * invoke the release callback, so it is safe to leave release in place; the Rust copy retains
   * ownership and will free on drop.
   */
  static void copySchemaToAddress(MemorySegment srcSegment, long destAddr) {
    MemorySegment dest = MemorySegment.ofAddress(destAddr).reinterpret(ARROW_SCHEMA_SIZE);
    dest.copyFrom(srcSegment);
    // Offset 64 = private_data on ArrowSchema; same numeric offset as the array release.
    srcSegment.set(ValueLayout.ADDRESS, ARROW_ARRAY_RELEASE_OFFSET, MemorySegment.NULL);
  }

  /**
   * Copy a Java-allocated FFI_ArrowArray's contents to the given foreign-allocated address. Clears
   * the release callback in the Java source so {@code ArrowArray.close()} does not also free the
   * data (the Rust copy now owns it).
   */
  static void copyArrayToAddress(MemorySegment srcSegment, long destAddr) {
    MemorySegment dest = MemorySegment.ofAddress(destAddr).reinterpret(ARROW_ARRAY_SIZE);
    dest.copyFrom(srcSegment);
    srcSegment.set(ValueLayout.ADDRESS, ARROW_ARRAY_RELEASE_OFFSET, MemorySegment.NULL);
  }

  /**
   * Import a {@link Field} from an {@code FFI_ArrowSchema} memory segment that wraps a Schema with
   * a single field (format "+s"). Clears the release callback in the source segment.
   */
  static Field importFieldFromSegment(BufferAllocator allocator, MemorySegment srcSegment) {
    try (ArrowSchema ffiSchema = importSchemaFromSegment(allocator, srcSegment)) {
      Schema schema = Data.importSchema(allocator, ffiSchema, null);
      return schema.getFields().get(0);
    }
  }

  /** Import a {@link Field} from an FFI_ArrowSchema at the given foreign-allocated address. */
  static Field importFieldFromAddress(BufferAllocator allocator, long addr) {
    MemorySegment segment = MemorySegment.ofAddress(addr).reinterpret(ARROW_SCHEMA_SIZE);
    return importFieldFromSegment(allocator, segment);
  }

  /**
   * Import an array of FFI_ArrowSchema structs (each wrapping a single-field Schema) starting at
   * {@code addr} and spanning {@code count} entries.
   */
  static List<Field> importFieldArray(BufferAllocator allocator, long addr, long count) {
    List<Field> fields = new ArrayList<>((int) count);
    if (count > 0 && addr != 0) {
      MemorySegment data = MemorySegment.ofAddress(addr).reinterpret(count * ARROW_SCHEMA_SIZE);
      for (int i = 0; i < count; i++) {
        MemorySegment segment = data.asSlice((long) i * ARROW_SCHEMA_SIZE, ARROW_SCHEMA_SIZE);
        fields.add(importFieldFromSegment(allocator, segment));
      }
    }
    return fields;
  }

  /**
   * Export a {@link Field} as an FFI_ArrowSchema written to the given foreign-allocated address.
   * Wraps the field in a single-field Schema before export (matches the Rust import side).
   */
  static void exportFieldToAddress(BufferAllocator allocator, Field field, long destAddr) {
    Schema wrapper = new Schema(List.of(field));
    try (ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, wrapper, null, ffiSchema);
      MemorySegment src =
          MemorySegment.ofAddress(ffiSchema.memoryAddress()).reinterpret(ARROW_SCHEMA_SIZE);
      copySchemaToAddress(src, destAddr);
    }
  }
}
