package org.apache.arrow.datafusion.common;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.arrow.datafusion.generated.DfError;
import org.apache.arrow.datafusion.generated.DfExprBytes;
import org.apache.arrow.datafusion.generated.DfStringArray;

/**
 * Helpers used by {@code *Bridge} classes. Scoped to the <em>downcall</em> side of the FFI
 * boundary: each method takes a Diplomat-generated opaque type ({@link DfError},
 * {@link DfExprBytes}, {@link DfStringArray}) returned by a Diplomat downcall and converts it into
 * a Java-friendly form. Public because bridge classes live in several packages
 * ({@code execution}, {@code logical_expr}, {@code physical_expr}, etc.).
 *
 * <p>This is distinct from:
 *
 * <ul>
 *   <li>{@code NativeUtil} (root package, package-private) — adapter-side helpers that work with
 *       <em>raw {@code long} addresses</em> handed in by Rust during trait upcalls.
 *   <li>{@code ArrowFfiUtil} (root package, package-private) — Arrow C Data Interface struct
 *       transfer (copy + clear-release for {@code FFI_ArrowSchema} / {@code FFI_ArrowArray}).
 * </ul>
 *
 * <p>Canonical usage:
 *
 * <pre>{@code
 * return BridgeUtil.unwrap("Failed to do X", () -> dfThing.doSomething(arg));
 * try (DfStringArray names = dfCtx.catalogNames()) {
 *   return BridgeUtil.toList(names);
 * }
 * }</pre>
 */
public final class BridgeUtil {
  private BridgeUtil() {}

  /**
   * Invoke {@code work} and translate Diplomat errors. {@link DfError} (thrown by Diplomat-generated
   * methods) becomes a {@link NativeDataFusionError}; any other exception is wrapped in a {@link
   * DataFusionError} with {@code context} as the message.
   */
  public static <T> T unwrap(String context, Supplier<T> work) {
    try {
      return work.get();
    } catch (DfError e) {
      throw new NativeDataFusionError(e);
    } catch (DataFusionError e) {
      throw e;
    } catch (Exception e) {
      throw new DataFusionError(context, e);
    }
  }

  /** Void-returning variant of {@link #unwrap(String, Supplier)}. */
  public static void unwrap(String context, Runnable work) {
    unwrap(
        context,
        () -> {
          work.run();
          return null;
        });
  }

  /**
   * Copy the bytes wrapped by a {@link DfExprBytes} into a Java {@code byte[]}. The caller is
   * responsible for closing the {@code DfExprBytes} (typically via try-with-resources).
   */
  public static byte[] toBytes(DfExprBytes bytes) {
    long len = bytes.len();
    if (len == 0) {
      return new byte[0];
    }
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment buf = arena.allocate(len);
      bytes.copyTo(buf.address(), len);
      return buf.toArray(ValueLayout.JAVA_BYTE);
    }
  }

  /**
   * Copy the contents of a Diplomat-returned {@link DfStringArray} into an immutable
   * {@code List<String>}. The caller is responsible for closing the {@code DfStringArray}.
   */
  public static List<String> toList(DfStringArray arr) {
    int len = (int) arr.len();
    if (len == 0) {
      return List.of();
    }
    List<String> result = new ArrayList<>(len);
    for (int i = 0; i < len; i++) {
      result.add(arr.get(i));
    }
    return List.copyOf(result);
  }
}
