package org.apache.arrow.datafusion.providers;

import org.apache.arrow.datafusion.generated.DfRustTableProvider;

/**
 * Handle for a {@code TableProvider} constructed on the Rust side.
 *
 * <p>Instances are produced by per-backend factory classes (for example, {@link
 * org.apache.arrow.datafusion.providers.flight.FlightSqlTableProvider}) and registered with a
 * session via {@code SessionContext.registerTable(String, RustTableProvider)}. Unlike the
 * user-implementable {@link org.apache.arrow.datafusion.catalog.TableProvider} interface, a
 * {@code RustTableProvider} requires no Java-side callbacks — the backing provider lives entirely
 * in Rust.
 *
 * {@snippet :
 * try (FlightSqlTableProvider tp = FlightSqlTableProvider.builder()
 *         .endpoint("http://localhost:32010")
 *         .query("SELECT * FROM taxi")
 *         .build();
 *     SessionContext ctx = new SessionContext()) {
 *     ctx.registerTable("taxi", tp);
 *     ctx.sql("SELECT count(*) FROM taxi").show();
 * }
 * }
 *
 * <p>Instances must be {@link #close() closed} exactly once. Registering a provider with a session
 * does not transfer ownership; callers still close the handle when no longer needed.
 *
 * @see <a
 *     href="https://docs.rs/datafusion/53.1.0/datafusion/catalog/trait.TableProvider.html">Rust
 *     DataFusion: TableProvider</a>
 */
public abstract class RustTableProvider implements AutoCloseable {

  private final DfRustTableProvider handle;

  /** Protected — only subclasses (per-backend factories) construct instances. */
  protected RustTableProvider(DfRustTableProvider handle) {
    this.handle = handle;
  }

  /**
   * Internal accessor used by {@code SessionContextBridge} to call the Rust-side registration. Not
   * intended for application code; treat the returned opaque as internal.
   */
  public DfRustTableProvider handle() {
    return handle;
  }

  @Override
  public void close() {
    handle.close();
  }
}
